"""rel3vl 固定输入演示（离线、确定性、纯标准库）。

运行：``python demo.py``（90 秒预算内，实测远小于；现场计算、无 sleep、无预录）。

内容：
A. 旧批量解释器：3VL 过滤（UNKNOWN 丢弃）与 LEFT JOIN 的 ON/WHERE 差异；
B. **新增增量能力端到端**：带行 ID 的增删流 → 左外连接 → 分组聚合 →
   事件时间滚动窗口。一个批次里同时发生
   ①删除外连接最后一条匹配（撤回旧行/补 NULL）、
   ②补入阈值内迟到订单（撤回+新增，绝不只追加）、
   ③水位推进关闭窗口；结果只发布一次；并打印实际触碰的状态键，
   证明没有对无关组全量扫描；末态现场调用原批量解释器对照；
C. 一次真实触发的拒绝：更新落入已密封窗口 → StreamError，状态不变。
"""

from __future__ import annotations

from collections import Counter

from rel3vl import StreamError, execute
from rel3vl.errors import SchemaError
from rel3vl.streaming import IncrementalProcessor


# ================================================================ A. 旧批量 3VL

def scan(table, alias):
    return {"op": "scan", "table": table, "as": alias}


def col(t, c):
    return {"kind": "col", "table": t, "column": c}


def lit(v):
    return {"kind": "lit", "value": v}


def cmp(op, a, b):
    return {"kind": "cmp", "op": op, "left": a, "right": b}


def band(a, b):
    return {"kind": "and", "left": a, "right": b}


def is_null(e):
    return {"kind": "is_null", "expr": e}


def part_a_batch_3vl():
    print("=" * 70)
    print("A. 批量解释器（旧契约）：3VL 过滤与 LEFT JOIN 的 ON/WHERE 差异")
    print("=" * 70)
    tables = {
        "employees": {"columns": ["id", "dept", "score"],
                      "rows": [[1, "A", 10], [2, "A", None], [4, None, 30]]},
        "departments": {"columns": ["did", "dname"],
                        "rows": [["A", "Alpha"]]},
    }
    plan_ge = {"op": "filter", "input": scan("employees", "e"),
               "expr": cmp(">=", col("e", "score"), lit(15))}
    print("filter(score >= 15)：score=NULL -> UNKNOWN，行被丢弃 ->",
          [r[0] for r in execute(plan_ge, tables)["rows"]])

    on_plan = {"op": "left_join", "left": scan("employees", "e"),
               "right": scan("departments", "d"),
               "on": band(cmp("=", col("e", "dept"), col("d", "did")),
                          cmp("=", col("e", "id"), lit(1)))}
    where_plan = {"op": "filter",
                  "input": {"op": "left_join", "left": scan("employees", "e"),
                            "right": scan("departments", "d"),
                            "on": cmp("=", col("e", "dept"), col("d", "did"))},
                  "expr": cmp("=", col("e", "id"), lit(1))}
    on_n = len(execute(on_plan, tables)["rows"])
    where_n = len(execute(where_plan, tables)["rows"])
    print(f"LEFT JOIN ON(dept=did AND id=1) 输出 {on_n} 行（未匹配补 NULL）；"
          f"同条件放连接后 WHERE 输出 {where_n} 行（补 NULL 行也被过滤）")


# ================================================ B. 增量：外连接→聚合→窗口

# 计划：orders LEFT JOIN customers  ON o.cid = c.cid
#       -> AGGREGATE GROUP BY (city, ts) : n=count(*), s=sum(amt)
#       -> TUMBLING WINDOW(size=10, allowed_lateness=2) ON ts
def streaming_plan():
    left_join = {
        "op": "left_join", "left": scan("orders", "o"),
        "right": scan("customers", "c"),
        "on": cmp("=", col("o", "cid"), col("c", "cid"))}
    aggregate = {
        "op": "aggregate", "input": left_join,
        "groups": [{"name": "city", "expr": col("c", "city")},
                   {"name": "ts", "expr": col("o", "ts")}],
        "aggs": [{"name": "n", "func": "count"},
                 {"name": "s", "func": "sum", "arg": col("o", "amt")}]}
    return {"op": "window", "input": aggregate,
            "time_col": col("", "ts"), "size": 10, "allowed_lateness": 2}


INPUTS = {"orders": ["oid", "cid", "ts", "amt"],
          "customers": ["cid", "city"]}


def show(title, res):
    print(f"\n{title}")
    for c in res["changes"]:
        print(f"  {c['op']} {c['row']}  (mult={c['mult']})")
    for w in res["sealed_windows"]:
        print(f"  sealed window [{w['window_start']}, {w['window_end']})")


def part_b_incremental():
    print("\n" + "=" * 70)
    print("B. 增量解释器：行 ID 增删流 → 左外连接 → 分组聚合 → 滚动窗口")
    print("=" * 70)
    proc = IncrementalProcessor(streaming_plan(), INPUTS)
    live = {"orders": {}, "customers": {}}

    # 批 1：订单 o1 到来，客户维表未到 -> 外连接补 NULL -> city 为 NULL 的组
    b1 = {"watermark": 0, "changes": {"orders": [
        {"id": "o1", "op": "insert", "row": [1, 9, 2, 10]}]}}
    show("批1（watermark=0）：订单 o1 先到，客户缺失，外连接补 NULL",
         proc.apply_batch(b1))
    live["orders"]["o1"] = (1, 9, 2, 10)

    # 批 2：迟到客户 c9 补入 -> 匹配数 0->1，撤回补 NULL 行
    b2 = {"changes": {"customers": [
        {"id": "c9", "op": "insert", "row": [9, "X"]}]}}
    show("批2：迟到客户 c9 补入（匹配数 0->1，撤回补 NULL 行）",
         proc.apply_batch(b2))
    live["customers"]["c9"] = (9, "X")

    # 水位推进到 11：[0,10) 的 end=10 > wm-lateness=9，窗口仍开放
    # （阈值 2 个时间单位内的迟到更新仍可接收）。
    show("水位推进 0->11：[0,10) 尚未密封（10 > 11-2=9），无结果变更",
         proc.apply_batch({"watermark": 11}))

    # 批 3（棘手边界，三件事同一原子批，批前水位=11）：
    #  ① 删除客户 c9 —— o1 的最后（唯一）匹配被删；
    #  ② 补入阈值内迟到订单 o2（ts=3：[0,10) 此刻 10 > 11-2=9 仍开放，允许）；
    #  ③ 水位推进到 12：10 <= 12-2=10，本批末关闭并密封 [0,10)。
    b3 = {"watermark": 12, "changes": {
        "customers": [{"id": "c9", "op": "delete"}],
        "orders": [{"id": "o2", "op": "insert", "row": [2, None, 3, 5]}]}}
    r3 = proc.apply_batch(b3)
    show("批3（原子）：① 删除最后匹配 c9  ② 阈值内迟到订单 o2 补入"
         "  ③ 水位 11->12 关闭 [0,10)", r3)
    print("  -> 撤回(-)与新增(+)成对出现，且整批结果只发布这一次；"
          "没有只追加一条新聚合行")
    del live["customers"]["c9"]
    live["orders"]["o2"] = (2, None, 3, 5)

    print("\n  实际触碰的状态键（证明未做无关组全量扫描）：")
    for node, info in sorted(r3["touched"].items()):
        op = info.get("op")
        if op == "left_join":
            print(f"    node#{node} left_join：仅等值键桶 {info['right_keys']}；"
                  f"PAD 翻转左身份 {info['pad_switched']}")
        elif op == "aggregate":
            print(f"    node#{node} aggregate：仅触碰分组 {info['groups']}"
                  f"（其它窗口/城市组不扫描）")
        elif op == "window":
            print(f"    node#{node} window：仅触碰窗口起点 "
                  f"{info['window_starts']}")

    # 现场调用原批量解释器，对累计输入整体重算做对照（独立代码路径）。
    tables = {t: {"columns": INPUTS[t],
                  "rows": [list(r) for r in live[t].values()]} for t in INPUTS}
    batch_rows = Counter(tuple(r) for r in execute(
        streaming_plan(), tables, max_rows=100_000, max_nodes=50)["rows"])
    inc_rows = Counter(tuple(r) for r in proc.current_result())
    print("\n  累计输入交给原批量解释器整体重算 -> "
          f"{'一致 ✓' if batch_rows == inc_rows else '不一致 ✗'}")
    print("  当前窗口结果：")
    for row in proc.current_result():
        print("   ", row)
    return proc


# ================================================ C. 超保留期限 -> 明确拒绝

def part_c_reject(proc):
    print("\n" + "=" * 70)
    print("C. 拒绝边界：更新落入已密封窗口（超过保留期限）")
    print("=" * 70)
    before = Counter(tuple(r) for r in proc.current_result())
    wm_before = proc.watermark
    # wm=12, allowed_lateness=2：[0,10) 已密封（10 <= 12-2=10）。
    # 删除 ts=2 的 o1 会进入该密封窗口。
    bad = {"changes": {"orders": [{"id": "o1", "op": "delete"}]}}
    print("尝试在 watermark=12 删除 ts=2 的订单 o1（窗口 [0,10) 已密封）：")
    try:
        proc.apply_batch(bad)
    except StreamError as exc:
        print(f"  明确拒绝（StreamError，QueryError 子类）：\n    {exc}")
    after = Counter(tuple(r) for r in proc.current_result())
    assert before == after and proc.watermark == wm_before
    print("  已核对：结果袋与水位均未改变，失败不留部分变更。")

    # 旧契约的非法列拒绝仍然保留
    try:
        execute({"op": "filter", "input": scan("t", "t"),
                 "expr": cmp("=", col("t", "nope"), lit(1))},
                {"t": {"columns": ["x"], "rows": [[1]]}})
    except SchemaError as exc:
        print(f"旧入口非法列仍按 SchemaError 拒绝：{str(exc).split('：')[0]}")


def main():
    part_a_batch_3vl()
    proc = part_b_incremental()
    part_c_reject(proc)
    print("\n演示完成：全部结果现场计算，无预录、无 sleep。")


if __name__ == "__main__":
    main()
