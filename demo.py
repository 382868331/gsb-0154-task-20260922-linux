"""rel3vl 固定输入演示（离线、确定性、纯标准库）。

运行：``python demo.py``（实测远小于 90 秒预算）。

演示内容：
1. 两种过滤结果：普通比较过滤（UNKNOWN 行被丢）与 ``is_null`` 过滤；
2. LEFT JOIN 的 ON 条件与连接后 WHERE 的差异（3VL 经典边界）；
3. 一个真实触发的拒绝：引用不存在的列 -> :class:`rel3vl.SchemaError`；
4. 【增量】增删流上的左外连接 -> 事件时间窗口聚合：
   正常差量（撤回 + 新增）、一个棘手边界（同批删除最后匹配 + 补入迟到行 +
   水位关闭窗口，整批只发布一次）、一次真实触发的超期拒绝（不改状态）。

所有数据由本文件内确定性代码生成，无网络、无 SQL、无数据库、无 sleep。
"""

from __future__ import annotations

from rel3vl import (
    IncrementalExecutor,
    LateDataError,
    QueryError,
    execute,
)
from rel3vl.errors import SchemaError


# ------------------------------------------------------------ 确定性示例数据

def build_tables():
    # 员工：覆盖普通行、重复部门、NULL 分数、NULL 部门；无随机源。
    employees = {
        "columns": ["id", "dept", "score"],
        "rows": [
            [1, "A", 10],
            [2, "A", None],   # 分数未知
            [3, "B", 20],
            [4, None, 30],    # 部门未知
        ],
    }
    departments = {
        "columns": ["did", "dname"],
        "rows": [
            ["A", "Alpha"],
            ["B", "Beta"],
            [None, "Mystery"],  # NULL 主键：NULL = NULL 为 UNKNOWN，永不匹配
        ],
    }
    return {"employees": employees, "departments": departments}


# ------------------------------------------------------------ 表达式小工具

def col(t, c):
    return {"kind": "col", "table": t, "column": c}


def lit(v):
    return {"kind": "lit", "value": v}


def cmp(op, l, r):
    return {"kind": "cmp", "op": op, "left": l, "right": r}


def band(l, r):
    return {"kind": "and", "left": l, "right": r}


def is_null(e):
    return {"kind": "is_null", "expr": e}


def scan(table, alias):
    return {"op": "scan", "table": table, "as": alias}


def print_rows(title, result):
    header = ", ".join(f"{c['source']}.{c['name']}" for c in result["columns"])
    print(f"\n{title}（{len(result['rows'])} 行）")
    print(f"  {header}")
    for row in result["rows"]:
        print(f"  {row}")


def vcol(t, c):
    return {"kind": "col", "table": t, "column": c}


def vcmp(a, b):
    return {"kind": "cmp", "op": "=", "left": a, "right": b}


def vscan(t, a):
    return {"op": "scan", "table": t, "as": a}


def print_changes(title, out):
    print(f"\n{title}（seq={out['seq']}，watermark={out['watermark']}，"
          f"本批净变更 {len(out['changes'])} 条，整批只发布这一次）")
    for ch in out["changes"]:
        sign = "+" if ch["delta"] > 0 else ""
        print(f"  delta={sign}{ch['delta']:<3d} row={ch['row']}")
    if out["closed"]:
        print(f"  本批关闭窗口（结果行冻结、状态释放）：{out['closed']}")


def incremental_demo():
    print("\n" + "=" * 72)
    print("增量演示：orders LEFT JOIN price_dim -> 事件时间滚动窗口聚合")
    print("=" * 72)
    tables = {
        "orders": {"columns": ["c", "ts"], "rows": []},
        "price_dim": {"columns": ["k", "price"], "rows": []},
    }
    # window(width=5)：键 c，聚合 count_star 与 sum(price)
    plan = {
        "op": "window",
        "input": {
            "op": "left_join",
            "left": vscan("orders", "o"),
            "right": vscan("price_dim", "p"),
            "on": vcmp(vcol("o", "c"), vcol("p", "k")),
        },
        "time": vcol("o", "ts"),
        "width": 5,
        "keys": [{"name": "c", "expr": vcol("o", "c")}],
        "aggs": [
            {"name": "n", "func": "count_star"},
            {"name": "total_price", "func": "sum", "expr": vcol("p", "price")},
        ],
    }
    ex = IncrementalExecutor(plan, tables, allowed_lateness=1)

    out = ex.apply_batch({"changes": [
        {"table": "orders", "op": "+", "id": "o1", "row": ["A", 0]},
        {"table": "orders", "op": "+", "id": "o2", "row": ["B", 5]},
        {"table": "price_dim", "op": "+", "id": "pA", "row": ["A", 100]},
        {"table": "price_dim", "op": "+", "id": "pB", "row": ["B", 200]},
    ]})
    print_changes("[初始批] A 订单进入 [0,5)，B 订单进入 [5,10)，各匹配维表", out)

    # 阈值内迟到（t=2，新水位 3，保留下界 3-1=2）：聚合改变必须撤回 + 新增
    out = ex.apply_batch({"changes": [
        {"table": "orders", "op": "+", "id": "o3", "row": ["A", 2]}],
        "watermark": 3})
    print_changes("[正常差量] 阈值内迟到一条 A 订单：A 组 n=1->2、"
                  "total_price=100->200，旧行撤回(-1)、新行插入(+1)", out)

    # 棘手边界：同批 (删除 B 的最后匹配 -> 补 NULL) + (补入迟到新组 C)
    # + 水位 6 关闭 [0,5)（0+5 <= 6-1）。
    out = ex.apply_batch({"changes": [
        {"table": "price_dim", "op": "-", "id": "pB"},
        {"table": "orders", "op": "+", "id": "o4", "row": ["C", 5]},
    ], "watermark": 6})
    print_changes("[棘手边界] 同批：删 B 最后匹配(撤回 200、补 NULL) + "
                  "迟到 t=5 的新组 C 落入仍开放的 [5,10) + 关闭 [0,5)", out)
    print("  触碰的窗口状态键（只含实际受影响的桶，无全组扫描）：")
    for key in out["touched"][ex.root.num]:
        print(f"    {key}")

    # 真实触发的拒绝：窗口已关闭，再写 t=0 的更新 -> LateDataError，状态不变
    frozen = ex.current_result()
    print("\n[拒绝边界] 向已关闭窗口 [0,5) 插入 t=0 的更新：")
    try:
        ex.apply_batch({"changes": [
            {"table": "orders", "op": "+", "id": "bad", "row": ["A", 0]}]})
    except LateDataError as exc:
        print(f"  按契约拒绝（LateDataError 是 QueryError 子类）：\n    {exc}")
    assert ex.current_result()["rows"] == frozen["rows"], "拒绝后状态必须不变"
    print("  已断言：拒绝前后结果袋逐行相同，状态未被修改；seq 仍为", ex.seq)

    # 快照恢复：恢复后续跑得到相同变更
    snap = ex.snapshot()
    ex2 = ex.restore(snap)
    out1 = ex.apply_batch({"changes": [
        {"table": "orders", "op": "-", "id": "o4"}], "watermark": 6})
    out2 = ex2.apply_batch({"changes": [
        {"table": "orders", "op": "-", "id": "o4"}], "watermark": 6})
    assert out1["changes"] == out2["changes"]
    print("\n[快照恢复] 导出快照 -> 恢复 -> 同一批输入产生完全相同的变更流：")
    for ch in out2["changes"]:
        print(f"  delta={ch['delta']:+d} row={ch['row']}")


def main():
    tables = build_tables()
    emp = scan("employees", "e")

    # --- 过滤结果一：score >= 15 —— NULL 分数产生 UNKNOWN，行被丢弃
    plan_ge = {
        "op": "filter",
        "input": emp,
        "expr": cmp(">=", col("e", "score"), lit(15)),
    }
    print_rows("[过滤一] filter(score >= 15)：仅 TRUE 保留，UNKNOWN(NULL) 丢弃",
               execute(plan_ge, tables))

    # --- 过滤结果二：is_null(score) —— 显式 NULL 判定，不产生 UNKNOWN
    plan_null = {
        "op": "filter",
        "input": scan("employees", "e"),
        "expr": is_null(col("e", "score")),
    }
    print_rows("[过滤二] filter(is_null(score))：直接判定 NULL",
               execute(plan_null, tables))

    # --- LEFT JOIN：ON 附加条件 vs 连接后 WHERE
    on_plan = {
        "op": "left_join",
        "left": scan("employees", "e"),
        "right": scan("departments", "d"),
        "on": band(
            cmp("=", col("e", "dept"), col("d", "did")),
            cmp("=", col("e", "id"), lit(1)),
        ),
    }
    print_rows("[ON] LEFT JOIN ON dept=did AND id=1：未匹配左行补 NULL",
               execute(on_plan, tables))

    basic_left = {
        "op": "left_join",
        "left": scan("employees", "e"),
        "right": scan("departments", "d"),
        "on": cmp("=", col("e", "dept"), col("d", "did")),
    }
    where_plan = {
        "op": "filter",
        "input": basic_left,
        "expr": cmp("=", col("e", "id"), lit(1)),
    }
    print_rows("[WHERE] LEFT JOIN ... 后 WHERE id=1：补 NULL 行也被过滤",
               execute(where_plan, tables))

    # --- 真实触发的拒绝边界：引用不存在的列
    bad_plan = {
        "op": "filter",
        "input": scan("employees", "e"),
        "expr": cmp("=", col("e", "salary"), lit(0)),  # salary 列不存在
    }
    print("\n[拒绝边界] 引用非法列 e.salary：")
    try:
        execute(bad_plan, tables)
    except SchemaError as exc:
        print(f"  按契约拒绝（SchemaError，是 QueryError 的子类）：\n    {exc}")
    else:  # pragma: no cover - 演示中不应发生
        raise AssertionError("非法列必须被拒绝")

    # 再展示一个数值卫生边界：NaN 禁止进入表数据
    bad_tables = {"t": {"columns": ["x"], "rows": [[float("nan")]]}}
    print("\n[拒绝边界] 表数据含 NaN：")
    try:
        execute(scan("t", "t"), bad_tables)
    except QueryError as exc:
        print(f"  按契约拒绝（QueryError）：\n    {exc}")
    else:  # pragma: no cover
        raise AssertionError("NaN 必须被拒绝")

    print("\n演示完成：以上结果均为现场计算，无预录、无 sleep。")
    incremental_demo()
    print("\n全部演示完成（批量 + 增量）：结果均为现场计算，无预录、无 sleep。")


if __name__ == "__main__":
    main()
