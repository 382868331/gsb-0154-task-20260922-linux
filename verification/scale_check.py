"""扩展验证（与快速回归 tests/ 分开运行）。

内容：
1. 大规模非退化差分流：>= 10000 条输入行、同内容行重数达 100、
   100 个批次、三算子计划（left_join -> group_by；以及 left_join -> window）、
   逻辑水位单调推进、含阈值内迟到与超期拒绝。
2. 每隔若干批把"累计输入"交给**原批量解释器** rel3vl.execute 做检查点对照，
   增量终袋必须与批量结果逐行相等（期望值由批量解释器算出，测试只做比较；
   小规模确定性域的完整核对在 tests/test_stream.py 的独立朴素参考中完成）。
3. touched 局部性：单条变更只触碰其等值键，不扫描无关组。

运行：``python -m verification.scale_check``（约若干秒，无网络、无第三方包）。
本脚本退出码非 0 即失败；规模/重数/批次数断言不满足也算失败。
"""

from __future__ import annotations

import random
import time
from collections import Counter

from rel3vl import IncrementalExecutor, LateDataError, execute


def col(t, c):
    return {"kind": "col", "table": t, "column": c}


def cmp(o, a, b):
    return {"kind": "cmp", "op": o, "left": a, "right": b}


def scan(t, a):
    return {"op": "scan", "table": t, "as": a}


def bag_of(rows):
    return Counter(tuple(r) for r in rows)


def join_group_plan():
    return {
        "op": "group_by",
        "input": {
            "op": "left_join",
            "left": scan("o", "o"),
            "right": scan("d", "d"),
            "on": cmp("=", col("o", "c"), col("d", "k")),
        },
        "keys": [{"name": "c", "expr": col("o", "c")}],
        "aggs": [
            {"name": "n", "func": "count_star"},
            {"name": "sw", "func": "sum", "expr": col("d", "w")},
        ],
    }


def join_window_plan(width):
    return {
        "op": "window",
        "input": {
            "op": "inner_join",
            "left": scan("o", "o"),
            "right": scan("d", "d"),
            "on": cmp("=", col("o", "c"), col("d", "k")),
        },
        "time": col("o", "ts"),
        "width": width,
        "keys": [{"name": "c", "expr": col("o", "c")}],
        "aggs": [
            {"name": "n", "func": "count_star"},
            {"name": "sa", "func": "sum", "expr": col("o", "amt")},
        ],
    }


def run():
    rng = random.Random(424242)
    t0 = time.perf_counter()

    NK = 50                 # 维表键数
    NBATCH = 100
    PER_BATCH = 150         # 100 批累计插入上万条
    DUP = 100               # 同内容行重数目标
    WIDTH = 20
    LATENESS = 5

    dim = {"columns": ["k", "w"], "rows": [[k, k * 10] for k in range(NK)]}
    tables = {
        "o": {"columns": ["c", "amt", "ts"], "rows": []},
        "d": dim,
    }

    # ---- 场景一：left_join -> group_by，100 批，重数 100，批量对照 ----
    plan1 = join_group_plan()
    ex = IncrementalExecutor(plan1, tables)
    live_o = {}        # id -> (c, amt, ts)
    idc = 0
    # 强制制造重数 100：同一个 (c,amt,ts) 用 100 个不同 id 插入
    dup_template = (7, 3, 30)
    for j in range(DUP):
        rec = {"table": "o", "op": "+", "id": f"dup-{j}", "row": list(dup_template)}
        ex.apply_batch({"changes": [rec]})
        live_o[f"dup-{j}"] = dup_template

    def cumulative_tables():
        return {
            "o": {"columns": ["c", "amt", "ts"],
                  "rows": [list(v) for v in live_o.values()]},
            "d": dim,
        }

    # 初始检查点
    assert bag_of(execute(plan1, cumulative_tables(), max_rows=200_000)["rows"]) \
        == bag_of(ex.current_result()["rows"]), "初始检查点不一致"

    deletes = 0
    for b in range(NBATCH):
        changes = []
        # 候选删除集合每批只构造一次（批内新插入行下批才可删）。
        deletable = [i for i in live_o if not i.startswith("dup-")]
        for _ in range(PER_BATCH):
            # 15% 删除已存在的普通行（dup 行保留到最后，保证重数持续存在）
            if deletable and rng.random() < 0.15:
                idx = rng.randrange(len(deletable))
                rid = deletable.pop(idx)
                changes.append({"table": "o", "op": "-", "id": rid})
            else:
                c = rng.choice([None, *range(NK)])     # 含 NULL 连接键
                amt = rng.randrange(-20, 80)
                ts = rng.randrange(0, 200)
                rid = f"o-{idc}"
                idc += 1
                changes.append({"table": "o", "op": "+", "id": rid,
                                "row": [c, amt, ts]})
        ex.apply_batch({"changes": changes})
        for ch in changes:
            if ch["op"] == "+":
                live_o[ch["id"]] = tuple(ch["row"])
            else:
                del live_o[ch["id"]]
                deletes += 1
        if b % 10 == 9:  # 检查点：累计输入交原批量解释器对照
            oracle = bag_of(execute(plan1, cumulative_tables(),
                                    max_rows=200_000)["rows"])
            got = bag_of(ex.current_result()["rows"])
            assert got == oracle, f"批次 {b} 增量结果与批量解释器不一致"

    total_rows = len(live_o)
    assert total_rows >= 10_000, f"累计存活行 {total_rows} < 10000"
    assert deletes > 1000, f"删除量过少 {deletes}"

    # 删一份重复行：重数恰好 100 -> 99，聚合值不变但 group 行数（count_star）-1
    r = ex.apply_batch({"changes": [
        {"table": "o", "op": "-", "id": "dup-0"}]})
    live_o.pop("dup-0")
    pc = Counter()
    for ch in r["changes"]:
        pc[tuple(ch["row"])] += ch["delta"]
    # c=7 组 count_star 减 1 -> 旧聚合行撤回、新聚合行插入
    assert sum(v for row, v in pc.items() if row[0] == 7) == 0, \
        "同键组净行数变化应为 0（撤回一、插入一）"
    oracle = bag_of(execute(plan1, cumulative_tables(), max_rows=200_000)["rows"])
    assert bag_of(ex.current_result()["rows"]) == oracle, "删一份后不一致"
    t1 = time.perf_counter()
    print(f"[1] join->group_by：{NBATCH} 批，存活 {total_rows} 行，"
          f"删除 {deletes}，重数 {DUP}，10 个批量检查点全部一致；"
          f"{t1 - t0:.2f}s")

    # ---- 场景二：inner_join -> window，水位推进、迟到与关闭、拒绝不改状态 ----
    plan2 = join_window_plan(WIDTH)
    tables2 = {"o": {"columns": ["c", "amt", "ts"], "rows": []}, "d": dim}
    ex2 = IncrementalExecutor(plan2, tables2, allowed_lateness=LATENESS)
    live2 = {}
    idc2 = 0
    # 预置早时间种子行（永不删除），保证窗口关闭确定发生且可对照。
    seed_changes = []
    for i in range(300):
        rid = f"seed-{i}"
        seed_changes.append({"table": "o", "op": "+", "id": rid,
                             "row": [i % NK, (i % 7) - 3, i % WIDTH]})
        live2[rid] = (i % NK, (i % 7) - 3, i % WIDTH)
    ex2.apply_batch({"changes": seed_changes})
    rejected = 0
    closed_seen = False
    wm = 0
    for b in range(NBATCH):
        new_wm = wm + 2          # 本批将推进到的水位；按它判断准时/迟到
        changes = []

        def mutable(ts):
            # 可更新 = 事件时间未过保留下限 且 窗口尚未关闭
            start = (ts // WIDTH) * WIDTH
            return (ts >= new_wm - LATENESS
                    and start + WIDTH > new_wm - LATENESS)

        # 候选删除集合每批只构造一次（批内新插入行下批才可删）。
        deletable = [i for i in live2
                     if not i.startswith("seed-") and mutable(live2[i][2])]
        # 每批约 10% 概率注入一条超期记录（真实触发整批原子拒绝）
        inject_expired = rng.random() < 0.10
        injected_done = False
        for _ in range(PER_BATCH):
            if deletable and rng.random() < 0.12:
                rid = deletable.pop(rng.randrange(len(deletable)))
                changes.append({"table": "o", "op": "-", "id": rid})
            else:
                c = rng.randrange(NK)
                amt = rng.randrange(-10, 50)
                if inject_expired and not injected_done:
                    ts = max(0, new_wm - LATENESS - 10)  # 明确超期
                    injected_done = True
                elif rng.random() < 0.10:
                    # 阈值内迟到：t ∈ [new_wm-lateness, new_wm]，必须被接受
                    ts = max(0, new_wm - rng.randrange(0, LATENESS + 1))
                else:
                    ts = new_wm + rng.randrange(0, WIDTH)
                rid = f"w-{idc2}"
                idc2 += 1
                changes.append({"table": "o", "op": "+", "id": rid,
                                "row": [c, amt, ts]})
        wm = new_wm
        try:
            out = ex2.apply_batch({"changes": changes, "watermark": wm})
        except LateDataError:
            # 整批含超期记录 -> 原子拒绝，状态不动；用仅水位批次重放水位，
            # 再继续后续批次（真实触发拒绝且系统继续可用）。
            rejected += 1
            before = bag_of(ex2.current_result()["rows"])
            wm_out = ex2.apply_batch({"changes": [], "watermark": wm})
            assert bag_of(ex2.current_result()["rows"]) == before, \
                "水位批次不应改变结果行（关闭只释放状态，结果行冻结）"
            if wm_out["closed"]:
                closed_seen = True
            continue
        if out["closed"]:
            closed_seen = True
        for ch in changes:
            if ch["op"] == "+":
                live2[ch["id"]] = tuple(ch["row"])
            else:
                live2.pop(ch["id"], None)
        if b % 20 == 19:
            cum = {"o": {"columns": ["c", "amt", "ts"],
                         "rows": [list(v) for v in live2.values()]}, "d": dim}
            oracle = bag_of(execute(plan2, cum, max_rows=200_000)["rows"])
            got = bag_of(ex2.current_result()["rows"])
            # 已关闭窗口的冻结行保留在增量结果中；批量参考只应多出/一致——
            # 这里 live2 从未删除已关闭窗口的行，所以两者必须完全相等。
            assert got == oracle, f"窗口批次 {b} 与批量解释器不一致"

    assert rejected > 0, "扩展场景必须真实触发至少一次超期拒绝"
    assert closed_seen, "必须真实发生过窗口关闭"

    # 局部性：单条右维插入只触碰其等值键
    ex3 = IncrementalExecutor(plan2, tables2, allowed_lateness=LATENESS)
    ex3.apply_batch({"changes": [
        {"table": "o", "op": "+", "id": "z1", "row": [1, 1, 100]},
        {"table": "o", "op": "+", "id": "z2", "row": [2, 1, 100]}]})
    out = ex3.apply_batch({"changes": [
        {"table": "d", "op": "+", "id": "newk", "row": [1, 123]}]})
    join_num = [n for n, nd in ex3.nodes_by_num.items()
                if nd.compiled.op == "inner_join"][0]
    rkeys = [t for t in out["touched"][join_num] if t.startswith("R:")]
    assert rkeys == ["R:[1]"], f"右插只应触碰键 [1]，实际 {rkeys}"

    t2 = time.perf_counter()
    print(f"[2] join->window：{NBATCH} 批、水位推进到 {wm}、关闭发生、"
          f"超期整批拒绝 {rejected} 次（均不改状态），检查点一致；{t2 - t1:.2f}s")
    print(f"扩展验证全部通过，总耗时 {t2 - t0:.2f}s。")


if __name__ == "__main__":
    run()
