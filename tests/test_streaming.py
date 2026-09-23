"""增量解释器测试（带行 ID 增删流、窗口、水位、快照、原子批）。

期望值来源（不调用被测增量求解器）：

1. **手工差量推导**：核心用例逐层写出批前/批后袋与应输出的 ``-``/``+``；
2. **原批量解释器** :func:`rel3vl.execute`：把每批的**累计输入**整体重算，
   与增量处理器的当前结果袋、changelog 累计袋对照（这是题面指定的对照方式，
   批量解释器与增量求解器是两套独立代码路径）。
"""

from __future__ import annotations

import random
import unittest
from collections import Counter

from rel3vl import (
    ExpressionError,
    MAX_STREAM_NODES,
    MAX_STREAM_ROWS,
    StreamError,
    execute,
)
from rel3vl.streaming import IncrementalProcessor


# ------------------------------------------------------------ 计划小工具

def scan(t, a=None):
    n = {"op": "scan", "table": t}
    if a is not None:
        n["as"] = a
    return n


def col(a, c):
    return {"kind": "col", "table": a, "column": c}


def lit(v):
    return {"kind": "lit", "value": v}


def cmp(op, a, b):
    return {"kind": "cmp", "op": op, "left": a, "right": b}


def ins(cid, row):
    return {"id": cid, "op": "insert", "row": row}


def dele(cid):
    return {"id": cid, "op": "delete"}


def bag(proc):
    return Counter(tuple(r) for r in proc.current_result())


def ch_bag(changes):
    out = Counter()
    for c in changes:
        out[tuple(c["row"])] += c["mult"] if c["op"] == "+" else -c["mult"]
    return out


def run_cumulative(plan, inputs, live):
    tables = {t: {"columns": cs, "rows": [list(r) for r in live[t].values()]}
              for t, cs in inputs.items()}
    return Counter(tuple(r) for r in execute(
        plan, tables, max_rows=MAX_STREAM_ROWS, max_nodes=MAX_STREAM_NODES)["rows"])


# ------------------------------------------------------------ 内连接 / 袋重数

class TestInnerJoinBag(unittest.TestCase):
    def setUp(self):
        self.plan = {
            "op": "inner_join", "left": scan("l", "l"), "right": scan("r", "r"),
            "on": cmp("=", col("l", "k"), col("r", "k"))}
        self.inputs = {"l": ["k", "v"], "r": ["k", "w"]}

    def test_duplicate_rows_count_with_multiplicity(self):
        p = IncrementalProcessor(self.plan, self.inputs)
        # 左两行相同内容（不同 id），右三行相同内容：输出 2*3=6 重
        r = p.apply_batch({"changes": {
            "l": [ins("l1", [1, "a"]), ins("l2", [1, "a"])],
            "r": [ins("r1", [1, "x"]), ins("r2", [1, "x"]), ins("r3", [1, "x"])]}})
        self.assertEqual(ch_bag(r["changes"]),
                         Counter({(1, "a", 1, "x"): 6}))
        # 删除其中一份左行：6 -> 3，只撤回 3 重
        r = p.apply_batch({"changes": {"l": [dele("l1")]}})
        self.assertEqual(ch_bag(r["changes"]),
                         Counter({(1, "a", 1, "x"): -3}))
        self.assertEqual(bag(p), Counter({(1, "a", 1, "x"): 3}))

    def test_null_join_keys_never_match(self):
        p = IncrementalProcessor(self.plan, self.inputs)
        r = p.apply_batch({"changes": {
            "l": [ins("l1", [None, "a"]), ins("l2", [1, "b"])],
            "r": [ins("r1", [None, "x"]), ins("r2", [1, "y"])]}})
        # NULL=NULL 为 UNKNOWN：只有 (1,b)-(1,y) 一对
        self.assertEqual(ch_bag(r["changes"]),
                         Counter({(1, "b", 1, "y"): 1}))
        self.assertEqual(bag(p), Counter({(1, "b", 1, "y"): 1}))

    def test_delete_right_propagates_to_paired_left(self):
        p = IncrementalProcessor(self.plan, self.inputs)
        p.apply_batch({"changes": {
            "l": [ins("l1", [1, "a"])],
            "r": [ins("r1", [1, "x"]), ins("r2", [1, "y"])]}})
        r = p.apply_batch({"changes": {"r": [dele("r1")]}})
        self.assertEqual(ch_bag(r["changes"]),
                         Counter({(1, "a", 1, "x"): -1}))

    def test_cross_type_families_raise_like_batch(self):
        # 等值列两侧并存互不兼容类型族时即报错，即使值不同（与批量一致）。
        p = IncrementalProcessor(self.plan, self.inputs)
        p.apply_batch({"changes": {
            "l": [ins("l1", [1, "a"])], "r": [ins("r1", [1, "x"])]}})
        with self.assertRaises(ExpressionError):
            p.apply_batch({"changes": {"r": [ins("r2", ["z", "w"])]}})
        self.assertEqual(bag(p), Counter({(1, "a", 1, "x"): 1}))
        p2 = IncrementalProcessor(self.plan, self.inputs)
        with self.assertRaises(ExpressionError):
            p2.apply_batch({"changes": {
                "l": [ins("b1", [True, "a"])],
                "r": [ins("i1", [1, "x"])]}})

    def test_touched_keys_do_not_scan_unrelated_buckets(self):
        p = IncrementalProcessor(self.plan, self.inputs)
        p.apply_batch({"changes": {
            "l": [ins("l1", [1, "a"]), ins("l2", [2, "b"])],
            "r": [ins("r1", [1, "x"]), ins("r2", [2, "y"]), ins("r3", [3, "z"])]}})
        r = p.apply_batch({"changes": {"l": [ins("l9", [1, "q"])]}})
        jnode = [k for k, v in r["touched"].items()
                 if v.get("op") == "inner_join"][0]
        touched = r["touched"][jnode]
        # 只触碰等值键 1 的桶；键 2/3 既不在 left_keys 也不在 right_keys
        self.assertEqual(touched["left_keys"], [[1]])
        self.assertEqual(touched["right_keys"], [[1]])


# ------------------------------------------------------------ 左外连接 / PAD

class TestLeftJoinPad(unittest.TestCase):
    def setUp(self):
        self.plan = {
            "op": "left_join", "left": scan("o", "o"), "right": scan("c", "c"),
            "on": cmp("=", col("o", "cid"), col("c", "cid"))}
        self.inputs = {"o": ["oid", "cid"], "c": ["cid", "name"]}

    def test_zero_to_one_withdraws_pad_row(self):
        p = IncrementalProcessor(self.plan, self.inputs)
        r = p.apply_batch({"changes": {"o": [ins("o1", [10, 9])]}})
        self.assertEqual(ch_bag(r["changes"]),
                         Counter({(10, 9, None, None): 1}))
        # 补入迟到右行：匹配数 0->1，必须撤回补 NULL 行（- 与 + 成对）
        r = p.apply_batch({"changes": {"c": [ins("c1", [9, "A"])]}})
        self.assertEqual(
            r["changes"],
            [{"op": "-", "row": [10, 9, None, None], "mult": 1},
             {"op": "+", "row": [10, 9, 9, "A"], "mult": 1}])
        self.assertEqual(bag(p), Counter({(10, 9, 9, "A"): 1}))

    def test_delete_last_matching_right_repads(self):
        p = IncrementalProcessor(self.plan, self.inputs)
        p.apply_batch({"changes": {
            "o": [ins("o1", [10, 9])], "c": [ins("c1", [9, "A"])]}})
        # 删除最后（也是唯一）一条匹配：1->0，补 NULL 行重新出现
        r = p.apply_batch({"changes": {"c": [dele("c1")]}})
        self.assertEqual(
            r["changes"],
            [{"op": "-", "row": [10, 9, 9, "A"], "mult": 1},
             {"op": "+", "row": [10, 9, None, None], "mult": 1}])
        self.assertEqual(bag(p), Counter({(10, 9, None, None): 1}))

    def test_delete_one_of_two_matches_no_pad(self):
        p = IncrementalProcessor(self.plan, self.inputs)
        p.apply_batch({"changes": {
            "o": [ins("o1", [10, 9])],
            "c": [ins("c1", [9, "A"]), ins("c2", [9, "B"])]}})
        r = p.apply_batch({"changes": {"c": [dele("c1")]}})
        # 匹配数 2->1：只撤回该配对，不产生 PAD
        self.assertEqual(ch_bag(r["changes"]),
                         Counter({(10, 9, 9, "A"): -1}))


# ------------------------------------------------------------ 分组聚合

class TestAggregate(unittest.TestCase):
    def setUp(self):
        self.plan = {
            "op": "aggregate", "input": scan("t", "t"),
            "groups": [{"name": "g", "expr": col("t", "g")}],
            "aggs": [
                {"name": "c", "func": "count"},
                {"name": "cv", "func": "count", "arg": col("t", "v")},
                {"name": "s", "func": "sum", "arg": col("t", "v")},
                {"name": "mn", "func": "min", "arg": col("t", "v")}]}
        self.inputs = {"t": ["g", "v"]}

    def test_group_update_emits_withdraw_and_insert(self):
        p = IncrementalProcessor(self.plan, self.inputs)
        p.apply_batch({"changes": {"t": [ins("t1", ["a", 1]), ins("t2", ["a", 2])]}})
        # 迟到更新（删旧插新，同批）：a 组 sum 3 -> 6，必须撤回旧聚合行
        r = p.apply_batch({"changes": {"t": [dele("t2"), ins("t3", ["a", 5])]}})
        self.assertEqual(
            r["changes"],
            [{"op": "-", "row": ["a", 2, 2, 3, 1], "mult": 1},
             {"op": "+", "row": ["a", 2, 2, 6, 1], "mult": 1}])

    def test_count_expr_ignores_null_but_count_star_not(self):
        p = IncrementalProcessor(self.plan, self.inputs)
        r = p.apply_batch({"changes": {"t": [
            ins("t1", ["a", None]), ins("t2", ["a", 5])]}})
        self.assertEqual(ch_bag(r["changes"]),
                         Counter({("a", 2, 1, 5, 5): 1}))

    def test_last_row_removes_group(self):
        p = IncrementalProcessor(self.plan, self.inputs)
        p.apply_batch({"changes": {"t": [ins("t1", ["a", 1])]}})
        r = p.apply_batch({"changes": {"t": [dele("t1")]}})
        self.assertEqual(r["changes"],
                         [{"op": "-", "row": ["a", 1, 1, 1, 1], "mult": 1}])
        self.assertEqual(bag(p), Counter())

    def test_empty_global_aggregate_is_one_zero_row(self):
        plan = {"op": "aggregate", "input": scan("t", "t"),
                "aggs": [{"name": "c", "func": "count"},
                         {"name": "s", "func": "sum", "arg": col("t", "v")}]}
        p = IncrementalProcessor(plan, {"t": ["v"]})
        self.assertEqual(p.current_result(), [[0, None]])
        r = p.apply_batch({"changes": {"t": [ins("t1", [4])]}})
        # 空组零行被撤回，真实组行新增（不是只追加）
        self.assertEqual(r["changes"],
                         [{"op": "-", "row": [0, None], "mult": 1},
                          {"op": "+", "row": [1, 4], "mult": 1}])
        r = p.apply_batch({"changes": {"t": [dele("t1")]}})
        self.assertEqual(r["changes"],
                         [{"op": "-", "row": [1, 4], "mult": 1},
                          {"op": "+", "row": [0, None], "mult": 1}])


# ------------------------------------------------------------ 窗口与水位

class TestWindow(unittest.TestCase):
    def setUp(self):
        # window -> 每窗口 count
        self.inputs = {"e": ["ts", "v"]}
        self.plan = {
            "op": "aggregate",
            "input": {"op": "window", "input": scan("e", "e"),
                      "time_col": col("e", "ts"), "size": 5},
            "groups": [{"name": "ws", "expr": col("", "window_start")}],
            "aggs": [{"name": "c", "func": "count"}]}

    def test_tumbling_half_open_boundary(self):
        p = IncrementalProcessor(self.plan, self.inputs)
        r = p.apply_batch({"changes": {"e": [
            ins("e1", [4, 1]), ins("e2", [5, 1])]}})
        # [0,5) 含 t=4 不含 t=5；[5,10) 含 t=5
        self.assertEqual(ch_bag(r["changes"]),
                         Counter({(0, 1): 1, (5, 1): 1}))

    def test_watermark_closes_window_once(self):
        p = IncrementalProcessor(self.plan, self.inputs)
        p.apply_batch({"watermark": 0, "changes": {"e": [ins("e1", [1, 1])]}})
        r = p.apply_batch({"watermark": 5})   # end=5 <= 5：密封 [0,5)
        self.assertEqual(r["sealed_windows"],
                         [{"node": 1, "window_start": 0, "window_end": 5}])
        # 纯水位重复推进不重复发布
        r = p.apply_batch({"watermark": 5})
        self.assertEqual(r["sealed_windows"], [])
        r = p.apply_batch({"watermark": 6})
        self.assertEqual(r["sealed_windows"], [])

    def test_event_on_boundary_stays_in_new_window(self):
        p = IncrementalProcessor(self.plan, self.inputs)
        p.apply_batch({"watermark": 5})
        # t=5 属于 [5,10)，[0,5) 已密封但 t=5 不触碰它：允许
        r = p.apply_batch({"changes": {"e": [ins("e1", [5, 1])]}})
        self.assertEqual(ch_bag(r["changes"]), Counter({(5, 1): 1}))

    def test_sliding_window_duplicates_row(self):
        inputs = {"e": ["ts", "v"]}
        plan = {
            "op": "aggregate",
            "input": {"op": "window", "input": scan("e", "e"),
                      "time_col": col("e", "ts"), "size": 4, "slide": 2},
            "groups": [{"name": "ws", "expr": col("", "window_start")}],
            "aggs": [{"name": "c", "func": "count"}]}
        p = IncrementalProcessor(plan, inputs)
        r = p.apply_batch({"changes": {"e": [ins("e1", [3, 1])]}})
        # t=3 属于 [0,4) 与 [2,6)（非负轴上无 [-2,2)）
        self.assertEqual(ch_bag(r["changes"]), Counter({(0, 1): 1, (2, 1): 1}))

    def test_late_within_threshold_allowed_with_replace(self):
        inputs = {"e": ["ts", "v"]}
        plan = {
            "op": "aggregate",
            "input": {"op": "window", "input": scan("e", "e"),
                      "time_col": col("e", "ts"), "size": 10,
                      "allowed_lateness": 3},
            "groups": [{"name": "ws", "expr": col("", "window_start")}],
            "aggs": [{"name": "c", "func": "count"}]}
        p = IncrementalProcessor(plan, inputs)
        p.apply_batch({"watermark": 0, "changes": {"e": [ins("e1", [2, 1])]}})
        # wm=12：阈值内最晚 t = wm - lateness - size = -1；[0,10) 已密封
        p.apply_batch({"watermark": 12})
        # wm=12 时 [10,20) 未密封（end 20 > 9）；t=11 迟到但允许
        r = p.apply_batch({"changes": {"e": [ins("e2", [11, 1])]}})
        self.assertEqual(ch_bag(r["changes"]), Counter({(10, 1): 1}))

    def test_late_beyond_retention_rejected_state_untouched(self):
        inputs = {"e": ["ts", "v"]}
        plan = {"op": "window", "input": scan("e", "e"),
                "time_col": col("e", "ts"), "size": 10, "allowed_lateness": 2}
        p = IncrementalProcessor(plan, inputs)
        p.apply_batch({"watermark": 0, "changes": {"e": [ins("e1", [1, 1])]}})
        p.apply_batch({"watermark": 12})  # 密封 [0,10)
        before = bag(p)
        with self.assertRaises(StreamError):
            p.apply_batch({"changes": {"e": [ins("e2", [1, 1])]}})
        # 状态、水位完全不变
        self.assertEqual(bag(p), before)
        self.assertEqual(p.watermark, 12)


# ------------------------------------------------------------ 原子批 / 输入校验

class TestAtomicBatch(unittest.TestCase):
    def setUp(self):
        self.plan = {
            "op": "aggregate", "input": scan("t", "t"),
            "groups": [{"name": "g", "expr": col("t", "g")}],
            "aggs": [{"name": "c", "func": "count"}]}
        self.inputs = {"t": ["g"]}

    def test_invalid_batch_leaves_state_unchanged(self):
        p = IncrementalProcessor(self.plan, self.inputs)
        p.apply_batch({"changes": {"t": [ins("t1", ["a"])]}})
        before = bag(p)
        bad = [
            {"changes": {"t": [dele("nope")]}},               # 未知 id
            {"changes": {"t": [ins("t1", ["b"])]}},           # 重复 id
            {"changes": {"t": [{"id": "x", "op": "bogus"}]}}, # 非法 op
            {"changes": {"t": [{"id": "x", "op": "insert", "row": ["a", 1]}]}},
            {"changes": {"nope": [ins("x", [1])]}},           # 未声明表
            {"watermark": -1},
            {"changes": {"t": [ins(True, ["a"])]}},           # bool id
        ]
        for b in bad:
            with self.assertRaises(StreamError):
                p.apply_batch(b)
            self.assertEqual(bag(p), before, msg=f"批 {b} 改动了状态")

    def test_watermark_monotone(self):
        p = IncrementalProcessor(self.plan, self.inputs)
        p.apply_batch({"watermark": 5})
        with self.assertRaises(StreamError):
            p.apply_batch({"watermark": 4})

    def test_batch_with_data_and_watermark_publishes_once(self):
        """同批：删除最后匹配 + 补入迟到行 + 水位关闭，结果只发布一次。"""
        inputs = {"e": ["ts", "g"]}
        plan = {
            "op": "aggregate",
            "input": {"op": "window", "input": scan("e", "e"),
                      "time_col": col("e", "ts"), "size": 10},
            "groups": [{"name": "ws", "expr": col("", "window_start")},
                       {"name": "g", "expr": col("e", "g")}],
            "aggs": [{"name": "c", "func": "count"}]}
        p = IncrementalProcessor(plan, inputs)
        p.apply_batch({"watermark": 0, "changes": {"e": [
            ins("e1", [1, "a"]), ins("e2", [11, "b"])]}})
        # 一批内：删除 [0,10) 的最后一行、补入 [10,20) 迟到行、水位推进到 10
        r = p.apply_batch({"watermark": 10, "changes": {"e": [
            dele("e1"), ins("e3", [12, "b"])]}})
        # 恰好一次 changelog；[0,10)/a 撤回，[10,20)/b 1->2
        self.assertEqual(ch_bag(r["changes"]),
                         Counter({(0, "a", 1): -1, (10, "b", 1): -1,
                                  (10, "b", 2): 1}))
        self.assertEqual(r["sealed_windows"],
                         [{"node": 1, "window_start": 0, "window_end": 10}])
        self.assertEqual(r["batch_seq"], 2)


# ------------------------------------------------------------ 快照

class TestSnapshot(unittest.TestCase):
    def test_restore_replays_identical_changelog(self):
        inputs = {"e": ["ts", "g", "v"]}
        plan = {
            "op": "aggregate",
            "input": {"op": "window", "input": scan("e", "e"),
                      "time_col": col("e", "ts"), "size": 6, "slide": 2},
            "groups": [{"name": "g", "expr": col("e", "g")},
                       {"name": "ws", "expr": col("", "window_start")}],
            "aggs": [{"name": "c", "func": "count"},
                     {"name": "s", "func": "sum", "arg": col("e", "v")}]}
        rng = random.Random(20260924)
        batches = []
        nid = 0
        for b in range(12):
            ops = []
            for _ in range(4):
                nid += 1
                ops.append(ins(f"e{nid}", [b * 2 + rng.randrange(3),
                                           rng.choice(["a", "b"]),
                                           rng.choice([1, 2, None])]))
            batches.append({"watermark": b * 2, "changes": {"e": ops}})
        p1 = IncrementalProcessor(plan, inputs)
        outs1 = [p1.apply_batch(b) for b in batches[:6]]
        snap = p1.dumps_snapshot()
        p2 = IncrementalProcessor(plan, inputs).loads_snapshot(snap)
        self.assertEqual(bag(p1), bag(p2))
        self.assertEqual((p1.watermark, p1.batch_seq),
                         (p2.watermark, p2.batch_seq))
        for b in batches[6:]:
            r1, r2 = p1.apply_batch(b), p2.apply_batch(b)
            self.assertEqual(r1["changes"], r2["changes"])
            self.assertEqual(r1["sealed_windows"], r2["sealed_windows"])
        self.assertEqual(bag(p1), bag(p2))


# ------------------------------------------------------------ 确定性

class TestDeterminism(unittest.TestCase):
    def test_same_input_same_output_across_runs(self):
        inputs = {"l": ["k"], "r": ["k"]}
        plan = {"op": "left_join", "left": scan("l", "l"),
                "right": scan("r", "r"),
                "on": cmp("=", col("l", "k"), col("r", "k"))}

        def run():
            p = IncrementalProcessor(plan, inputs)
            log = []
            log.append(p.apply_batch({"changes": {
                "l": [ins("l1", [1]), ins("l2", [1]), ins("l3", [2])],
                "r": [ins("r1", [1])]}})["changes"])
            log.append(p.apply_batch({"changes": {
                "r": [ins("r2", [2]), dele("r1")]}})["changes"])
            return log, p.dumps_snapshot()
        a = run()
        b = run()
        self.assertEqual(a, b)


# ------------------------------------------------------------ 累计批量对照（随机）

class TestDifferentialAgainstBatch(unittest.TestCase):
    """每批把累计输入交给原批量解释器，对照增量当前袋与 changelog 累计袋。"""

    def _make_plan(self, variant):
        if variant == "lj_agg":
            inputs = {"o": ["oid", "cid", "amt"], "c": ["cid", "city"]}
            plan = {
                "op": "aggregate",
                "input": {"op": "left_join", "left": scan("o", "o"),
                          "right": scan("c", "c"),
                          "on": cmp("=", col("o", "cid"), col("c", "cid"))},
                "groups": [{"name": "city", "expr": col("c", "city")}],
                "aggs": [{"name": "n", "func": "count"},
                         {"name": "s", "func": "sum", "arg": col("o", "amt")}]}
        elif variant == "win_agg":
            inputs = {"e": ["ts", "g", "v"]}
            plan = {
                "op": "aggregate",
                "input": {"op": "window", "input": scan("e", "e"),
                          "time_col": col("e", "ts"), "size": 8,
                          "allowed_lateness": 2},
                "groups": [{"name": "g", "expr": col("e", "g")},
                           {"name": "ws", "expr": col("", "window_start")}],
                "aggs": [{"name": "n", "func": "count"},
                         {"name": "s", "func": "sum", "arg": col("e", "v")}]}
        else:  # 三算子：left_join -> inner_join -> window -> aggregate
            inputs = {"a": ["k", "ts"], "b": ["k", "k2"], "c": ["k2", "v"]}
            j1 = {"op": "left_join", "left": scan("a", "a"),
                  "right": scan("b", "b"),
                  "on": cmp("=", col("a", "k"), col("b", "k"))}
            j2 = {"op": "inner_join", "left": j1, "right": scan("c", "c"),
                  "on": cmp("=", col("b", "k2"), col("c", "k2"))}
            plan = {
                "op": "aggregate",
                "input": {"op": "window", "input": j2,
                          "time_col": col("a", "ts"), "size": 10,
                          "allowed_lateness": 3},
                "groups": [{"name": "ws", "expr": col("", "window_start")}],
                "aggs": [{"name": "n", "func": "count"},
                         {"name": "s", "func": "sum", "arg": col("c", "v")}]}
        return inputs, plan

    def test_differential_small_domain(self):
        """小参考域完整核对：固定种子、短批序列，逐步双对照。"""
        for variant in ("lj_agg", "win_agg", "three_way"):
            inputs, plan = self._make_plan(variant)
            rng = random.Random({"lj_agg": 1, "win_agg": 2,
                                 "three_way": 3}[variant])
            p = IncrementalProcessor(plan, inputs)
            live = {t: {} for t in inputs}
            applied = Counter()
            for r0 in p.current_result():
                applied[tuple(r0)] += 1
            wm = 0
            for b in range(24):
                changes = {}
                for t, cs in inputs.items():
                    ops = []
                    for _ in range(rng.randrange(1, 4)):
                        if live[t] and rng.random() < 0.4:
                            did = rng.choice(sorted(live[t]))
                            if not any(o["id"] == did for o in ops):
                                ops.append(dele(did))
                                continue
                        row = self._rand_row(variant, t, rng, wm)
                        cid = f"{t[0]}{b}_{len(ops)}_{rng.randrange(10**9)}"
                        ops.append(ins(cid, row))
                    if ops:
                        changes[t] = ops
                new_wm = wm + (rng.randrange(1, 4) if rng.random() < 0.5 else 0)
                try:
                    r = p.apply_batch({"watermark": new_wm, "changes": changes})
                except StreamError:
                    # 合法拒绝（超保留期）：状态必须等于批量重算结果
                    self.assertEqual(bag(p), run_cumulative(plan, inputs, live))
                    self.assertEqual(p.watermark, wm)
                    continue
                wm = new_wm
                for t, ops in changes.items():
                    for o in ops:
                        if o["op"] == "insert":
                            live[t][o["id"]] = tuple(o["row"])
                        else:
                            live[t].pop(o["id"], None)
                applied.update(ch_bag(r["changes"]))
                expect = run_cumulative(plan, inputs, live)
                self.assertEqual(bag(p), expect,
                                 msg=f"{variant} b={b} 当前袋不符")
                self.assertEqual(Counter({k: v for k, v in applied.items() if v}),
                                 expect, msg=f"{variant} b={b} changelog 累计袋不符")

    @staticmethod
    def _rand_row(variant, t, rng, wm):
        if variant == "lj_agg":
            if t == "o":
                return [rng.randrange(4), rng.choice([1, 2, None]),
                        rng.choice([1, 5, None])]
            return [rng.choice([1, 2]), rng.choice(["X", "Y", None])]
        if variant == "win_agg":
            return [max(0, wm + rng.randrange(-3, 5)), rng.choice(["a", "b"]),
                    rng.choice([1, 2, None])]
        if t == "a":
            return [rng.choice([0, 1, None]), max(0, wm + rng.randrange(-4, 5))]
        if t == "b":
            return [rng.choice([0, 1]), rng.randrange(3)]
        return [rng.randrange(3), rng.choice([1, 2, None])]


# ------------------------------------------------------------ 外连接嵌套聚合再接窗口

class TestJoinAggWindowChain(unittest.TestCase):
    """题面指定的嵌套计划：left_join -> aggregate -> window -> aggregate。

    内聚合按 (city, ts) 汇总，窗口附在聚合结果上，外聚合按 (city, 窗口起点)。
    客户补入/删除必须逐层产生撤回+新增，而不是只追加。
    """

    PLAN = {
        "op": "aggregate",
        "groups": [{"name": "city", "expr": col("", "city")},
                   {"name": "ws", "expr": col("", "window_start")}],
        "aggs": [{"name": "tot", "func": "sum", "arg": col("", "s")}],
        "input": {
            "op": "window", "size": 10,
            "time_col": col("", "ts"),
            "input": {
                "op": "aggregate",
                "groups": [{"name": "city", "expr": col("c", "city")},
                           {"name": "ts", "expr": col("o", "ts")}],
                "aggs": [{"name": "s", "func": "sum",
                          "arg": col("o", "amt")}],
                "input": {
                    "op": "left_join", "left": scan("o", "o"),
                    "right": scan("c", "c"),
                    "on": cmp("=", col("o", "cid"), col("c", "cid"))}}}}
    INPUTS = {"o": ["oid", "cid", "ts", "amt"], "c": ["cid", "city"]}

    def test_customer_arrival_and_death_propagate_as_pairs(self):
        p = IncrementalProcessor(self.PLAN, self.INPUTS)
        r = p.apply_batch({"changes": {"o": [
            ins("o1", [1, 9, 2, 10]), ins("o2", [2, 9, 4, 5]),
            ins("o3", [3, None, 6, 7])]}})
        # 两笔 cid=9 无客户 -> city NULL 的 [0,10) 组 sum=15；
        # cid NULL 订单也补 NULL -> 同组 sum=22
        self.assertEqual(ch_bag(r["changes"]),
                         Counter({(None, 0, 22): 1}))
        # 迟到客户 c9：o1/o2 从 NULL 城市移到 X：两组各发 -/+
        r = p.apply_batch({"changes": {"c": [ins("c9", [9, "X"])]}})
        self.assertEqual(ch_bag(r["changes"]),
                         Counter({(None, 0, 22): -1, (None, 0, 7): 1,
                                  ("X", 0, 15): 1}))
        # 删除唯一客户：X 组消失，NULL 组恢复 22（撤回+新增成对）
        r = p.apply_batch({"changes": {"c": [dele("c9")]}})
        self.assertEqual(ch_bag(r["changes"]),
                         Counter({("X", 0, 15): -1, (None, 0, 7): -1,
                                  (None, 0, 22): 1}))


if __name__ == "__main__":
    unittest.main()
