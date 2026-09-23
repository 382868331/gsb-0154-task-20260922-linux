"""增删流增量解释器测试（快速回归）。

覆盖：
* 内/左外连接袋重数差量传播（0->1 撤回补 NULL、1->0 补回、删一份重复）；
* NULL 连接键、三值过滤、空组不输出、聚合撤回 + 新增（非只追加）；
* 事件时间滚动窗口、水位单调、阈值内迟到、关闭边界、超期拒绝且不改状态；
* 单批原子发布（批中任一非法记录整批不动）、快照恢复后变更流相同；
* 外连接->聚合->窗口嵌套计划的 touched 键局部性（不扫描无关组）；
* 与"每批累计输入交原批量 execute()"对照，并用完全独立的朴素参考
  （本文件内，不 import 被测核心算期望值）在小确定性域上完整核对。
"""

from __future__ import annotations

import json
import random
import unittest
from collections import Counter

from rel3vl import (
    ChangeError,
    IncrementalExecutor,
    LateDataError,
    WatermarkError,
    execute,
)


# ------------------------------------------------------------ 计划小工具

def col(t, c):
    return {"kind": "col", "table": t, "column": c}


def lit(v):
    return {"kind": "lit", "value": v}


def cmp(op, a, b):
    return {"kind": "cmp", "op": op, "left": a, "right": b}


def band(a, b):
    return {"kind": "and", "left": a, "right": b}


def scan(t, a=None):
    n = {"op": "scan", "table": t}
    if a is not None:
        n["as"] = a
    return n


def join(op, l, r, on):
    return {"op": op, "left": l, "right": r, "on": on}


def group_by(child, keys, aggs):
    return {"op": "group_by", "input": child, "keys": keys, "aggs": aggs}


def window(child, time_expr, width, keys, aggs):
    return {"op": "window", "input": child, "time": time_expr,
            "width": width, "keys": keys, "aggs": aggs}


def gkey(name, expr):
    return {"name": name, "expr": expr}


def agg(name, func, expr=None):
    d = {"name": name, "func": func}
    if expr is not None:
        d["expr"] = expr
    return d


def result_bag(ex):
    return Counter(tuple(r) for r in ex.current_result()["rows"])


# ----------------------------------------------- 独立朴素参考（不 import 核心）
#
# 与 tests/_fixtures.py 的朴素参考同样独立：直接解析计划 JSON，
# 行=元组、袋=Counter，代码路径与 rel3vl.engine / rel3vl.stream 完全不同。
# id_tables: {表名: (列名列表, {行id: 行元组})}

def _nev(expr, row):
    k = expr["kind"]
    if k == "lit":
        return expr["value"]
    if k == "col":
        return row[(expr["table"], expr["column"])]
    if k == "is_null":
        return _nev(expr["expr"], row) is None
    if k == "not":
        v = _nev(expr["expr"], row)
        return None if v is None else (not v)
    if k == "coalesce":
        for a in expr["args"]:
            v = _nev(a, row)
            if v is not None:
                return v
        return None
    if k in ("and", "or"):
        a = _nev(expr["left"], row)
        b = _nev(expr["right"], row)
        rank = {False: 0, None: 1, True: 2}
        return min((a, b), key=lambda x: rank[x]) if k == "and" \
            else max((a, b), key=lambda x: rank[x])
    a = _nev(expr["left"], row)
    b = _nev(expr["right"], row)
    if a is None or b is None:
        return None
    op = expr["op"]
    if op == "=":
        return a == b
    if op == "<>":
        return a != b
    return {"<": a < b, "<=": a <= b, ">": a > b, ">=": a >= b}[op]


def _agg_val(func, values, is_star=False):
    # values: 该组的度量袋 Counter
    if func == "count_star":
        return sum(values.values())
    nonnull = Counter()
    for v, m in values.items():
        if v is not None:
            nonnull[v] += m
    n = sum(nonnull.values())
    if func == "count":
        return n
    if func == "sum":
        return sum(v * m for v, m in nonnull.items()) if nonnull else None
    if func == "avg":
        return (sum(v * m for v, m in nonnull.items()) / n) if nonnull else None
    if func == "min":
        return min(nonnull) if nonnull else None
    if func == "max":
        return max(nonnull) if nonnull else None
    raise AssertionError(func)


def _nrun(node, tabs):
    op = node["op"]
    if op == "scan":
        cols, idrows = tabs[node["table"]]
        src = node.get("as", node["table"])
        return [(src, c) for c in cols], Counter(tuple(r) for r in idrows.values())
    if op in ("inner_join", "left_join"):
        ls, lb = _nrun(node["left"], tabs)
        rs, rb = _nrun(node["right"], tabs)
        out = Counter()
        rrows = list(rb.elements())
        for lt, lm in lb.items():
            for _ in range(lm):
                matched = 0
                for rt in rrows:
                    if _nev(node["on"], dict(zip(ls + rs, lt + rt))) is True:
                        out[lt + rt] += 1
                        matched += 1
                if op == "left_join" and matched == 0:
                    out[lt + (None,) * len(rs)] += 1
        return ls + rs, out
    if op == "union_all":
        ls, lb = _nrun(node["left"], tabs)
        _, rb = _nrun(node["right"], tabs)
        return ls, lb + rb
    sch, bag = _nrun(node["input"], tabs)
    if op == "filter":
        out = Counter()
        for tup, m in bag.items():
            if _nev(node["expr"], dict(zip(sch, tup))) is True:
                out[tup] += m
        return sch, out
    if op == "project":
        nsch = [(c.get("table", ""), c["name"]) for c in node["columns"]]
        out = Counter()
        for tup, m in bag.items():
            row = dict(zip(sch, tup))
            out[tuple(_nev(c["expr"], row) for c in node["columns"])] += m
        return nsch, out
    if op == "distinct":
        seen, out = set(), Counter()

        def tag(v):
            if v is None:
                return ("n",)
            if isinstance(v, bool):
                return ("b", v)
            if isinstance(v, (int, float)):
                return ("num", v)
            return ("s", type(v).__name__, v)

        for tup in bag:
            key = tuple(tag(v) for v in tup)
            if key not in seen:
                seen.add(key)
                out[tup] = 1
        return sch, out
    if op in ("group_by", "window"):
        return _nrun_agg(node, sch, bag)
    raise AssertionError(op)


def _nrun_agg(node, sch, bag):
    keys = node.get("keys", [])
    kfuncs = [(c.get("table", ""), c["name"], c["expr"]) for c in keys]
    abucket = {}
    is_win = node["op"] == "window"
    for tup, mult in bag.items():
        row = dict(zip(sch, tup))
        k = tuple(_nev(e, row) for _, _, e in kfuncs)
        if is_win:
            t = _nev(node["time"], row)
            start = (t // node["width"]) * node["width"]
            bk = (start, k)
        else:
            bk = k
        abucket.setdefault(bk, Counter())
        for a in node["aggs"]:
            pass
        abucket[bk][tuple(
            _nev(a["expr"], row) if "expr" in a else ("*",)
            for a in node["aggs"]
        )] += mult
    out = Counter()
    nsch = [(s, n) for s, n, _ in kfuncs]
    if is_win:
        nsch += [("", "window_start"), ("", "window_end")]
    nsch += [(a.get("table", ""), a["name"]) for a in node["aggs"]]
    for bk, measures in abucket.items():
        vals = []
        for i, a in enumerate(node["aggs"]):
            if a["func"] == "count_star":
                vals.append(_agg_val("count_star", measures))
            else:
                vals.append(_agg_val(a["func"],
                                     Counter({m[i]: mult for m, mult in measures.items()})))
        if is_win:
            start, k = bk
            out[tuple(k) + (start, start + node["width"]) + tuple(vals)] = 1
        else:
            out[tuple(bk) + tuple(vals)] = 1
    return nsch, out


def naive_bag(plan, id_tables):
    _, bag = _nrun(plan, id_tables)
    return bag


def published_counter(changes):
    c = Counter()
    for ch in changes:
        c[tuple(ch["row"])] += ch["delta"]
    return c


# ------------------------------------------------------------ 测试

class TestInnerJoinIncremental(unittest.TestCase):
    def setUp(self):
        self.tables = {
            "l": {"columns": ["k", "v"], "rows": []},
            "r": {"columns": ["k", "w"], "rows": []},
        }
        self.plan = join("inner_join", scan("l", "l"), scan("r", "r"),
                         cmp("=", col("l", "k"), col("r", "k")))

    def test_insert_match_and_bag_multiplicity(self):
        ex = IncrementalExecutor(self.plan, self.tables)
        r = ex.apply_batch({"changes": [
            {"table": "l", "op": "+", "id": "a", "row": [1, 10]},
            {"table": "r", "op": "+", "id": "x", "row": [1, 90]},
            {"table": "r", "op": "+", "id": "y", "row": [1, 91]},
        ]})
        self.assertEqual(published_counter(r["changes"]),
                         Counter({(1, 10, 1, 90): 1, (1, 10, 1, 91): 1}))
        self.assertEqual(r["seq"], 1)  # 整批发布一次

    def test_duplicate_rows_count_by_multiplicity(self):
        ex = IncrementalExecutor(self.plan, self.tables)
        changes = [{"table": "l", "op": "+", "id": f"a{i}", "row": [1, 10]}
                   for i in range(100)]
        changes.append({"table": "r", "op": "+", "id": "x", "row": [1, 90]})
        ex.apply_batch({"changes": changes})
        self.assertEqual(result_bag(ex)[(1, 10, 1, 90)], 100)
        # 删除其中一份：重数恰好降 1
        r = ex.apply_batch({"changes": [
            {"table": "l", "op": "-", "id": "a37"}]})
        self.assertEqual(published_counter(r["changes"]),
                         Counter({(1, 10, 1, 90): -1}))
        self.assertEqual(result_bag(ex)[(1, 10, 1, 90)], 99)

    def test_null_keys_never_match(self):
        ex = IncrementalExecutor(self.plan, self.tables)
        ex.apply_batch({"changes": [
            {"table": "l", "op": "+", "id": "a", "row": [None, 1]},
            {"table": "r", "op": "+", "id": "x", "row": [None, 9]},
        ]})
        self.assertEqual(result_bag(ex), Counter())
        # 迟到一条非 NULL 不影响 NULL 行
        ex.apply_batch({"changes": [
            {"table": "r", "op": "+", "id": "y", "row": [2, 9]}]})
        self.assertEqual(result_bag(ex), Counter())

    def test_delete_right_copy_of_duplicate_match(self):
        ex = IncrementalExecutor(self.plan, self.tables)
        ex.apply_batch({"changes": [
            {"table": "l", "op": "+", "id": "a", "row": [1, 10]},
            {"table": "r", "op": "+", "id": "x", "row": [1, 90]},
            {"table": "r", "op": "+", "id": "y", "row": [1, 90]},
        ]})
        self.assertEqual(result_bag(ex)[(1, 10, 1, 90)], 2)
        r = ex.apply_batch({"changes": [
            {"table": "r", "op": "-", "id": "x"}]})
        self.assertEqual(published_counter(r["changes"]),
                         Counter({(1, 10, 1, 90): -1}))
        self.assertEqual(result_bag(ex)[(1, 10, 1, 90)], 1)


class TestLeftJoinPadding(unittest.TestCase):
    def setUp(self):
        self.tables = {
            "l": {"columns": ["k", "v"], "rows": []},
            "r": {"columns": ["k", "w"], "rows": []},
        }
        self.plan = join("left_join", scan("l", "l"), scan("r", "r"),
                         cmp("=", col("l", "k"), col("r", "k")))

    def test_zero_to_one_withdraws_padded_rows(self):
        ex = IncrementalExecutor(self.plan, self.tables)
        ex.apply_batch({"changes": [
            {"table": "l", "op": "+", "id": "a", "row": [1, 10]},
            {"table": "l", "op": "+", "id": "b", "row": [1, 10]},
        ]})
        self.assertEqual(result_bag(ex), Counter({(1, 10, None, None): 2}))
        # 匹配数 0 -> 1：撤回两份旧补 NULL 行 + 两份匹配行
        r = ex.apply_batch({"changes": [
            {"table": "r", "op": "+", "id": "x", "row": [1, 90]}]})
        self.assertEqual(published_counter(r["changes"]),
                         Counter({(1, 10, None, None): -2,
                                  (1, 10, 1, 90): 2}))

    def test_one_to_zero_repads_and_one_to_one_stays(self):
        ex = IncrementalExecutor(self.plan, self.tables)
        ex.apply_batch({"changes": [
            {"table": "l", "op": "+", "id": "a", "row": [1, 10]},
            {"table": "r", "op": "+", "id": "x", "row": [1, 90]},
            {"table": "r", "op": "+", "id": "y", "row": [1, 91]},
        ]})
        # 删除其中一条匹配：1 -> 1，补 NULL 不动
        r = ex.apply_batch({"changes": [
            {"table": "r", "op": "-", "id": "x"}]})
        self.assertEqual(published_counter(r["changes"]),
                         Counter({(1, 10, 1, 90): -1}))
        # 删除最后一条匹配：1 -> 0，补回 NULL
        r = ex.apply_batch({"changes": [
            {"table": "r", "op": "-", "id": "y"}]})
        self.assertEqual(published_counter(r["changes"]),
                         Counter({(1, 10, 1, 91): -1,
                                  (1, 10, None, None): 1}))

    def test_delete_left_padded_copy(self):
        ex = IncrementalExecutor(self.plan, self.tables)
        for i in range(100):
            ex.apply_batch({"changes": [
                {"table": "l", "op": "+", "id": f"a{i}", "row": [1, 10]}]})
        r = ex.apply_batch({"changes": [
            {"table": "l", "op": "-", "id": "a0"}]})
        self.assertEqual(published_counter(r["changes"]),
                         Counter({(1, 10, None, None): -1}))

    def test_unrelated_right_insert_does_not_touch_other_key(self):
        # 右表插入 k=2 不应触碰 k=1 的左行状态键
        ex = IncrementalExecutor(self.plan, self.tables)
        ex.apply_batch({"changes": [
            {"table": "l", "op": "+", "id": "a", "row": [1, 10]},
            {"table": "r", "op": "+", "id": "x", "row": [2, 90]}]})
        r = ex.apply_batch({"changes": [
            {"table": "r", "op": "+", "id": "y", "row": [3, 91]}]})
        # join 节点触碰的右侧键只有 [3]，不包含 [1]/[2]
        join_num = ex.root.num
        touched = r["touched"][join_num]
        self.assertTrue(any(t.startswith("R:[3]") for t in touched))
        self.assertFalse(any("[1]" in t or "[2]" in t for t in touched))


class TestGroupByIncremental(unittest.TestCase):
    PLAN = group_by(
        scan("o", "o"),
        [gkey("c", col("o", "c"))],
        [agg("total", "sum", col("o", "amt")),
         agg("n", "count_star"),
         agg("navg", "avg", col("o", "amt"))],
    )

    def ex(self):
        return IncrementalExecutor(
            self.PLAN, {"o": {"columns": ["c", "amt"], "rows": []}})

    def test_aggregate_change_emits_retract_and_insert(self):
        ex = self.ex()
        ex.apply_batch({"changes": [
            {"table": "o", "op": "+", "id": "a", "row": ["A", 10]}]})
        r = ex.apply_batch({"changes": [
            {"table": "o", "op": "+", "id": "b", "row": ["A", 5]}]})
        # 必须撤回旧聚合行并插入新聚合行，不能只追加
        self.assertEqual(published_counter(r["changes"]),
                         Counter({("A", 10, 1, 10.0): -1,
                                  ("A", 15, 2, 7.5): 1}))

    def test_empty_group_disappears_then_reappears(self):
        ex = self.ex()
        ex.apply_batch({"changes": [
            {"table": "o", "op": "+", "id": "a", "row": ["A", 10]}]})
        r = ex.apply_batch({"changes": [
            {"table": "o", "op": "-", "id": "a"}]})
        self.assertEqual(published_counter(r["changes"]),
                         Counter({("A", 10, 1, 10.0): -1}))
        self.assertEqual(result_bag(ex), Counter())  # 空组不输出
        r = ex.apply_batch({"changes": [
            {"table": "o", "op": "+", "id": "b", "row": ["A", 7]}]})
        self.assertEqual(published_counter(r["changes"]),
                         Counter({("A", 7, 1, 7.0): 1}))

    def test_null_measure_ignored_by_sum_count(self):
        plan = group_by(
            scan("o", "o"), [],
            [agg("s", "sum", col("o", "amt")),
             agg("c", "count", col("o", "amt")),
             agg("n", "count_star")],
        )
        ex = IncrementalExecutor(
            plan, {"o": {"columns": ["c", "amt"], "rows": []}})
        ex.apply_batch({"changes": [
            {"table": "o", "op": "+", "id": "a", "row": ["A", None]},
            {"table": "o", "op": "+", "id": "b", "row": ["A", 3]}]})
        self.assertEqual(result_bag(ex), Counter({(3, 1, 2): 1}))

    def test_global_aggregation_empty_input(self):
        ex = self.ex()
        self.assertEqual(result_bag(ex), Counter())


class TestWindowAndWatermark(unittest.TestCase):
    def plan(self):
        return window(
            scan("o", "o"), col("o", "ts"), 5,
            [gkey("c", col("o", "c"))],
            [agg("total", "sum", col("o", "amt")),
             agg("n", "count_star")],
        )

    def ex(self, lateness=2):
        return IncrementalExecutor(
            self.plan(),
            {"o": {"columns": ["c", "amt", "ts"], "rows": []}},
            allowed_lateness=lateness)

    def test_on_time_then_late_within_threshold_retracts(self):
        ex = self.ex()
        ex.apply_batch({"changes": [
            {"table": "o", "op": "+", "id": "a", "row": ["A", 10, 0]}]})
        ex.apply_batch({"changes": [], "watermark": 6})  # 保留下限 4
        # t=4 恰在边界：接受；同窗口聚合改变 -> 撤回 + 新增
        r = ex.apply_batch({"changes": [
            {"table": "o", "op": "+", "id": "late", "row": ["A", 1, 4]}]})
        self.assertEqual(published_counter(r["changes"]),
                         Counter({("A", 0, 5, 10, 1): -1,
                                  ("A", 0, 5, 11, 2): 1}))

    def test_beyond_retention_rejected_without_state_change(self):
        ex = self.ex()
        ex.apply_batch({"changes": [
            {"table": "o", "op": "+", "id": "a", "row": ["A", 10, 0]}]})
        before = result_bag(ex)
        ex.apply_batch({"changes": [], "watermark": 6})
        with self.assertRaises(LateDataError):
            ex.apply_batch({"changes": [
                {"table": "o", "op": "+", "id": "x", "row": ["A", 1, 3]}]})
        self.assertEqual(result_bag(ex), before)  # 状态未改
        # 被拒后系统仍可正常处理合法批次
        r = ex.apply_batch({"changes": [
            {"table": "o", "op": "+", "id": "y", "row": ["A", 2, 4]}]})
        self.assertTrue(r["changes"])

    def test_window_close_boundary(self):
        # s+width <= wm - lateness 关闭：width=5,late=2 -> wm=7 关 [0,5)
        ex = self.ex()
        ex.apply_batch({"changes": [
            {"table": "o", "op": "+", "id": "a", "row": ["A", 10, 0]}]})
        r = ex.apply_batch({"changes": [], "watermark": 6})
        self.assertEqual(r["closed"], {})  # 5 <= 4 不成立
        r = ex.apply_batch({"changes": [], "watermark": 7})
        self.assertEqual(r["closed"], {ex.root.num: [0]})
        # 关闭结果行冻结保留
        self.assertEqual(result_bag(ex), Counter({("A", 0, 5, 10, 1): 1}))
        # 关闭后的任何增删明确拒绝
        with self.assertRaises(LateDataError):
            ex.apply_batch({"changes": [
                {"table": "o", "op": "+", "id": "x", "row": ["A", 1, 1]}]})
        with self.assertRaises(LateDataError):
            ex.apply_batch({"changes": [
                {"table": "o", "op": "-", "id": "a"}]})

    def test_watermark_monotonic(self):
        ex = self.ex()
        ex.apply_batch({"changes": [], "watermark": 5})
        with self.assertRaises(WatermarkError):
            ex.apply_batch({"changes": [], "watermark": 4})
        self.assertEqual(ex.watermark, 5)

    def test_null_event_time_rejected_atomically(self):
        ex = self.ex()
        # 事件时间为 NULL/非整数：该更新不可接受，整批拒绝且状态不变
        with self.assertRaises(LateDataError) as ctx:
            ex.apply_batch({"changes": [
                {"table": "o", "op": "+", "id": "a", "row": ["A", 10, None]}]})
        self.assertEqual(ctx.exception.reason, "missing_timestamp")
        self.assertEqual(ex.seq, 0)
        self.assertEqual(result_bag(ex), Counter())


class TestAtomicBatch(unittest.TestCase):
    def setUp(self):
        self.tables = {
            "l": {"columns": ["k", "v"], "rows": []},
            "r": {"columns": ["k", "w"], "rows": []},
        }
        self.plan = join("left_join", scan("l", "l"), scan("r", "r"),
                         cmp("=", col("l", "k"), col("r", "k")))

    def test_invalid_record_mid_batch_leaves_state_untouched(self):
        ex = IncrementalExecutor(self.plan, self.tables)
        ex.apply_batch({"changes": [
            {"table": "l", "op": "+", "id": "a", "row": [1, 10]}]})
        before = ex.snapshot()
        with self.assertRaises(ChangeError):
            ex.apply_batch({"changes": [
                {"table": "r", "op": "+", "id": "x", "row": [1, 90]},
                {"table": "r", "op": "+", "id": "x", "row": [1, 90]},  # 重复 id
            ]})
        self.assertEqual(ex.seq, 1)
        # 首条记录的效果也必须回滚
        self.assertEqual(json.dumps(ex.snapshot(), sort_keys=True),
                         json.dumps(before, sort_keys=True))

    def test_late_record_after_valid_change_rolls_back_whole_batch(self):
        plan = window(
            scan("o", "o"), col("o", "ts"), 5,
            [gkey("c", col("o", "c"))], [agg("n", "count_star")])
        ex = IncrementalExecutor(
            plan, {"o": {"columns": ["c", "amt", "ts"], "rows": []}},
            allowed_lateness=0)
        ex.apply_batch({"changes": [
            {"table": "o", "op": "+", "id": "a", "row": ["A", 1, 10]}]})
        ex.apply_batch({"changes": [], "watermark": 10})
        before = result_bag(ex)
        with self.assertRaises(LateDataError):
            ex.apply_batch({"changes": [
                {"table": "o", "op": "+", "id": "ok", "row": ["A", 1, 11]},
                {"table": "o", "op": "+", "id": "late", "row": ["A", 1, 0]},
            ]})
        self.assertEqual(result_bag(ex), before)
        self.assertEqual(ex.watermark, 10)

    def test_watermark_regression_with_changes_rejected(self):
        ex = IncrementalExecutor(self.plan, self.tables)
        ex.apply_batch({"changes": [], "watermark": 5})
        with self.assertRaises(WatermarkError):
            ex.apply_batch({"changes": [
                {"table": "l", "op": "+", "id": "a", "row": [1, 1]}],
                "watermark": 4})
        self.assertEqual(result_bag(ex), Counter())


class TestSnapshotReplay(unittest.TestCase):
    def test_restore_produces_identical_change_stream(self):
        tables = {
            "l": {"columns": ["k", "v"], "rows": []},
            "r": {"columns": ["k", "w"], "rows": []},
        }
        plan = join("left_join", scan("l", "l"), scan("r", "r"),
                    cmp("=", col("l", "k"), col("r", "k")))
        batches = [
            {"changes": [{"table": "l", "op": "+", "id": "a", "row": [1, 10]},
                         {"table": "l", "op": "+", "id": "b", "row": [2, 20]}]},
            {"changes": [{"table": "r", "op": "+", "id": "x", "row": [1, 90]}]},
            {"changes": [{"table": "r", "op": "-", "id": "x"},
                         {"table": "r", "op": "+", "id": "y", "row": [2, 80]}]},
        ]
        ex1 = IncrementalExecutor(plan, tables)
        outs1 = [ex1.apply_batch(b) for b in batches[:2]]
        snap = json.loads(json.dumps(ex1.snapshot()))  # 真正序列化/反序列化
        ex2 = ex1.restore(snap)
        tail1 = ex1.apply_batch(batches[2])
        ex3 = IncrementalExecutor(plan, json.loads(json.dumps(tables)))
        ex3_full = [ex3.apply_batch(b) for b in batches]  # 全新重放全程
        # 从快照续跑与原执行器同一批的变更流逐字一致
        ex_restored = ex1.restore(snap)
        tail2 = ex_restored.apply_batch(batches[2])
        self.assertEqual(tail1["changes"], tail2["changes"])
        # 快照续跑终点与全程重放终点袋一致
        self.assertEqual(result_bag(ex_restored), result_bag(ex3))
        self.assertEqual(ex_restored.watermark, ex3.watermark)

    def test_bad_snapshot_rejected(self):
        plan = scan("t")
        ex = IncrementalExecutor(plan, {"t": {"columns": ["x"], "rows": []}})
        with self.assertRaises(Exception):
            ex.restore({"version": 999})


class TestBatchOracleDifferential(unittest.TestCase):
    """每批累计输入交*原批量解释器* execute() 对照（题面指定的对照方式）。"""

    def _cumulative(self, plan, tables, live):
        cum = {t: {"columns": tables[t]["columns"],
                   "rows": [list(r) for r in live[t].values()]}
               for t in tables}
        return execute(plan, cum, max_rows=100_000)

    def test_small_stream_matches_batch_every_batch(self):
        tables = {
            "l": {"columns": ["k", "v"], "rows": []},
            "r": {"columns": ["k", "w"], "rows": []},
        }
        plan = join("left_join", scan("l", "l"), scan("r", "r"),
                    cmp("=", col("l", "k"), col("r", "k")))
        ex = IncrementalExecutor(plan, tables)
        live = {t: {} for t in tables}
        prev_oracle = Counter()  # 上一批累计输入的批量结果袋
        script = [
            ("+", "l", "a", [1, 10]), ("+", "l", "b", [1, 10]),
            ("+", "l", "c", [None, 2]),
            ("+", "r", "x", [1, 90]),
            ("-", "l", "b", None),
            ("+", "r", "y", [1, 90]),
            ("-", "r", "x", None),
            ("+", "l", "d", [2, 20]),
            ("-", "r", "y", None),
            ("-", "l", "a", None),
        ]
        seq = 0
        for op, t, i, row in script:
            rec = {"table": t, "op": op, "id": i}
            if op == "+":
                rec["row"] = row
                live[t][i] = tuple(row)
            else:
                del live[t][i]
            r = ex.apply_batch({"changes": [rec]})
            oracle = Counter(tuple(x) for x in self._cumulative(
                plan, tables, live)["rows"])
            # 1) 增量终袋 == 原批量解释器在累计输入上的结果袋
            self.assertEqual(result_bag(ex), oracle,
                             msg=f"step {seq} {op} {t} {i}")
            # 2) 发布的带符号净增量 == 相邻两次批量物化之差（撤回为负）
            signed = Counter(oracle)
            signed.subtract(prev_oracle)
            self.assertEqual(published_counter(r["changes"]), signed,
                             msg=f"step {seq} 净增量不等于批量差")
            prev_oracle = oracle
            seq += 1


class TestIndependentReferenceSmallDomain(unittest.TestCase):
    """小确定性域：种子随机增删流，每批与完全独立的朴素参考核对净增量。"""

    PLANS = None

    def _plans(self):
        l = scan("l", "l")
        r = scan("r", "r")
        on_eq = cmp("=", col("l", "k"), col("r", "k"))
        on_resid = band(on_eq, cmp(">", col("l", "v"), lit(0)))
        o = scan("o", "o")
        grp = group_by(o, [gkey("c", col("o", "c"))],
                       [agg("s", "sum", col("o", "amt")),
                        agg("n", "count_star"),
                        agg("mn", "min", col("o", "amt"))])
        win = window(o, col("o", "ts"), 4, [gkey("c", col("o", "c"))],
                     [agg("s", "sum", col("o", "amt")), agg("n", "count_star")])
        jg = group_by(
            join("left_join", l, r, on_eq),
            [gkey("lk", col("l", "k"))],
            [agg("n", "count_star"), agg("sw", "sum", col("r", "w"))])
        jgw = window(
            join("left_join", scan("o", "o"), scan("r", "r"),
                 cmp("=", col("o", "c"), col("r", "k"))),
            col("o", "ts"), 4,
            [gkey("c", col("o", "c"))],
            [agg("n", "count_star"), agg("s", "sum", col("o", "amt"))])
        fjg = group_by(
            {"op": "filter",
             "input": join("left_join", scan("o", "o"), scan("r", "r"),
                           cmp("=", col("o", "c"), col("r", "k"))),
             "expr": cmp(">=", col("o", "amt"), lit(0))},
            [gkey("c", col("o", "c"))],
            [agg("n", "count_star"), agg("sw", "sum", col("r", "w"))])
        return [
            ("filter", {"op": "filter", "input": o,
                        "expr": cmp(">=", col("o", "amt"), lit(0))}),
            ("inner", join("inner_join", l, r, on_eq)),
            ("inner_resid", join("inner_join", l, r, on_resid)),
            ("left", join("left_join", l, r, on_eq)),
            ("union", {"op": "union_all", "left": l, "right": scan("l2", "l2")}),
            ("distinct", {"op": "distinct", "input": l}),
            ("group", grp),
            ("window", win),
            ("join_group", jg),
            ("join_group_window", jgw),
            ("filter_join_group", fjg),
        ]

    def SCHEMA(self, name):
        if name == "o":
            return ["c", "amt", "ts"]
        if name == "l2":
            return ["k", "v"]
        if name == "l":
            return ["k", "v"]
        return ["k", "w"]

    def test_random_streams_small_domain(self):
        rng = random.Random(20260924)
        for name, plan in self._plans():
            tables_used = sorted({n["table"] for n in self._all_scans(plan)})
            for trial in range(6):
                self._run_one(name, plan, tables_used, rng)

    def _all_scans(self, node):
        out = []
        if node["op"] == "scan":
            out.append(node)
        for key in ("input", "left", "right"):
            if key in node and isinstance(node[key], dict):
                out.extend(self._all_scans(node[key]))
        return out

    def _run_one(self, name, plan, tables_used, rng):
        schema = {t: self.SCHEMA(t) for t in tables_used}
        init = {t: {"columns": schema[t], "rows": []} for t in tables_used}
        ex = IncrementalExecutor(plan, init, allowed_lateness=1)
        live = {t: {} for t in tables_used}  # id -> tuple
        id_counter = 0

        def gen_row(t):
            if t == "o":
                c = rng.choice([None, 0, 1, 2])
                amt = rng.choice([None, -1, 0, 1, 2])
                ts = rng.randrange(0, 12)
                return [c, amt, ts]
            if t in ("l",):
                return [rng.choice([None, 0, 1, 2]), rng.choice([-1, 0, 1])]
            if t == "l2":
                return [rng.choice([None, 0, 1, 2]), rng.choice([-1, 0, 1])]
            return [rng.choice([None, 0, 1, 2]), rng.choice([None, 1, 9])]

        id_tables = {t: (schema[t], live[t]) for t in tables_used}
        for step in range(40):
            changes = []
            touched_ids = set()
            for _ in range(rng.randrange(1, 4)):
                t = rng.choice(tables_used)
                avail = [i for i in live[t] if i not in touched_ids]
                if avail and rng.random() < 0.4:
                    rid = rng.choice(avail)
                    touched_ids.add(rid)
                    changes.append({"table": t, "op": "-", "id": rid})
                else:
                    rid = f"{t}-{id_counter}"
                    id_counter += 1
                    touched_ids.add(rid)
                    row = gen_row(t)
                    changes.append({"table": t, "op": "+", "id": rid, "row": row})
            batch = {"changes": changes}
            before = naive_bag(plan, id_tables)
            out = ex.apply_batch(batch)
            for ch in changes:  # 同步朴素参考的活动行集合
                if ch["op"] == "+":
                    live[ch["table"]][ch["id"]] = tuple(ch["row"])
                else:
                    del live[ch["table"]][ch["id"]]
            after = naive_bag(plan, id_tables)
            # 1) 发布的净增量必须恰好等于两次物化的带符号差（撤回为负；
            #    Counter 的 a-b 会丢负项，故必须用 subtract 手工构造）。
            signed = Counter()
            signed.update(after)
            signed.subtract(before)
            self.assertEqual(published_counter(out["changes"]), signed,
                             msg=f"{name}: 净增量带符号不符")
            # 2) 终袋与独立参考一致
            self.assertEqual(result_bag(ex), after, msg=f"{name}: 终袋不符")
            # 3) 同输入确定性：净增量按确定顺序发布（None/int/str 跨类型键）
            from rel3vl.engine import _sort_tag
            self.assertEqual(out["changes"],
                             sorted(out["changes"],
                                    key=lambda c: tuple(_sort_tag(v)
                                                        for v in c["row"])))


class TestNestedPlanLocalTouchAndSinglePublish(unittest.TestCase):
    """外连接 -> 窗口聚合：同批删除最后匹配 + 补入迟到行 + 水位关闭。"""

    def plan(self):
        return window(
            join("left_join", scan("o", "o"), scan("r", "r"),
                 cmp("=", col("o", "c"), col("r", "k"))),
            col("o", "ts"), 5,
            [gkey("c", col("o", "c"))],
            [agg("n", "count_star"), agg("sw", "sum", col("r", "w"))])

    def test_combined_batch_publishes_once_and_touches_only_affected(self):
        p = self.plan()
        tables = {"o": {"columns": ["c", "amt", "ts"], "rows": []},
                  "r": {"columns": ["k", "w"], "rows": []}}
        ex = IncrementalExecutor(p, tables, allowed_lateness=1)
        # o0@t=0 落入将关闭的 [0,5)；o1@t=6 与新行落入仍开放的 [5,10)。
        ex.apply_batch({"changes": [
            {"table": "o", "op": "+", "id": "o0", "row": ["A", 1, 0]},
            {"table": "o", "op": "+", "id": "o1", "row": ["B", 1, 6]},
            {"table": "r", "op": "+", "id": "rA", "row": ["A", 5]},
            {"table": "r", "op": "+", "id": "rB", "row": ["B", 6]},
        ]})
        # 同批三件事：
        #  1) 删除 B 的最后匹配 rB -> 开放窗口 [5,10) 的 B 组补 NULL；
        #  2) 补入迟到行 o3：t=6 恰为保留下界（新水位 7 - 迟到 1），
        #     c=C 在维表无匹配 -> 左外补 NULL，落入仍开放的 [5,10)；
        #  3) 水位推进到 7：[0,5) 满足 0+5 <= 6 而关闭并冻结；[5,10) 开放。
        batch = {"changes": [
            {"table": "r", "op": "-", "id": "rB"},
            {"table": "o", "op": "+", "id": "o3", "row": ["C", 1, 6]},
        ], "watermark": 7}
        r = ex.apply_batch(batch)
        self.assertEqual(r["seq"], 2)             # 整批只发布一次
        self.assertEqual(r["closed"], {ex.root.num: [0]})
        pc = published_counter(r["changes"])
        # 开放窗口 B 组：撤回匹配行、补入 NULL 行
        self.assertEqual(pc[("B", 5, 10, 1, 6)], -1)
        self.assertEqual(pc[("B", 5, 10, 1, None)], 1)
        # 迟到行进入仍开放窗口的新组 C（左外补 NULL）
        self.assertEqual(pc[("C", 5, 10, 1, None)], 1)
        # 已关闭窗口的冻结行不出现在本批变更中
        self.assertNotIn(("A", 0, 5, 1, 5), pc)
        # 窗口数据键只触碰开放窗口 [5,10) 的 B/C；关闭枚举仅到期起点 [0]
        win_touched = r["touched"][ex.root.num]
        data_touch = [t for t in win_touched if t.startswith("w:")]
        self.assertTrue(all(t.startswith('w:[5,') for t in data_touch))
        self.assertTrue(any('"B"' in t for t in data_touch))
        self.assertTrue(any('"C"' in t for t in data_touch))
        closes = [t for t in win_touched if t.startswith("close:")]
        self.assertTrue(closes and all(t.startswith('close:[0,') for t in closes))
        # 冻结行与新结果都保留在当前袋中
        bag = result_bag(ex)
        self.assertEqual(bag[("A", 0, 5, 1, 5)], 1)
        self.assertEqual(bag[("B", 5, 10, 1, None)], 1)
        self.assertEqual(bag[("C", 5, 10, 1, None)], 1)
        # 关闭窗口的任何增删明确拒绝且不动状态
        with self.assertRaises(LateDataError):
            ex.apply_batch({"changes": [
                {"table": "o", "op": "-", "id": "o0"}]})
        with self.assertRaises(LateDataError):
            ex.apply_batch({"changes": [
                {"table": "r", "op": "-", "id": "rA"}]})
        self.assertEqual(result_bag(ex), bag)
        # 仍开放窗口的后续更新照常撤回/新增
        r = ex.apply_batch({"changes": [
            {"table": "o", "op": "-", "id": "o3"}]})
        self.assertEqual(published_counter(r["changes"]),
                         Counter({("C", 5, 10, 1, None): -1}))


class TestMiscIncremental(unittest.TestCase):
    def test_project_over_join_with_duplicate_collapse(self):
        plan = {
            "op": "project",
            "input": join("inner_join", scan("l", "l"), scan("r", "r"),
                          cmp("=", col("l", "k"), col("r", "k"))),
            "columns": [{"name": "v", "expr": col("l", "v")},
                        {"name": "w", "expr": col("r", "w")}],
        }
        tables = {"l": {"columns": ["k", "v"], "rows": []},
                  "r": {"columns": ["k", "w"], "rows": []}}
        ex = IncrementalExecutor(plan, tables)
        ex.apply_batch({"changes": [
            {"table": "l", "op": "+", "id": "a", "row": [1, 7]},
            {"table": "l", "op": "+", "id": "b", "row": [1, 7]},
            {"table": "r", "op": "+", "id": "x", "row": [1, 9]}]})
        self.assertEqual(result_bag(ex), Counter({(7, 9): 2}))
        r = ex.apply_batch({"changes": [
            {"table": "l", "op": "-", "id": "a"}]})
        self.assertEqual(published_counter(r["changes"]), Counter({(7, 9): -1}))

    def test_initial_rows_loaded_silently_and_match_batch(self):
        tables = {
            "l": {"columns": ["k", "v"], "rows": [[1, 10], [1, 10], [2, 20]]},
            "r": {"columns": ["k", "w"], "rows": [[1, 90]]},
        }
        plan = join("left_join", scan("l", "l"), scan("r", "r"),
                    cmp("=", col("l", "k"), col("r", "k")))
        ex = IncrementalExecutor(plan, tables)
        # 初始行静默装载：尚无批次发布
        self.assertEqual(ex.seq, 0)
        oracle = Counter(tuple(x) for x in execute(plan, tables)["rows"])
        self.assertEqual(result_bag(ex), oracle)
        self.assertEqual(result_bag(ex),
                         Counter({(1, 10, 1, 90): 2, (2, 20, None, None): 1}))

    def test_deterministic_replay_same_change_stream(self):
        tables = {"o": {"columns": ["c", "amt", "ts"], "rows": []}}
        plan = window(scan("o", "o"), col("o", "ts"), 3,
                      [gkey("c", col("o", "c"))],
                      [agg("s", "sum", col("o", "amt"))])
        batches = [
            {"changes": [{"table": "o", "op": "+", "id": "a", "row": [1, 5, 2]},
                         {"table": "o", "op": "+", "id": "b", "row": [None, 2, 1]}],
             "watermark": 1},
            {"changes": [{"table": "o", "op": "-", "id": "a"},
                         {"table": "o", "op": "+", "id": "c", "row": [1, 7, 2]}],
             "watermark": 3},
        ]
        def run():
            ex = IncrementalExecutor(plan, tables, allowed_lateness=2)
            return [ex.apply_batch(b)["changes"] for b in batches]
        self.assertEqual(run(), run())

    def test_inner_join_window_delete_last_match(self):
        # 内连接（非外）嵌窗口：删除唯一匹配 -> 窗口组撤回（空组消失）
        plan = window(
            join("inner_join", scan("o", "o"), scan("r", "r"),
                 cmp("=", col("o", "c"), col("r", "k"))),
            col("o", "ts"), 5,
            [gkey("c", col("o", "c"))],
            [agg("n", "count_star"), agg("sw", "sum", col("r", "w"))])
        tables = {"o": {"columns": ["c", "amt", "ts"], "rows": []},
                  "r": {"columns": ["k", "w"], "rows": []}}
        ex = IncrementalExecutor(plan, tables)
        ex.apply_batch({"changes": [
            {"table": "o", "op": "+", "id": "o1", "row": ["A", 1, 0]},
            {"table": "r", "op": "+", "id": "rA", "row": ["A", 5]}]})
        r = ex.apply_batch({"changes": [
            {"table": "r", "op": "-", "id": "rA"}]})
        # 内连接下匹配行消失，窗口 A 组变空 -> 撤回聚合行，结果袋为空
        self.assertEqual(published_counter(r["changes"]),
                         Counter({("A", 0, 5, 1, 5): -1}))
        self.assertEqual(result_bag(ex), Counter())


class TestStreamValidation(unittest.TestCase):
    def setUp(self):
        self.tables = {"t": {"columns": ["x"], "rows": []}}

    def ex(self):
        return IncrementalExecutor(scan("t"), self.tables)

    def test_bad_change_records(self):
        from rel3vl import QueryError
        cases = [
            ({"changes": [{"table": "nope", "op": "+", "id": "a", "row": [1]}]},
             ChangeError),
            ({"changes": [{"table": "t", "op": "X", "id": "a", "row": [1]}]},
             ChangeError),
            ({"changes": [{"table": "t", "op": "+", "id": "", "row": [1]}]},
             ChangeError),
            ({"changes": [{"table": "t", "op": "+", "id": "a"}]}, ChangeError),
            ({"changes": [{"table": "t", "op": "+", "id": "a", "row": [1, 2]}]},
             ChangeError),
            ({"changes": [{"table": "t", "op": "+", "id": "a", "row": [[1]]}]},
             QueryError),  # 非标量 -> TableError（同为 QueryError）
        ]
        for batch, exc in cases:
            with self.subTest(batch=batch):
                with self.assertRaises(exc):
                    self.ex().apply_batch(batch)

    def test_duplicate_id_in_stream_rejected(self):
        ex = self.ex()
        ex.apply_batch({"changes": [
            {"table": "t", "op": "+", "id": "a", "row": [1]}]})
        with self.assertRaises(ChangeError):
            ex.apply_batch({"changes": [
                {"table": "t", "op": "+", "id": "a", "row": [2]}]})

    def test_delete_with_wrong_content_rejected(self):
        ex = self.ex()
        ex.apply_batch({"changes": [
            {"table": "t", "op": "+", "id": "a", "row": [1]}]})
        with self.assertRaises(ChangeError):
            ex.apply_batch({"changes": [
                {"table": "t", "op": "-", "id": "a", "row": [2]}]})

    def test_bad_plan_options(self):
        with self.assertRaises(Exception):
            IncrementalExecutor(scan("t"), self.tables, allowed_lateness=-1)
        win = window(scan("t"), col("t", "x"), 0, [], [agg("n", "count_star")])
        with self.assertRaises(Exception):  # width 必须正整数
            IncrementalExecutor(win, self.tables)

    def test_watermark_bool_rejected(self):
        ex = self.ex()
        with self.assertRaises(WatermarkError):
            ex.apply_batch({"changes": [], "watermark": True})


    def test_outer_join_then_group_then_window(self):
        # 外连接 -> 分组聚合 -> 再接窗口：ts 作为分组键之一上推给窗口。
        plan = window(
            group_by(
                join("left_join", scan("o", "o"), scan("r", "r"),
                     cmp("=", col("o", "c"), col("r", "k"))),
                [gkey("c", col("o", "c")), gkey("ts", col("o", "ts"))],
                [agg("n", "count_star"), agg("sw", "sum", col("r", "w"))]),
            col("", "ts"), 5,
            [gkey("c", col("", "c"))],
            [agg("rows", "sum", col("", "n")),
             agg("w", "sum", col("", "sw"))])
        tables = {"o": {"columns": ["c", "amt", "ts"], "rows": []},
                  "r": {"columns": ["k", "w"], "rows": []}}
        ex = IncrementalExecutor(plan, tables, allowed_lateness=0)
        ex.apply_batch({"changes": [
            {"table": "o", "op": "+", "id": "o1", "row": ["A", 1, 0]},
            {"table": "o", "op": "+", "id": "o2", "row": ["X", 1, 1]},  # 无匹配 -> 补 NULL
            {"table": "r", "op": "+", "id": "rA", "row": ["A", 5]}]})
        bag = result_bag(ex)
        # A: (c=A) 窗口内行数 1、w 之和 5；X: 补 NULL 行 w 贡献 NULL -> w=NULL
        self.assertEqual(bag[("A", 0, 5, 1, 5)], 1)
        self.assertEqual(bag[("X", 0, 5, 1, None)], 1)
        # 删除最后匹配：A 的聚合撤回（n 1->0 组消失走下层），最终窗口行撤回
        r = ex.apply_batch({"changes": [
            {"table": "r", "op": "-", "id": "rA"}]})
        pc = published_counter(r["changes"])
        # A 组 join 行补 NULL -> group 行 sw 5->NULL -> 窗口 w 5->NULL：撤回+新增
        self.assertEqual(pc[("A", 0, 5, 1, 5)], -1)
        self.assertEqual(pc[("A", 0, 5, 1, None)], 1)
        # 与批量解释器对照
        cum = {"o": {"columns": ["c", "amt", "ts"],
                     "rows": [["A", 1, 0], ["X", 1, 1]]},
               "r": {"columns": ["k", "w"], "rows": []}}
        self.assertEqual(result_bag(ex),
                         Counter(tuple(x) for x in
                                 execute(plan, cum, max_rows=1000)["rows"]))


if __name__ == "__main__":
    unittest.main()
