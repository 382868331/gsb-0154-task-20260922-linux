"""批量解释器新算子测试：aggregate / window，以及等值哈希连接与嵌套循环
在结果（袋与顺序）与异常上的等价性。期望值为手工计数/独立朴素参考。"""

from __future__ import annotations

import unittest
from collections import Counter

from rel3vl import (
    ExpressionError,
    MAX_STREAM_NODES,
    MAX_STREAM_ROWS,
    execute,
)

try:
    from ._fixtures import naive_expected
except ImportError:  # pragma: no cover
    from _fixtures import naive_expected  # type: ignore


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


KW = {"max_rows": MAX_STREAM_ROWS, "max_nodes": MAX_STREAM_NODES}


class TestAggregateBatch(unittest.TestCase):
    def agg_plan(self, groups, aggs, table="t"):
        node = {"op": "aggregate", "input": scan(table), "aggs": aggs}
        if groups is not None:
            node["groups"] = groups
        return node

    def test_count_sum_min_max_avg_manual(self):
        plan = self.agg_plan(
            [{"name": "g", "expr": col("t", "g")}],
            [{"name": "c", "func": "count"},
             {"name": "cv", "func": "count", "arg": col("t", "v")},
             {"name": "s", "func": "sum", "arg": col("t", "v")},
             {"name": "a", "func": "avg", "arg": col("t", "v")},
             {"name": "lo", "func": "min", "arg": col("t", "v")},
             {"name": "hi", "func": "max", "arg": col("t", "v")}])
        tables = {"t": {"columns": ["g", "v"], "rows": [
            ["x", 2], ["x", 4], ["x", None], ["y", None]]}}
        res = execute(plan, tables, **KW)
        self.assertEqual(Counter(tuple(r) for r in res["rows"]),
                         Counter({("x", 3, 2, 6, 3.0, 2, 4): 1,
                                  ("y", 1, 0, None, None, None, None): 1}))

    def test_group_order_is_first_appearance(self):
        plan = self.agg_plan(
            [{"name": "g", "expr": col("t", "g")}],
            [{"name": "c", "func": "count"}])
        tables = {"t": {"columns": ["g"], "rows": [["b"], ["a"], ["b"], ["a"]]}}
        self.assertEqual([r[0] for r in execute(plan, tables, **KW)["rows"]],
                         ["b", "a"])

    def test_global_agg_empty_input_is_zero_row(self):
        plan = self.agg_plan(None, [
            {"name": "c", "func": "count"},
            {"name": "s", "func": "sum", "arg": col("t", "v")}])
        self.assertEqual(execute(plan, {"t": {"columns": ["v"], "rows": []}},
                                 **KW)["rows"], [[0, None]])

    def test_sum_rejects_bool_and_str(self):
        plan = self.agg_plan(None, [{"name": "s", "func": "sum",
                                    "arg": col("t", "v")}])
        with self.assertRaises(ExpressionError):
            execute(plan, {"t": {"columns": ["v"], "rows": [[True]]}}, **KW)
        with self.assertRaises(ExpressionError):
            execute(plan, {"t": {"columns": ["v"], "rows": [["x"]]}}, **KW)

    def test_int_sum_stays_int_mixed_float_promotes(self):
        plan = self.agg_plan(None, [{"name": "s", "func": "sum",
                                    "arg": col("t", "v")}])
        self.assertEqual(execute(plan, {"t": {"columns": ["v"], "rows": [[2], [3]]}},
                                 **KW)["rows"], [[5]])
        self.assertEqual(execute(plan, {"t": {"columns": ["v"], "rows": [[2], [0.5]]}},
                                 **KW)["rows"], [[2.5]])

    def test_naive_reference_cross_check(self):
        plan = self.agg_plan(
            [{"name": "g", "expr": col("t", "g")}],
            [{"name": "c", "func": "count"},
             {"name": "s", "func": "sum", "arg": col("t", "v")},
             {"name": "a", "func": "avg", "arg": col("t", "v")}])
        tables = {"t": {"columns": ["g", "v"], "rows": [
            ["a", 1], ["a", None], ["b", 10], ["a", 2.0], ["b", None]]}}
        _, exp = naive_expected(plan, tables)
        self.assertEqual(Counter(tuple(r) for r in execute(plan, tables, **KW)["rows"]),
                         exp)


class TestWindowBatch(unittest.TestCase):
    def plan(self, **kw):
        w = {"op": "window", "input": scan("e"), "time_col": col("e", "ts"),
             "size": kw.pop("size")}
        w.update(kw)
        return w

    def test_half_open_tumbling_boundary(self):
        plan = self.plan(size=5)
        tables = {"e": {"columns": ["ts", "v"],
                        "rows": [[4, 1], [5, 2], [9, 3]]}}
        rows = execute(plan, tables, **KW)["rows"]
        self.assertEqual(Counter(tuple(r) for r in rows),
                         Counter({(4, 1, 0, 5): 1, (5, 2, 5, 10): 1,
                                  (9, 3, 5, 10): 1}))

    def test_sliding_multiplicity(self):
        plan = self.plan(size=4, slide=2)
        tables = {"e": {"columns": ["ts", "v"], "rows": [[3, 1]]}}
        rows = execute(plan, tables, **KW)["rows"]
        # t=3 属 [0,4) 与 [2,6)；非负轴上无 [-2,2)
        self.assertEqual(sorted(r[-2:] for r in rows), [[0, 4], [2, 6]])

    def test_bad_window_specs_rejected(self):
        tables = {"e": {"columns": ["ts"], "rows": [[1]]}}
        for kw in ({"size": 0}, {"size": 4, "slide": 3},
                   {"size": 4, "allowed_lateness": -1}):
            with self.assertRaises(Exception):
                execute(self.plan(**kw), tables, **KW)

    def test_non_integer_event_time_rejected(self):
        plan = self.plan(size=5)
        with self.assertRaises(ExpressionError):
            execute(plan, {"e": {"columns": ["ts"], "rows": [[1.5]]}}, **KW)

    def test_naive_reference_window(self):
        plan = self.plan(size=6, slide=2)
        tables = {"e": {"columns": ["ts", "v"],
                        "rows": [[t, t % 3] for t in range(15)]}}
        _, exp = naive_expected(plan, tables)
        self.assertEqual(Counter(tuple(r) for r in execute(plan, tables, **KW)["rows"]),
                         exp)


class TestEquiJoinEquivalence(unittest.TestCase):
    """等值 ON 走哈希加速：与嵌套循环逐行（含顺序）一致；类型异常一致。"""

    def _plans(self, outer):
        on = cmp("=", col("l", "k"), col("r", "k"))
        return {"op": "left_join" if outer else "inner_join",
                "left": scan("l", "l"), "right": scan("r", "r"), "on": on}

    def test_bag_and_order_match_nested_via_naive(self):
        # ON 恒真无法被等值分析，naive 参考天然是嵌套循环语义。
        tables = {
            "l": {"columns": ["k", "v"],
                  "rows": [[1, "a"], [1, "a"], [2, "b"], [None, "z"]]},
            "r": {"columns": ["k", "w"],
                  "rows": [[1, "x"], [1, "y"], [2, "q"], [None, "n"]]}}
        for outer in (False, True):
            plan = self._plans(outer)
            res = execute(plan, tables, **KW)
            _, exp = naive_expected(plan, tables)
            # 顺序（左×右嵌套序，PAD 在左行末尾）与袋都必须一致
            self.assertEqual([tuple(r) for r in res["rows"]],
                             self._nested_order(plan, tables))
            self.assertEqual(Counter(tuple(r) for r in res["rows"]), exp)

    @staticmethod
    def _nested_order(plan, tables):
        # 独立嵌套循环重放（不 import 核心）：左行序×右行序 + 末尾补 NULL
        lr = [tuple(r) for r in tables["l"]["rows"]]
        rr = [tuple(r) for r in tables["r"]["rows"]]
        outer = plan["op"] == "left_join"
        out = []
        for lrow in lr:
            matched = False
            for rrow in rr:
                if lrow[0] is not None and rrow[0] is not None and lrow[0] == rrow[0]:
                    out.append(lrow + rrow)
                    matched = True
            if outer and not matched:
                out.append(lrow + (None, None))
        return out

    def test_cross_type_equi_raises_like_nested(self):
        # bool 与 int 即使数值相等也报 ExpressionError（与嵌套循环一致）
        tables = {"l": {"columns": ["k"], "rows": [[True]]},
                  "r": {"columns": ["k"], "rows": [[1]]}}
        with self.assertRaises(ExpressionError):
            execute(self._plans(False), tables, **KW)
        tables = {"l": {"columns": ["k"], "rows": [["1"]]},
                  "r": {"columns": ["k"], "rows": [[1]]}}
        with self.assertRaises(ExpressionError):
            execute(self._plans(False), tables, **KW)


if __name__ == "__main__":
    unittest.main()
