"""grouping_sets 增量测试：两级汇总手算 + 朴素参考交叉校验。"""

from __future__ import annotations

import unittest
from collections import Counter

from rel3vl import ExpressionError, PlanError, QueryError, SchemaError, execute

try:  # 兼容 `discover -s tests` 与包方式运行
    from ._fixtures import col, lit, naive_expected, scan
except ImportError:  # pragma: no cover
    from _fixtures import col, lit, naive_expected, scan


def sales_table(extra=None, empty=False):
    # (region, city, amount)：含同值 NULL、同组重复行、NULL/数值混合
    rows = [] if empty else [
        ["E", "BJ", 10],
        ["E", "BJ", 10],   # 与首行完全重复：袋语义下参与两次
        ["E", "TJ", None], # amount NULL：不参与 sum/count(expr)
        ["W", "CD", 7],
        [None, None, 3],   # region/city 均为真正 NULL
        [None, None, 5],   # 同值 NULL：两行应并入同一组
    ]
    if extra:
        rows.extend(extra)
    return {"t": {"columns": ["region", "city", "amount"], "rows": rows}}


def grouping_plan(sets, group_by=None, aggs=None, input_node=None):
    if group_by is None:
        group_by = [{"table": "s", "column": "region"}, {"table": "s", "column": "city"}]
    if aggs is None:
        aggs = [
            {"name": "n", "func": "count"},
            {"name": "amount_n", "func": "count", "expr": col("s", "amount")},
            {"name": "total", "func": "sum", "expr": col("s", "amount")},
        ]
    return {
        "op": "grouping_sets",
        "input": input_node if input_node is not None else scan("t", "s"),
        "group_by": group_by,
        "sets": sets,
        "aggregates": aggs,
    }


class GroupingSetsTests(unittest.TestCase):
    def test_two_level_hand_computed(self):
        # 城市级 + 区域级 + 总计，手工逐组计数。
        plan = grouping_plan([["region", "city"], ["region"], []])
        res = execute(plan, sales_table())
        self.assertEqual(
            [c["name"] for c in res["columns"]],
            ["region", "city", "grouping", "n", "amount_n", "total"],
        )
        rows = [tuple(r) for r in res["rows"]]
        expected = [
            # 城市级（位图 11）：组按首次出现顺序
            ("E", "BJ", "11", 2, 2, 20),
            ("E", "TJ", "11", 1, 0, None),
            ("W", "CD", "11", 1, 1, 7),
            (None, None, "11", 2, 2, 8),  # 同值 NULL 同组；NULL 是真实值
            # 区域级（位图 10）：city 被汇总为空列（NULL 但 bitmap=0）
            ("E", None, "10", 3, 2, 20),
            ("W", None, "10", 1, 1, 7),
            (None, None, "10", 2, 2, 8),
            # 总计（位图 00）
            (None, None, "00", 6, 5, 35),
        ]
        self.assertEqual(rows, expected)

    def test_naive_cross_check(self):
        # 逐个分组集分别执行的朴素实现（tests/_fixtures.py）交叉校验。
        plan = grouping_plan([["region", "city"], ["region"], ["city"], []])
        res = execute(plan, sales_table())
        got = Counter(tuple(r) for r in res["rows"])
        schema, exp_bag = naive_expected(plan, sales_table())
        self.assertEqual(got, exp_bag)
        self.assertEqual([(c["source"], c["name"]) for c in res["columns"]], schema)

    def test_duplicate_sets_kept(self):
        plan = grouping_plan([["region"], ["region"]])  # 同一分组集出现两次
        res = execute(plan, sales_table())
        rows = [tuple(r) for r in res["rows"]]
        once = [
            ("E", None, "10", 3, 2, 20),
            ("W", None, "10", 1, 1, 7),
            (None, None, "10", 2, 2, 8),
        ]
        self.assertEqual(rows, once + once)  # 不去重，两份结果按序拼接

    def test_empty_input_empty_set_has_row(self):
        plan = grouping_plan([[]])
        res = execute(plan, sales_table(empty=True))
        self.assertEqual(res["rows"], [[None, None, "00", 0, 0, None]])

    def test_empty_input_nonempty_set_zero_rows(self):
        plan = grouping_plan([["region"], ["region", "city"]])
        res = execute(plan, sales_table(empty=True))
        self.assertEqual(res["rows"], [])

    def test_empty_input_mixed_sets(self):
        plan = grouping_plan([["region"], []])
        res = execute(plan, sales_table(empty=True))
        self.assertEqual(res["rows"], [[None, None, "00", 0, 0, None]])

    def test_grouping_bitmap_distinguishes_real_null(self):
        # 真实 NULL 组（11 行）与汇总空列（10/00）值都是 NULL，位图不同。
        plan = grouping_plan([["region", "city"], ["region"], []])
        res = execute(plan, sales_table())
        by_bm = {}
        for r in res["rows"]:
            by_bm.setdefault(r[2], []).append(tuple(r))
        self.assertIn((None, None, "11", 2, 2, 8), by_bm["11"])
        self.assertIn((None, None, "00", 6, 5, 35), by_bm["00"])

    def test_bag_duplicates_counted(self):
        # 额外复制两行：BJ 组从 2 行变 4 行，总计 8 行。
        plan = grouping_plan([["region", "city"], []])
        tables = sales_table(extra=[["E", "BJ", 10], ["E", "BJ", 10]])
        res = execute(plan, tables)
        rows = [tuple(r) for r in res["rows"]]
        self.assertIn(("E", "BJ", "11", 4, 4, 40), rows)
        self.assertIn((None, None, "00", 8, 7, 55), rows)

    def test_after_filter(self):
        # 节点可放在过滤之后；先过滤再汇总。
        child = {
            "op": "filter",
            "input": scan("t", "s"),
            "expr": {"kind": "cmp", "op": ">=", "left": col("s", "amount"), "right": lit(7)},
        }
        plan = grouping_plan([["region"], []], input_node=child)
        res = execute(plan, sales_table())
        rows = [tuple(r) for r in res["rows"]]
        # amount>=7: 仅 BJ10,BJ10,CD7 保留；3、5 与 TJ NULL 行被丢弃
        self.assertEqual(rows, [
            ("E", None, "10", 2, 2, 20),
            ("W", None, "10", 1, 1, 7),
            (None, None, "00", 3, 3, 27),
        ])

    def test_sum_all_null_is_null(self):
        plan = grouping_plan([["city"]])
        tables = sales_table(empty=True)
        tables["t"]["rows"] = [["E", "X", None], ["E", "X", None]]
        res = execute(plan, tables)
        self.assertEqual([tuple(r) for r in res["rows"]], [(None, "X", "01", 2, 0, None)])

    def test_errors(self):
        base_tables = sales_table()
        with self.assertRaises(SchemaError):  # group_by 引用不存在的列
            execute(grouping_plan([[]], group_by=[{"table": "s", "column": "nope"}]), base_tables)
        with self.assertRaises(PlanError):  # sets 引用不在 group_by 的列名
            execute(grouping_plan([["nope"]]), base_tables)
        with self.assertRaises(PlanError):  # sets 引用不在 group_by 的列
            execute(grouping_plan([["amount"]]), base_tables)
        with self.assertRaises(PlanError):  # sets 为空
            execute(grouping_plan([]), base_tables)
        with self.assertRaises(PlanError):  # sum 缺 expr
            execute(grouping_plan([[]], aggs=[{"name": "x", "func": "sum"}]), base_tables)
        with self.assertRaises(PlanError):  # 未知聚合函数
            execute(grouping_plan([[]], aggs=[{"name": "x", "func": "avg"}]), base_tables)
        with self.assertRaises(QueryError):  # 聚合输出列名重复
            execute(grouping_plan([[]], aggs=[
                {"name": "x", "func": "count"},
                {"name": "x", "func": "count"},
            ]), base_tables)
        with self.assertRaises(ExpressionError):  # sum 字符串列
            bad = grouping_plan([[]], aggs=[{"name": "x", "func": "sum", "expr": col("s", "city")}])
            execute(bad, base_tables)

    def test_count_star_includes_all_null_rows(self):
        aggs = [{"name": "n", "func": "count"},
                {"name": "a", "func": "sum", "expr": col("s", "amount")}]
        plan = grouping_plan([[]], aggs=aggs)
        res = execute(plan, sales_table())
        self.assertEqual(res["rows"][0][2:], ["00", 6, 35])


if __name__ == "__main__":
    unittest.main()
