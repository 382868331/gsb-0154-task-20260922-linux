"""grouping sets 聚合节点的测试。

覆盖：两级小汇总手算、袋语义重复行、同值 NULL 同组、空表边界、
重复分组集、grouping 位图区分 NULL、count/sum 的 NULL 与类型口径、
逐个分组集分别执行的朴素参考交叉核对、以及错误定位。
"""

from __future__ import annotations

import unittest

from rel3vl import ExpressionError, PlanError, SchemaError, execute

try:  # 同时兼容 `python -m unittest discover -s tests` 与包方式运行
    from ._fixtures import (
        col,
        cmp,
        employees_table,
        employees_with_dup_table,
        lit,
        naive_grouping_sets,
        scan,
    )
except ImportError:  # pragma: no cover
    from _fixtures import (  # type: ignore[no-redef]
        col,
        cmp,
        employees_table,
        employees_with_dup_table,
        lit,
        naive_grouping_sets,
        scan,
    )


def agg(child, grouping_sets, aggregates):
    return {
        "op": "aggregate",
        "input": child,
        "grouping_sets": grouping_sets,
        "aggregates": aggregates,
    }


def gcol(source, name):
    return {"table": source, "column": name}


def count_star(name="n"):
    return {"name": name, "func": "count"}


def count_expr(name, expr):
    return {"name": name, "func": "count", "expr": expr}


def sum_expr(name, expr):
    return {"name": name, "func": "sum", "expr": expr}


def emp_tables():
    return {"employees": employees_table()}


class TestGroupingSetsHandComputed(unittest.TestCase):
    """两级小汇总（按 dept + 全表）手工袋计数。"""

    def test_two_level_rollup_hand_computed(self):
        # employees: (1,A,10) (2,A,NULL) (3,B,20) (4,NULL,30)
        plan = agg(
            scan("employees", "e"),
            [[gcol("e", "dept")], []],
            [count_star("n"), sum_expr("s", col("e", "score"))],
        )
        result = execute(plan, emp_tables())
        self.assertEqual(
            [(c["source"], c["name"]) for c in result["columns"]],
            [("e", "dept"), ("", "n"), ("", "s"), ("", "grouping")],
        )
        # 组顺序 = 键首次出现顺序；分组集顺序 = 给定顺序。
        # 位图：dept 参与 -> 1；空分组集 -> 0。
        self.assertEqual(
            result["rows"],
            [
                ["A", 2, 10, 1],     # dept='A'：score NULL 被 sum 跳过
                ["B", 1, 20, 1],
                [None, 1, 30, 1],    # 真实 NULL 部门，bit 置位
                [None, 4, 60, 0],    # 全表汇总：dept 列的 NULL 是"空列"，bit=0
            ],
        )

    def test_bitmap_distinguishes_real_null_from_empty_column(self):
        plan = agg(
            scan("employees", "e"),
            [[gcol("e", "dept")], []],
            [count_star("n")],
        )
        rows = execute(plan, emp_tables())["rows"]
        null_dept_row = rows[2]    # 输入中真实 NULL 部门
        grand_total_row = rows[3]  # 汇总产生的空列
        self.assertIsNone(null_dept_row[0])
        self.assertIsNone(grand_total_row[0])
        self.assertEqual(null_dept_row[2], 0b1)   # 真实 NULL：dept 参与分组
        self.assertEqual(grand_total_row[2], 0b0)  # 空列：dept 不参与

    def test_bag_duplicates_counted(self):
        # employees_with_dup 额外重复一行 (1,'A',10)
        plan = agg(
            scan("employees", "e"),
            [[]],
            [count_star("n"), sum_expr("s", col("e", "score"))],
        )
        result = execute(plan, {"employees": employees_with_dup_table()})
        self.assertEqual(result["rows"], [[5, 70, 0]])

    def test_same_value_nulls_group_together(self):
        tables = {
            "t": {
                "columns": ["k", "v"],
                "rows": [[None, 1], [None, 2], ["x", 3], [None, 4]],
            }
        }
        plan = agg(
            scan("t", "t"),
            [[gcol("t", "k")]],
            [count_star("n"), sum_expr("s", col("t", "v"))],
        )
        rows = execute(plan, tables)["rows"]
        # 三个 NULL 键同组（IS NOT DISTINCT FROM），且按首现顺序在 'x' 前
        self.assertEqual(rows, [[None, 3, 7, 1], ["x", 1, 3, 1]])

    def test_duplicate_grouping_sets_kept(self):
        plan = agg(
            scan("employees", "e"),
            [[gcol("e", "dept")], [gcol("e", "dept")]],
            [count_star("n")],
        )
        rows = execute(plan, emp_tables())["rows"]
        # 同一分组集出现两次 => 结果各出一份，不去重
        self.assertEqual(
            rows,
            [
                ["A", 2, 1], ["B", 1, 1], [None, 1, 1],
                ["A", 2, 1], ["B", 1, 1], [None, 1, 1],
            ],
        )

    def test_group_key_order_is_first_appearance(self):
        tables = {
            "t": {
                "columns": ["k"],
                "rows": [["b"], ["a"], ["b"], ["c"], ["a"]],
            }
        }
        plan = agg(scan("t", "t"), [[gcol("t", "k")]], [count_star("n")])
        rows = execute(plan, tables)["rows"]
        self.assertEqual(rows, [["b", 2, 1], ["a", 2, 1], ["c", 1, 1]])


class TestGroupingSetsEmptyInput(unittest.TestCase):
    def setUp(self):
        self.tables = {"t": {"columns": ["k", "v"], "rows": []}}

    def test_empty_input_empty_grouping_set_one_row(self):
        plan = agg(
            scan("t", "t"),
            [[]],
            [count_star("n"), sum_expr("s", col("t", "v"))],
        )
        rows = execute(plan, self.tables)["rows"]
        self.assertEqual(rows, [[0, None, 0]])  # count=0、sum=NULL

    def test_empty_input_nonempty_grouping_set_zero_rows(self):
        plan = agg(
            scan("t", "t"),
            [[gcol("t", "k")]],
            [count_star("n"), sum_expr("s", col("t", "v"))],
        )
        self.assertEqual(execute(plan, self.tables)["rows"], [])

    def test_empty_input_mixed_sets(self):
        plan = agg(
            scan("t", "t"),
            [[gcol("t", "k")], []],
            [count_star("n")],
        )
        # 非空集零行；空集仍有一行
        self.assertEqual(execute(plan, self.tables)["rows"], [[None, 0, 0]])


class TestAggregateNullAndTypes(unittest.TestCase):
    def test_sum_all_null_is_null(self):
        tables = {"t": {"columns": ["v"], "rows": [[None], [None]]}}
        plan = agg(scan("t", "t"), [[]], [sum_expr("s", col("t", "v"))])
        self.assertEqual(execute(plan, tables)["rows"], [[None, 0]])

    def test_count_expr_skips_null(self):
        plan = agg(
            scan("employees", "e"),
            [[]],
            [count_star("n"), count_expr("ns", col("e", "score"))],
        )
        rows = execute(plan, emp_tables())["rows"]
        self.assertEqual(rows, [[4, 3, 0]])  # score 有一个 NULL

    def test_sum_int_stays_int_and_float_mix(self):
        tables = {"t": {"columns": ["v"], "rows": [[1], [2], [3]]}}
        plan = agg(scan("t", "t"), [[]], [sum_expr("s", col("t", "v"))])
        rows = execute(plan, tables)["rows"]
        self.assertEqual(rows[0][0], 6)
        self.assertIsInstance(rows[0][0], int)
        tables_f = {"t": {"columns": ["v"], "rows": [[1], [0.5]]}}
        rows_f = execute(plan, tables_f)["rows"]
        self.assertEqual(rows_f[0][0], 1.5)
        self.assertIsInstance(rows_f[0][0], float)

    def test_sum_rejects_bool_and_str(self):
        for bad in (True, "x"):
            tables = {"t": {"columns": ["v"], "rows": [[bad]]}}
            plan = agg(scan("t", "t"), [[]], [sum_expr("s", col("t", "v"))])
            with self.assertRaises(ExpressionError):
                execute(plan, tables)


class TestAggregatePlacement(unittest.TestCase):
    """aggregate 可放在 filter / project / join 之后。"""

    def test_after_filter(self):
        plan = agg(
            {
                "op": "filter",
                "input": scan("employees", "e"),
                "expr": cmp(">=", col("e", "score"), lit(15)),
            },
            [[]],
            [count_star("n")],
        )
        # score>=15：id=3,4 保留（NULL 分数 UNKNOWN 被丢）
        self.assertEqual(execute(plan, emp_tables())["rows"], [[2, 0]])

    def test_after_project(self):
        plan = agg(
            {
                "op": "project",
                "input": scan("employees", "e"),
                "columns": [
                    {"name": "d", "table": "p", "expr": col("e", "dept")},
                ],
            },
            [[gcol("p", "d")]],
            [count_star("n")],
        )
        rows = execute(plan, emp_tables())["rows"]
        self.assertEqual(rows, [["A", 2, 1], ["B", 1, 1], [None, 1, 1]])

    def test_after_join(self):
        tables = {
            "employees": employees_table(),
            "departments": {
                "columns": ["did", "dname"],
                "rows": [["A", "Alpha"], ["B", "Beta"]],
            },
        }
        plan = agg(
            {
                "op": "inner_join",
                "left": scan("employees", "e"),
                "right": scan("departments", "d"),
                "on": cmp("=", col("e", "dept"), col("d", "did")),
            },
            [[gcol("d", "dname")], []],
            [count_star("n"), sum_expr("s", col("e", "score"))],
        )
        rows = execute(plan, tables)["rows"]
        # NULL 部门不匹配；id=2 分数 NULL 被 sum 跳过
        self.assertEqual(
            rows,
            [["Alpha", 2, 10, 1], ["Beta", 1, 20, 1], [None, 3, 30, 0]],
        )


class TestAggregateNaiveCrossCheck(unittest.TestCase):
    """与逐个分组集分别执行的朴素参考（独立实现）逐行核对。"""

    def check(self, child, grouping_sets, aggregates, tables):
        plan = agg(child, grouping_sets, aggregates)
        got = [tuple(r) for r in execute(plan, tables)["rows"]]
        want = naive_grouping_sets(child, grouping_sets, aggregates, tables)
        self.assertEqual(got, [tuple(r) for r in want])

    def test_two_level_with_nulls(self):
        self.check(
            scan("employees", "e"),
            [[gcol("e", "dept")], []],
            [
                count_star("n"),
                sum_expr("s", col("e", "score")),
                count_expr("ns", col("e", "score")),
            ],
            emp_tables(),
        )

    def test_multi_column_sets_and_duplicates(self):
        self.check(
            scan("employees", "e"),
            [
                [gcol("e", "dept"), gcol("e", "id")],
                [gcol("e", "dept")],
                [gcol("e", "dept")],  # 重复集
                [],
            ],
            [count_star("n"), sum_expr("s", col("e", "score"))],
            {"employees": employees_with_dup_table()},
        )

    def test_empty_table(self):
        self.check(
            scan("t", "t"),
            [[gcol("t", "k")], []],
            [count_star("n"), sum_expr("s", col("t", "v"))],
            {"t": {"columns": ["k", "v"], "rows": []}},
        )

    def test_after_filter_cross_check(self):
        child = {
            "op": "filter",
            "input": scan("employees", "e"),
            "expr": cmp(">=", col("e", "score"), lit(15)),
        }
        self.check(
            child,
            [[gcol("e", "dept")], []],
            [count_star("n"), sum_expr("s", col("e", "score"))],
            emp_tables(),
        )


class TestAggregateErrors(unittest.TestCase):
    def test_unknown_grouping_column(self):
        plan = agg(scan("employees", "e"), [[gcol("e", "nope")]], [count_star()])
        with self.assertRaises(SchemaError):
            execute(plan, emp_tables())

    def test_duplicate_column_within_one_set(self):
        plan = agg(
            scan("employees", "e"),
            [[gcol("e", "dept"), gcol("e", "dept")]],
            [count_star()],
        )
        with self.assertRaises(PlanError):
            execute(plan, emp_tables())

    def test_unknown_func(self):
        plan = agg(scan("employees", "e"), [[]], [{"name": "a", "func": "avg"}])
        with self.assertRaises(PlanError):
            execute(plan, emp_tables())

    def test_sum_requires_expr(self):
        plan = agg(scan("employees", "e"), [[]], [{"name": "s", "func": "sum"}])
        with self.assertRaises(PlanError):
            execute(plan, emp_tables())

    def test_empty_grouping_sets_rejected(self):
        plan = agg(scan("employees", "e"), [], [count_star()])
        with self.assertRaises(PlanError):
            execute(plan, emp_tables())

    def test_empty_aggregates_rejected(self):
        plan = agg(scan("employees", "e"), [[]], [])
        with self.assertRaises(PlanError):
            execute(plan, emp_tables())

    def test_duplicate_aggregate_names_rejected(self):
        plan = agg(
            scan("employees", "e"),
            [[]],
            [count_star("n"), count_star("n")],
        )
        with self.assertRaises(SchemaError):
            execute(plan, emp_tables())

    def test_reserved_grouping_name_rejected(self):
        plan = agg(scan("employees", "e"), [[]], [count_star("grouping")])
        with self.assertRaises(SchemaError):
            execute(plan, emp_tables())

    def test_error_prefix_locates_node(self):
        plan = agg(scan("employees", "e"), [[gcol("e", "nope")]], [count_star()])
        with self.assertRaises(SchemaError) as ctx:
            execute(plan, emp_tables())
        self.assertIn("[node#0](aggregate)", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
