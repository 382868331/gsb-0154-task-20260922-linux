"""核心 3VL 关系语义测试（验收点：手工袋计数 + ON/WHERE 差异 + NULL 等）。"""

from __future__ import annotations

import math
import unittest
from collections import Counter

from rel3vl import (
    ExpressionError,
    MAX_NODES,
    MAX_ROWS_PER_TABLE,
    PlanError,
    QueryError,
    SchemaError,
    TableError,
    execute,
)

try:  # 同时兼容 `python -m unittest discover -s tests` 与包方式运行
    from ._fixtures import (
        band,
        bnot,
        bor,
        cmp,
        coalesce,
        col,
        departments_table,
        employees_table,
        employees_with_dup_table,
        empty_departments_table,
        is_null,
        join_plan,
        lit,
        naive_expected,
        scan,
        scan_dep,
        scan_emp,
        tables_fixture,
    )
except ImportError:  # pragma: no cover - 仅为不同启动方式的兼容分支
    from _fixtures import (  # type: ignore[no-redef]
        band,
        bnot,
        bor,
        cmp,
        coalesce,
        col,
        departments_table,
        employees_table,
        employees_with_dup_table,
        empty_departments_table,
        is_null,
        join_plan,
        lit,
        naive_expected,
        scan,
        scan_dep,
        scan_emp,
        tables_fixture,
    )


def bag_of(result):
    return Counter(tuple(r) for r in result["rows"])


def column_names(result):
    return [(c["source"], c["name"]) for c in result["columns"]]


class TestScanAndProjection(unittest.TestCase):
    def test_scan_bag_preserves_duplicates_and_order(self):
        tables = {"t": {"columns": ["x"], "rows": [[1], [1], [2]]}}
        res = execute(scan("t"), tables)
        self.assertEqual(res["rows"], [[1], [1], [2]])  # 确定顺序
        self.assertEqual(bag_of(res), Counter({(1,): 2, (2,): 1}))

    def test_scan_alias_sets_source_id(self):
        res = execute(scan("t", "s"), {"t": {"columns": ["x"], "rows": []}})
        self.assertEqual(column_names(res), [("s", "x")])

    def test_project_renames_and_evaluates_expr(self):
        plan = {
            "op": "project",
            "input": scan("t"),
            "columns": [{"name": "y", "expr": lit(9)}],
        }
        tables = {"t": {"columns": ["x"], "rows": [[1], [2]]}}
        res = execute(plan, tables)
        self.assertEqual(column_names(res), [("", "y")])
        self.assertEqual(res["rows"], [[9], [9]])


class TestThreeValuedLogicFilter(unittest.TestCase):
    def setUp(self):
        self.tables = tables_fixture()

    def test_filter_keeps_only_true_not_false_or_unknown(self):
        # score = NULL 的行：score >= 15 -> UNKNOWN，必须被丢弃
        plan = {
            "op": "filter",
            "input": scan_emp(),
            "expr": cmp(">=", col("e", "score"), lit(15)),
        }
        res = execute(plan, self.tables)
        self.assertEqual(bag_of(res), Counter({(3, "B", 20): 1, (4, None, 30): 1}))

    def test_null_equals_null_is_unknown_and_filters_out(self):
        # dept 为 NULL 的员工不匹配 NULL 部门：col = NULL 永远 UNKNOWN
        plan = {
            "op": "filter",
            "input": scan_emp(),
            "expr": cmp("=", col("e", "dept"), lit(None)),
        }
        res = execute(plan, self.tables)
        self.assertEqual(res["rows"], [])

    def test_is_null_finds_nulls(self):
        plan = {
            "op": "filter",
            "input": scan_emp(),
            "expr": is_null(col("e", "score")),
        }
        res = execute(plan, self.tables)
        self.assertEqual(bag_of(res), Counter({(2, "A", None): 1}))

    def test_not_unknown_is_unknown(self):
        # NOT (dept = NULL) 仍是 UNKNOWN，不是 TRUE
        plan = {
            "op": "filter",
            "input": scan_emp(),
            "expr": bnot(cmp("=", col("e", "dept"), lit(None))),
        }
        self.assertEqual(execute(plan, self.tables)["rows"], [])

    def test_and_or_truth_tables_over_unknown(self):
        cases = [
            # (表达式, 期望)；None 表示 UNKNOWN
            (band(lit(False), lit(None)), False),  # FALSE AND UNKNOWN = FALSE
            (band(lit(True), lit(None)), None),    # TRUE AND UNKNOWN = UNKNOWN
            (bor(lit(True), lit(None)), True),     # TRUE OR UNKNOWN = TRUE
            (bor(lit(False), lit(None)), None),    # FALSE OR UNKNOWN = UNKNOWN
        ]
        for expr, expected in cases:
            with self.subTest(expr=expr["kind"]):
                plan = {"op": "filter", "input": scan_emp(), "expr": expr}
                res = execute(plan, self.tables)
                if expected is True:
                    self.assertEqual(len(res["rows"]), 4)
                else:  # FALSE 与 UNKNOWN 都不满足 -> 全部丢弃
                    self.assertEqual(res["rows"], [])

    def test_coalesce_returns_first_non_null(self):
        plan = {
            "op": "project",
            "input": scan_emp(),
            "columns": [
                {
                    "name": "d_or_x",
                    "expr": coalesce(col("e", "dept"), lit("X")),
                }
            ],
        }
        res = execute(plan, self.tables)
        # 员工顺序确定：A, A, B, NULL(->X)
        self.assertEqual(res["rows"], [["A"], ["A"], ["B"], ["X"]])

    def test_coalesce_all_null(self):
        plan = {
            "op": "project",
            "input": scan_emp(),
            "columns": [
                {"name": "v", "expr": coalesce(lit(None), col("e", "score"))}
            ],
        }
        rows = execute(plan, self.tables)["rows"]
        # id=2 的 score 为 NULL -> 结果 NULL
        self.assertIn([None], rows)


class TestJoins(unittest.TestCase):
    def setUp(self):
        self.tables = tables_fixture()

    def test_inner_join_manual_bag_counts(self):
        """手工袋计数：A 部门有两个员工（其中一个 score NULL 不影响 ON），
        B 一个；NULL 部门员工与 NULL did 部门互不匹配。"""
        res = execute(join_plan(outer=False), self.tables)
        # e.id=1 -> A/Alpha; e.id=2(dept A) -> A/Alpha; e.id=3 -> B/Beta;
        # e.id=4(dept NULL) -> 无匹配
        expected = Counter(
            {
                (1, "A", 10, "A", "Alpha"): 1,
                (2, "A", None, "A", "Alpha"): 1,
                (3, "B", 20, "B", "Beta"): 1,
            }
        )
        self.assertEqual(bag_of(res), expected)
        self.assertEqual(len(res["rows"]), 3)

    def test_null_does_not_match_null_in_join_on(self):
        res = execute(join_plan(outer=False), self.tables)
        for row in res["rows"]:
            self.assertFalse(row[1] is None and row[3] is None)
        # NULL 部门的员工 id=4 不出现在内连接
        self.assertFalse(any(r[0] == 4 for r in res["rows"]))

    def test_on_vs_where_after_join_difference(self):
        """验收核心：ON 中的非匹配条件给左行补 NULL（外连接）；
        同样条件放到连接后的 WHERE 会把补 NULL 行也过滤掉。"""
        # LEFT JOIN ... ON dept=did AND e.id = 1
        on_plan = {
            "op": "left_join",
            "left": scan_emp(),
            "right": scan_dep(),
            "on": band(
                cmp("=", col("e", "dept"), col("d", "did")),
                cmp("=", col("e", "id"), lit(1)),
            ),
        }
        on_res = execute(on_plan, self.tables)
        on_bag = bag_of(on_res)
        # id=1 匹配 A；id=2(dept A 但 id<>1) 无匹配 -> 补 NULL；
        # id=3 无匹配补 NULL；id=4 NULL 部门无匹配补 NULL
        self.assertEqual(
            on_bag,
            Counter(
                {
                    (1, "A", 10, "A", "Alpha"): 1,
                    (2, "A", None, None, None): 1,
                    (3, "B", 20, None, None): 1,
                    (4, None, 30, None, None): 1,
                }
            ),
        )

        # 等价的 LEFT JOIN ON dept=did 之后 WHERE e.id = 1
        where_plan = {
            "op": "filter",
            "input": join_plan(outer=True),
            "expr": cmp("=", col("e", "id"), lit(1)),
        }
        where_res = execute(where_plan, self.tables)
        self.assertEqual(
            bag_of(where_res), Counter({(1, "A", 10, "A", "Alpha"): 1})
        )
        # 两种写法的结果袋必然不同
        self.assertNotEqual(on_bag, bag_of(where_res))

    def test_left_join_pads_only_when_zero_matches(self):
        res = execute(join_plan(outer=True), self.tables)
        expected = Counter(
            {
                (1, "A", 10, "A", "Alpha"): 1,
                (2, "A", None, "A", "Alpha"): 1,
                (3, "B", 20, "B", "Beta"): 1,
                (4, None, 30, None, None): 1,  # 唯一无匹配左行，补一次 NULL
            }
        )
        self.assertEqual(bag_of(res), expected)

    def test_left_join_empty_right_pads_every_left_row(self):
        """空右表：每个左行都补全 NULL，左行本身（含重复）保留。"""
        tables = {
            "employees": employees_table(),
            "departments": empty_departments_table(),
        }
        res = execute(join_plan(outer=True), tables)
        self.assertEqual(len(res["rows"]), 4)
        self.assertTrue(all(r[3] is None and r[4] is None for r in res["rows"]))

        # 内连接空右表 -> 空关系
        inner = execute(join_plan(outer=False), tables)
        self.assertEqual(inner["rows"], [])

    def test_duplicate_left_rows_duplicate_join_outputs(self):
        """重复左行：每行独立匹配，补 NULL 也按行重复。"""
        tables = {
            "employees": employees_with_dup_table(),  # id=1,A,10 出现两次
            "departments": empty_departments_table(),
        }
        res = execute(join_plan(outer=True), tables)
        self.assertEqual(bag_of(res)[(1, "A", 10, None, None)], 2)
        self.assertEqual(len(res["rows"]), 5)

    def test_duplicate_matches_multiply_bag_style(self):
        # 右表 A 部门重复一次 -> id=1/id=2 各匹配两行
        deps = departments_table()
        deps = {"columns": deps["columns"], "rows": deps["rows"] + [["A", "Alpha2"]]}
        res = execute(
            join_plan(outer=False),
            {"employees": employees_table(), "departments": deps},
        )
        b = bag_of(res)
        self.assertEqual(b[(1, "A", 10, "A", "Alpha")], 1)
        self.assertEqual(b[(1, "A", 10, "A", "Alpha2")], 1)
        self.assertEqual(b[(2, "A", None, "A", "Alpha")], 1)
        self.assertEqual(b[(2, "A", None, "A", "Alpha2")], 1)


class TestUnionAndDistinct(unittest.TestCase):
    def setUp(self):
        self.tables = tables_fixture()

    def test_union_all_concatenates_bags(self):
        plan = {
            "op": "union_all",
            "left": scan("a", "u"),
            "right": scan("b", "v"),
        }
        tables = {
            "a": {"columns": ["x"], "rows": [[1], [2]]},
            "b": {"columns": ["x"], "rows": [[2], [3]]},
        }
        res = execute(plan, tables)
        self.assertEqual(res["rows"], [[1], [2], [2], [3]])  # 确定顺序
        self.assertEqual(bag_of(res), Counter({(1,): 1, (2,): 2, (3,): 1}))

    def test_union_all_requires_same_column_names(self):
        plan = {
            "op": "union_all",
            "left": scan("a", "u"),
            "right": scan("b", "v"),
        }
        tables = {
            "a": {"columns": ["x"], "rows": []},
            "b": {"columns": ["y"], "rows": []},
        }
        with self.assertRaises(SchemaError):
            execute(plan, tables)

    def test_distinct_collapses_duplicates_keeps_first_order(self):
        tables = {"t": {"columns": ["x"], "rows": [[2], [1], [2], [None], [None], [1]]}}
        plan = {"op": "distinct", "input": scan("t")}
        res = execute(plan, tables)
        # DISTINCT 的 NULL 与 NULL 视为相同（IS NOT DISTINCT FROM）
        self.assertEqual(res["rows"], [[2], [1], [None]])

    def test_distinct_bools_not_merged_with_ints(self):
        tables = {"t": {"columns": ["x"], "rows": [[1], [True], [1.0], [False], [0]]}}
        res = execute({"op": "distinct", "input": scan("t")}, tables)
        # 用类型标签比较，避开 Python 字典中 1/True 哈希相同的混淆：
        # 1 与 1.0 数学值相同 -> 合并为一组；True/False 为独立类型 -> 保留。
        tagged = Counter((type(v).__name__, v) for (v,) in map(tuple, res["rows"]))
        self.assertEqual(tagged, Counter({("int", 1): 1, ("bool", True): 1,
                                          ("bool", False): 1, ("int", 0): 1}))


class TestSourceQualification(unittest.TestCase):
    def test_same_named_columns_disambiguated_by_source(self):
        tables = {
            "t": {"columns": ["v"], "rows": [[1]]},
        }
        plan = {
            "op": "inner_join",
            "left": scan("t", "l"),
            "right": scan("t", "r"),
            "on": cmp("=", col("l", "v"), col("r", "v")),
        }
        res = execute(plan, tables)
        self.assertEqual(column_names(res), [("l", "v"), ("r", "v")])
        self.assertEqual(res["rows"], [[1, 1]])

    def test_self_join_without_alias_rejected(self):
        # 两次 scan 默认来源都是表名 -> (source,name) 冲突，编译期报错
        tables = {"t": {"columns": ["v"], "rows": [[1]]}}
        plan = {
            "op": "inner_join",
            "left": scan("t"),
            "right": scan("t"),
            "on": cmp("=", col("t", "v"), col("t", "v")),
        }
        with self.assertRaises(SchemaError):
            execute(plan, tables)

    def test_unknown_column_error_is_localizable(self):
        plan = {
            "op": "filter",
            "input": scan_emp(),
            "expr": cmp("=", col("e", "nope"), lit(1)),
        }
        with self.assertRaises(SchemaError) as ctx:
            execute(plan, tables_fixture())
        self.assertIn("node#0", str(ctx.exception))
        self.assertIn("nope", str(ctx.exception))


class TestInputValidation(unittest.TestCase):
    def test_reject_nan_and_infinity(self):
        for bad in (math.nan, math.inf, -math.inf):
            tables = {"t": {"columns": ["x"], "rows": [[bad]]}}
            with self.assertRaises(TableError):
                execute(scan("t"), tables)

    def test_bool_param_not_silently_treated_as_int_but_bool_is_a_value(self):
        # bool 作为列值是合法的独立类型
        res = execute(
            scan("t"), {"t": {"columns": ["x"], "rows": [[True], [False]]}}
        )
        self.assertEqual(res["rows"], [[True], [False]])

    def test_bool_not_equal_to_int_in_comparison(self):
        plan = {
            "op": "filter",
            "input": scan("t"),
            "expr": cmp("=", col("t", "x"), lit(1)),
        }
        tables = {"t": {"columns": ["x"], "rows": [[True]]}}
        with self.assertRaises(ExpressionError):
            execute(plan, tables)

    def test_string_number_cross_comparison_rejected(self):
        plan = {
            "op": "filter",
            "input": scan("t"),
            "expr": cmp("=", col("t", "x"), lit(1)),
        }
        with self.assertRaises(ExpressionError):
            execute(plan, {"t": {"columns": ["x"], "rows": [["1"]]}})

    def test_row_width_mismatch_rejected(self):
        tables = {"t": {"columns": ["x", "y"], "rows": [[1]]}}
        with self.assertRaises(TableError):
            execute(scan("t"), tables)

    def test_unsupported_scalar_type_rejected(self):
        tables = {"t": {"columns": ["x"], "rows": [[[1, 2]]]}}
        with self.assertRaises(TableError):
            execute(scan("t"), tables)

    def test_unknown_op_rejected(self):
        with self.assertRaises(PlanError):
            execute({"op": "bogus"}, {"t": {"columns": ["x"], "rows": []}})

    def test_unknown_table_rejected(self):
        with self.assertRaises(PlanError):
            execute(scan("nope"), {"t": {"columns": ["x"], "rows": []}})

    def test_missing_field_rejected(self):
        plan = {"op": "filter", "input": scan("t")}  # 缺 expr
        with self.assertRaises(PlanError):
            execute(plan, {"t": {"columns": ["x"], "rows": []}})

    def test_node_limit(self):
        # 构造 MAX_NODES+1 个 distinct 嵌套
        node = scan("t")
        for _ in range(MAX_NODES):
            node = {"op": "distinct", "input": node}
        tables = {"t": {"columns": ["x"], "rows": []}}
        with self.assertRaises(PlanError) as ctx:
            execute(node, tables)
        self.assertIn(str(MAX_NODES), str(ctx.exception))

    def test_row_limit(self):
        tables = {"t": {"columns": ["x"], "rows": [[i] for i in range(MAX_ROWS_PER_TABLE + 1)]}}
        with self.assertRaises(TableError):
            execute(scan("t"), tables)

    def test_shared_subtree_rejected(self):
        leaf = scan("t")
        plan = {"op": "union_all", "left": leaf, "right": leaf}
        with self.assertRaises(PlanError):
            execute(plan, {"t": {"columns": ["x"], "rows": [[1]]}})

    def test_compile_failure_has_no_partial_effect(self):
        # 输入表对象在执行失败后保持原样（无"部分变更"语义可言：
        # execute 不修改入参；这里固定该契约）。
        tables = {"t": {"columns": ["x"], "rows": [[1]]}}
        snapshot = {"columns": list(tables["t"]["columns"]), "rows": [list(r) for r in tables["t"]["rows"]]}
        bad_plan = {"op": "filter", "input": scan("t"), "expr": {"kind": "col", "table": "t", "column": "z"}}
        with self.assertRaises(QueryError):
            execute(bad_plan, tables)
        self.assertEqual(tables["t"], snapshot)


class TestNaiveReferenceCrossCheck(unittest.TestCase):
    """独立朴素参考（袋计数）交叉校验 —— 仅补充，不替代手工断言。"""

    def test_cross_check_bag_of_plans(self):
        dep = departments_table()
        tables = {
            "employees": employees_with_dup_table(),
            "departments": {
                "columns": dep["columns"],
                "rows": dep["rows"] + [["A", "Alpha"]],  # 右表也含重复 A
            },
        }
        plans = [
            join_plan(outer=False),
            join_plan(outer=True),
            {"op": "distinct", "input": join_plan(outer=True)},
            {
                "op": "union_all",
                "left": {"op": "project", "input": scan_emp(),
                         "columns": [{"name": "v", "expr": col("e", "id")}]},
                "right": {"op": "project", "input": scan_dep(),
                          "columns": [{"name": "v", "expr": coalesce(lit(None), lit(7))}]},
            },
            {
                "op": "filter",
                "input": join_plan(outer=True),
                "expr": bor(
                    cmp(">", col("e", "score"), lit(15)),
                    is_null(col("d", "did")),
                ),
            },
        ]
        for plan in plans:
            with self.subTest(plan=plan["op"]):
                got = bag_of(execute(plan, tables))
                schema, exp = naive_expected(plan, tables)
                # 独立参考输出列名仅用于核对，行袋直接比较
                self.assertEqual(got, exp, msg=f"plan={plan['op']}")


if __name__ == "__main__":
    unittest.main()
