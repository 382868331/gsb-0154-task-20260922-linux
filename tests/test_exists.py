"""相关存在性子查询（exists / not_exists）测试。

验收点：显式相关列绑定、列作用域先行校验、存在性只看有无行、
外层重数与顺序保留、内层匹配不复制外层行、UNKNOWN 比较、空内表、
两层同名相关列嵌套、嵌套与节点上限、失败不污染下一次执行，
以及与独立逐行朴素参考的精确（保序）交叉校验。
"""

from __future__ import annotations

import unittest
from collections import Counter

from rel3vl import (
    MAX_CORRELATION_DEPTH,
    MAX_NODES,
    ExpressionError,
    PlanError,
    SchemaError,
    compile_plan,
    execute,
    validate_tables,
)

try:  # 同时兼容 `python -m unittest discover -s tests` 与包方式运行
    from ._fixtures import (
        bind,
        cmp,
        col,
        departments_table,
        employees_table,
        employees_with_dup_table,
        empty_departments_table,
        exists_plan,
        lit,
        naive_rows,
        scan,
        scan_dep,
        scan_emp,
        tables_fixture,
        band,
    )
except ImportError:  # pragma: no cover - 仅为不同启动方式的兼容分支
    from _fixtures import (  # type: ignore[no-redef]
        bind,
        cmp,
        col,
        departments_table,
        employees_table,
        employees_with_dup_table,
        empty_departments_table,
        exists_plan,
        lit,
        naive_rows,
        scan,
        scan_dep,
        scan_emp,
        tables_fixture,
        band,
    )


def dept_match_exists(negate=False):
    """员工存在同名部门（dept = did）的 exists / not_exists 计划。"""
    return exists_plan(
        scan_emp(),
        {
            "op": "filter",
            "input": scan_dep(),
            "expr": cmp("=", col("d", "did"), col("o", "dept")),
        },
        [bind("e", "dept", "o")],
        negate=negate,
    )


class TestExistsBasic(unittest.TestCase):
    def setUp(self):
        self.tables = tables_fixture()

    def test_exists_keeps_outer_rows_with_match(self):
        res = execute(dept_match_exists(), self.tables)
        # id=4 的 dept 为 NULL：NULL = did 恒 UNKNOWN -> 子计划无行 -> 丢弃
        self.assertEqual(
            res["rows"], [[1, "A", 10], [2, "A", None], [3, "B", 20]]
        )
        # exists 是过滤节点：输出模式与输入一致
        self.assertEqual(
            [(c["source"], c["name"]) for c in res["columns"]],
            [("e", "id"), ("e", "dept"), ("e", "score")],
        )

    def test_not_exists_is_complement(self):
        res = execute(dept_match_exists(negate=True), self.tables)
        self.assertEqual(res["rows"], [[4, None, 30]])

    def test_existence_ignores_null_fields_in_subplan_rows(self):
        """子计划产出的行全为 NULL 也算"有行"：存在性只看行数。"""
        sub = {
            "op": "project",
            "input": scan_dep(),
            "columns": [{"name": "v", "expr": lit(None)}],
        }
        res = execute(exists_plan(scan_emp(), sub, []), self.tables)
        self.assertEqual(len(res["rows"]), 4)  # departments 有 3 行全 NULL 投影
        # 空内表 + 全 NULL 投影 -> 子计划无行 -> 全丢
        tables_empty = {
            "employees": employees_table(),
            "departments": empty_departments_table(),
        }
        res_empty = execute(exists_plan(scan_emp(), sub, []), tables_empty)
        self.assertEqual(res_empty["rows"], [])

    def test_empty_inner_table(self):
        plan = exists_plan(
            scan_emp(),
            {
                "op": "filter",
                "input": scan("departments", "d"),
                "expr": cmp("=", col("d", "did"), col("o", "dept")),
            },
            [bind("e", "dept", "o")],
        )
        tables = {
            "employees": employees_table(),
            "departments": empty_departments_table(),
        }
        self.assertEqual(execute(plan, tables)["rows"], [])
        neg = exists_plan(
            scan_emp(), plan["subplan"], [bind("e", "dept", "o")], negate=True
        )
        res = execute(neg, tables)
        # not_exists：空内表 -> 全部外层行按原顺序保留
        self.assertEqual(
            res["rows"], [[1, "A", 10], [2, "A", None], [3, "B", 20], [4, None, 30]]
        )

    def test_outer_duplicates_keep_multiplicity_and_order(self):
        tables = {
            "employees": employees_with_dup_table(),  # [1,"A",10] 出现两次，在首尾
            "departments": departments_table(),
        }
        res = execute(dept_match_exists(), tables)
        self.assertEqual(
            res["rows"],
            [[1, "A", 10], [2, "A", None], [3, "B", 20], [1, "A", 10]],
        )
        self.assertEqual(
            Counter(tuple(r) for r in res["rows"])[(1, "A", 10)], 2
        )

    def test_inner_multiple_matches_do_not_duplicate_outer_row(self):
        dep = departments_table()
        dep = {"columns": dep["columns"], "rows": dep["rows"] + [["A", "Alpha2"]]}
        tables = {"employees": employees_table(), "departments": dep}
        res = execute(dept_match_exists(), tables)
        # 部门 A 有两行，但每个外层行最多保留一次
        self.assertEqual(
            res["rows"], [[1, "A", 10], [2, "A", None], [3, "B", 20]]
        )

    def test_unknown_comparison_in_subplan(self):
        """绑定值为 NULL -> 子计划比较 UNKNOWN -> 无行；exists 丢、not_exists 留。"""
        tables = {
            "employees": employees_table(),
            "nums": {"columns": ["x"], "rows": [[10], [30]]},
        }
        sub = {
            "op": "filter",
            "input": scan("nums", "n"),
            "expr": cmp("=", col("n", "x"), col("o", "score")),
        }
        b = [bind("e", "score", "o")]
        res = execute(exists_plan(scan_emp(), sub, b), tables)
        self.assertEqual(res["rows"], [[1, "A", 10], [4, None, 30]])
        # id=2 的 score 为 NULL：UNKNOWN 不匹配；id=3 的 20 不等于 10/30
        res_neg = execute(exists_plan(scan_emp(), sub, b, negate=True), tables)
        self.assertEqual(res_neg["rows"], [[2, "A", None], [3, "B", 20]])

    def test_binding_usable_inside_subplan_join_on(self):
        """绑定列在子计划内部的 join ON 中同样可见（env 贯穿子树）。"""
        tables = {
            "employees": employees_table(),
            "departments": departments_table(),
            "flag": {"columns": ["x"], "rows": [[1]]},
        }
        plan = exists_plan(
            scan_emp(),
            {
                "op": "inner_join",
                "left": scan_dep(),
                "right": scan("flag", "f"),
                "on": cmp("=", col("d", "did"), col("o", "dept")),
            },
            [bind("e", "dept", "o")],
        )
        res = execute(plan, tables)
        self.assertEqual(
            res["rows"], [[1, "A", 10], [2, "A", None], [3, "B", 20]]
        )


class TestTwoLevelNesting(unittest.TestCase):
    """两层相关嵌套，且三层使用同名列名 x（靠来源 id 区分）。"""

    def _plan(self):
        # t1.x 中那些"在 t2 中有同名行、且该值又在 t3 中有同名行"的值
        return exists_plan(
            scan("t1", "a"),
            exists_plan(
                {
                    "op": "filter",
                    "input": scan("t2", "b"),
                    "expr": cmp("=", col("b", "x"), col("p", "x")),
                },
                {
                    "op": "filter",
                    "input": scan("t3", "c"),
                    "expr": cmp("=", col("c", "x"), col("q", "x")),
                },
                [bind("p", "x", "q")],  # 内层从本层环境（绑定列）再绑定
            ),
            [bind("a", "x", "p")],
        )

    def test_two_level_same_column_name(self):
        tables = {
            "t1": {"columns": ["x"], "rows": [[1], [2], [3], [3]]},
            "t2": {"columns": ["x"], "rows": [[2], [3]]},
            "t3": {"columns": ["x"], "rows": [[3]]},
        }
        res = execute(self._plan(), tables)
        # x=1: t2 无 -> 假；x=2: t2 有但 t3 无 -> 假；x=3: 两层都有 -> 真（重数 2）
        self.assertEqual(res["rows"], [[3], [3]])

    def test_third_level_rejected(self):
        innermost = exists_plan(scan("t3", "c"), scan("t4", "z"), [])
        mid = exists_plan(scan("t2", "b"), innermost, [])
        outer = exists_plan(scan("t1", "a"), mid, [])
        tables = {f"t{i}": {"columns": ["x"], "rows": []} for i in range(1, 5)}
        with self.assertRaises(PlanError) as ctx:
            execute(outer, tables)
        self.assertIn(str(MAX_CORRELATION_DEPTH), str(ctx.exception))


class TestScopeValidation(unittest.TestCase):
    def setUp(self):
        self.tables = tables_fixture()

    def test_unbound_outer_source_rejected(self):
        """子计划直接引用未绑定的外层来源 id -> 编译期 SchemaError。"""
        plan = exists_plan(
            scan_emp(),
            {
                "op": "filter",
                "input": scan_dep(),
                "expr": cmp("=", col("d", "did"), col("e", "dept")),  # e 未绑定
            },
            [],
        )
        with self.assertRaises(SchemaError) as ctx:
            execute(plan, self.tables)
        self.assertIn("e.dept", str(ctx.exception))

    def test_binding_from_nonexistent_column_rejected(self):
        plan = exists_plan(
            scan_emp(),
            scan_dep(),
            [bind("e", "nope", "o")],
        )
        with self.assertRaises(SchemaError) as ctx:
            execute(plan, self.tables)
        self.assertIn("e.nope", str(ctx.exception))

    def test_binding_from_unbound_env_rejected(self):
        """外层绑定不存在于更外层环境时，内层不能绑定它。"""
        plan = exists_plan(
            scan_emp(),
            exists_plan(scan_dep(), scan("departments", "d2"),
                        [bind("o", "dept", "q")]),  # o.dept 从未声明
            [bind("e", "dept", "o2")],
        )
        with self.assertRaises(SchemaError):
            execute(plan, self.tables)

    def test_duplicate_binding_target_rejected(self):
        plan = exists_plan(
            scan_emp(),
            scan_dep(),
            [bind("e", "dept", "o"), bind("e", "id", "o", "dept")],
        )
        with self.assertRaises(SchemaError):
            execute(plan, self.tables)

    def test_binding_target_collides_with_inner_column(self):
        """绑定目标与子计划本地列同名：拒绝遮蔽。"""
        plan = exists_plan(
            scan_emp(),
            {
                "op": "filter",
                "input": scan_dep(),
                "expr": cmp("=", col("d", "did"), col("d", "did")),
            },
            [bind("e", "dept", "d", "did")],
        )
        with self.assertRaises(SchemaError):
            execute(plan, self.tables)

    def test_malformed_correlate_rejected(self):
        for bad_corr in (
            "not-a-list",
            [{"from": {"table": "e", "column": "dept"}}],            # 缺 as/column
            [{"from": {"table": "e"}, "as": "o", "column": "dept"}],  # from 缺 column
            [{"from": {"table": "e", "column": "dept"}, "as": "", "column": "x"}],
            [{"from": {"table": "e", "column": "dept"}, "as": "o",
              "column": "x", "extra": 1}],
        ):
            plan = exists_plan(scan_emp(), scan_dep(), bad_corr)
            with self.assertRaises(PlanError, msg=f"correlate={bad_corr!r}"):
                execute(plan, self.tables)

    def test_node_limit_counts_subplan_nodes(self):
        sub = scan("departments", "d")
        for _ in range(MAX_NODES - 2):  # 1(scan e)+1(exists)+1(scan d)+(MAX-2) > MAX
            sub = {"op": "distinct", "input": sub}
        plan = exists_plan(scan_emp(), sub, [])
        with self.assertRaises(PlanError) as ctx:
            execute(plan, self.tables)
        self.assertIn(str(MAX_NODES), str(ctx.exception))


class TestFailureIsolation(unittest.TestCase):
    def test_failed_run_does_not_pollute_next_execution(self):
        # 求值期类型错误：str 部门与 int 列比较 -> ExpressionError
        tables = {
            "employees": employees_table(),
            "nums": {"columns": ["x"], "rows": [[10]]},
        }
        plan = exists_plan(
            scan_emp(),
            {
                "op": "filter",
                "input": scan("nums", "n"),
                "expr": cmp("=", col("n", "x"), col("o", "dept")),
            },
            [bind("e", "dept", "o")],
        )
        with self.assertRaises(ExpressionError):
            execute(plan, tables)
        # 同一计划对象、换成类型兼容的数据再次执行：不受上次失败影响
        tables_ok = {
            "employees": {"columns": ["id", "dept"], "rows": [[1, 10], [2, 20]]},
            "nums": {"columns": ["x"], "rows": [[10]]},
        }
        plan_ok = exists_plan(
            scan("employees", "e"),
            plan["subplan"],
            [bind("e", "dept", "o")],
        )
        res = execute(plan_ok, tables_ok)
        self.assertEqual(res["rows"], [[1, 10]])

    def test_compiled_plan_reruns_independently(self):
        """compile_plan 产物可重复 run：memo 是每次 run 的局部状态。"""
        tables = {
            "employees": employees_with_dup_table(),
            "departments": departments_table(),
        }
        compiled = compile_plan(dept_match_exists(), validate_tables(tables))
        first = compiled.run()
        second = compiled.run()
        self.assertEqual(first, second)
        self.assertEqual(len(first), 4)  # 4 个匹配行（含重复），id=4 被丢


class TestNaiveRowReferenceCrossCheck(unittest.TestCase):
    """与独立逐行朴素参考（list、保序）精确交叉校验。"""

    def _check(self, plan, tables):
        got = execute(plan, tables)
        _schema, rows = naive_rows(plan, tables)
        self.assertEqual(got["rows"], [list(r) for r in rows])

    def test_cross_check_exists_plans(self):
        dep_dup = departments_table()
        dep_dup = {
            "columns": dep_dup["columns"],
            "rows": dep_dup["rows"] + [["A", "Alpha2"]],
        }
        tables_main = {
            "employees": employees_with_dup_table(),
            "departments": dep_dup,
        }
        tables_empty = {
            "employees": employees_table(),
            "departments": empty_departments_table(),
        }
        sub_match = {
            "op": "filter",
            "input": scan_dep(),
            "expr": cmp("=", col("d", "did"), col("o", "dept")),
        }
        b = [bind("e", "dept", "o")]
        cases = [
            (dept_match_exists(), tables_main),
            (dept_match_exists(negate=True), tables_main),
            (dept_match_exists(), tables_empty),
            (dept_match_exists(negate=True), tables_empty),
            # 子计划输出全 NULL 行
            (
                exists_plan(
                    scan_emp(),
                    {
                        "op": "project",
                        "input": scan_dep(),
                        "columns": [{"name": "v", "expr": lit(None)}],
                    },
                    [],
                ),
                tables_main,
            ),
            # 绑定值经 UNKNOWN 比较（score 为 NULL 时 AND 右支 UNKNOWN）
            (
                exists_plan(
                    scan_emp(),
                    {
                        "op": "filter",
                        "input": scan("departments", "d"),
                        "expr": band(
                            cmp("=", col("d", "did"), col("o", "dept")),
                            cmp(">", col("o", "score"), lit(15)),
                        ),
                    },
                    [bind("e", "dept", "o"), bind("e", "score", "o")],
                ),
                tables_main,
            ),
        ]
        for plan, tables in cases:
            with self.subTest(plan=plan["op"]):
                self._check(plan, tables)

    def test_cross_check_two_level_nesting(self):
        tables = {
            "t1": {"columns": ["x"], "rows": [[1], [2], [3], [3], [None]]},
            "t2": {"columns": ["x"], "rows": [[2], [3], [None]]},
            "t3": {"columns": ["x"], "rows": [[3], [None]]},
        }
        plan = TestTwoLevelNesting()._plan()
        self._check(plan, tables)


if __name__ == "__main__":
    unittest.main()
