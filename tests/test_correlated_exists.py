"""相关 EXISTS / NOT EXISTS 的验收测试。

覆盖题面点名的边界：

* 子计划输出**全 NULL 行**也算存在；
* 外层重复行保留重数与确定顺序，内层多个匹配不复制外层行；
* 相关比较仍走三值逻辑：NULL = NULL 为 UNKNOWN，不构成存在；
* 空内表；两层相关嵌套中的同名列；外层参数透传；
* 列作用域：未声明的外层列 / 不存在的列一律编译期 SchemaError，
  绝不当 NULL；层数、节点数上限；
* 相关缓存键把来源限定名与 NULL 哨兵一起入键，且一次执行失败
  （求值期异常）不会污染下一次执行；
* 与独立朴素参考（``_fixtures.naive_expected``，逐外层行执行、
  不 import 核心）逐袋交叉核对。
"""

from __future__ import annotations

import unittest
from collections import Counter

from rel3vl import (
    MAX_CORRELATION_DEPTH,
    MAX_NODES,
    ExpressionError,
    PlanError,
    QueryError,
    SchemaError,
    execute,
)
from rel3vl.values import value_key

try:  # 兼容 `python -m unittest discover -s tests` 与包方式运行
    from ._fixtures import (
        awards_table,
        band,
        cmp,
        col,
        corr,
        corr_tables,
        customers_table,
        customers_with_orders_plan,
        empty_orders_table,
        exists_node,
        is_null,
        lit,
        naive_expected,
        orders_table,
        players_table,
        scan,
        team_player_award_exists_plan,
        team_player_award_exists_with_passthrough_plan,
        teams_table,
    )
except ImportError:  # pragma: no cover - 仅为不同启动方式的兼容分支
    from _fixtures import (  # type: ignore[no-redef]
        awards_table,
        band,
        cmp,
        col,
        corr,
        corr_tables,
        customers_table,
        customers_with_orders_plan,
        empty_orders_table,
        exists_node,
        is_null,
        lit,
        naive_expected,
        orders_table,
        players_table,
        scan,
        team_player_award_exists_plan,
        team_player_award_exists_with_passthrough_plan,
        teams_table,
    )


def bag(result):
    return Counter(tuple(r) for r in result["rows"])


def null_project_subplan():
    # 子计划不做任何过滤，只投影一个全 NULL 列：
    # 内表非空时每个外层行都得到全 NULL 行 -> EXISTS 恒真。
    return {
        "op": "project",
        "input": scan("orders", "o"),
        "columns": [{"name": "z", "expr": lit(None)}],
    }


class TestExistsBasicSemantics(unittest.TestCase):
    def setUp(self):
        self.tables = corr_tables()

    def test_exists_matches_manual_bag(self):
        # cid=1 Ann 两行（重数保留）；cid=2 Bob 一行；
        # cid=3/4 无订单；cid=None 与订单 None 行比较为 UNKNOWN -> 不存在。
        res = execute(customers_with_orders_plan(False), self.tables)
        self.assertEqual(res["rows"], [[1, "Ann"], [1, "Ann"], [2, "Bob"]])
        self.assertEqual(bag(res), Counter({(1, "Ann"): 2, (2, "Bob"): 1}))

    def test_not_exists_matches_manual_bag(self):
        res = execute(customers_with_orders_plan(True), self.tables)
        # 顺序即外层 scan 顺序；cid=None 的 Nemo 因 UNKNOWN 也归入"不存在"
        self.assertEqual(res["rows"], [[3, "Cyd"], [4, None], [None, "Nemo"]])

    def test_multiple_inner_matches_do_not_duplicate_outer_row(self):
        # cid=1 有两个订单（oid 10/11），但 Ann 在结果中只出现两次
        # （外层本身两行），而不是 2 x 2 = 4 次。
        res = execute(customers_with_orders_plan(False), self.tables)
        self.assertEqual(bag(res)[(1, "Ann")], 2)
        self.assertEqual(len(res["rows"]), 3)

    def test_exists_sees_all_null_subplan_rows(self):
        # 存在性只看"有没有行"：全 NULL 投影行也算存在。
        plan = exists_node(
            scan("customers", "c"),
            null_project_subplan(),
            [corr("c", "cid", "oc", "cid")],
        )
        res = execute(plan, self.tables)
        # orders 非空 -> 6 个外层行（含重复/NULL）全部保留，行内容不变
        self.assertEqual(len(res["rows"]), 6)
        self.assertEqual(bag(res)[(1, "Ann")], 2)

    def test_not_exists_with_all_null_rows_keeps_nothing(self):
        plan = exists_node(
            scan("customers", "c"),
            null_project_subplan(),
            [corr("c", "cid", "oc", "cid")],
            negate=True,
        )
        self.assertEqual(execute(plan, self.tables)["rows"], [])

    def test_null_equals_null_is_unknown_no_match(self):
        # customers 的 (None, "Nemo") 与 orders 的 (13, None)：
        # NULL = NULL 为 UNKNOWN，filter 丢弃该内行吗？不——子计划是
        # filter(o.cid = oc.cid)，UNKNOWN 谓词下唯一内表行被丢弃 -> 空。
        res = execute(customers_with_orders_plan(False), self.tables)
        self.assertNotIn([None, "Nemo"], res["rows"])
        res_not = execute(customers_with_orders_plan(True), self.tables)
        self.assertIn([None, "Nemo"], res_not["rows"])

    def test_is_null_inside_subplan_still_3vl_consistent(self):
        # 子计划用 is_null 判定：外层 cid 为 NULL 的客户能"存在"，
        # 证明相关参数值本身可以是 NULL 且严格判定不产生 UNKNOWN。
        sub = {
            "op": "filter",
            "input": scan("orders", "o"),
            "expr": is_null(col("oc", "cid")),
        }
        plan = exists_node(
            scan("customers", "c"), sub, [corr("c", "cid", "oc", "cid")]
        )
        res = execute(plan, self.tables)
        # 仅外层 cid NULL 的 Nemo 满足（每行独立取参数，is_null 为 TRUE）
        self.assertEqual(res["rows"], [[None, "Nemo"]])

    def test_empty_inner_table(self):
        tables = dict(self.tables)
        tables["orders"] = empty_orders_table()
        self.assertEqual(execute(customers_with_orders_plan(False), tables)["rows"], [])
        not_res = execute(customers_with_orders_plan(True), tables)
        # NOT EXISTS + 空内表：外层全部行（含重数/顺序）原样保留
        self.assertEqual(
            not_res["rows"],
            [[1, "Ann"], [1, "Ann"], [2, "Bob"], [3, "Cyd"], [4, None], [None, "Nemo"]],
        )

    def test_output_schema_is_outer_schema(self):
        res = execute(customers_with_orders_plan(False), self.tables)
        self.assertEqual(
            [(c["source"], c["name"]) for c in res["columns"]],
            [("c", "cid"), ("c", "cname")],
        )


class TestCorrelationScope(unittest.TestCase):
    def setUp(self):
        self.tables = corr_tables()

    def _exists_with_subplan_expr(self, inner_expr, corrs=()):
        sub = {"op": "filter", "input": scan("orders", "o"), "expr": inner_expr}
        return exists_node(scan("customers", "c"), sub, list(corrs))

    def test_undeclared_outer_column_rejected_not_treated_as_null(self):
        # 子计划直接引用外层真实限定名 c.cid，但 correlations 没声明：
        # 必须编译期 SchemaError，绝不能把它当 NULL 字面量静默执行。
        plan = self._exists_with_subplan_expr(
            cmp("=", col("o", "cid"), col("c", "cid"))
        )
        with self.assertRaises(SchemaError) as ctx:
            execute(plan, self.tables)
        self.assertIn("c.cid", str(ctx.exception))

    def test_outer_column_that_does_not_exist_rejected(self):
        # 声明了绑定，但外层根本没有这一列 -> 同样不能当 NULL。
        plan = self._exists_with_subplan_expr(
            cmp("=", col("o", "cid"), col("oc", "v")),
            corrs=[corr("c", "nope", "oc", "v")],
        )
        with self.assertRaises(SchemaError) as ctx:
            execute(plan, self.tables)
        self.assertIn("nope", str(ctx.exception))

    def test_inner_name_collision_with_real_column_rejected(self):
        # 绑定名与子计划实际列 (o, cid) 同名 -> 引用歧义，编译期拒绝。
        plan = self._exists_with_subplan_expr(
            cmp("=", col("o", "cid"), col("o", "cid")),  # 自比较
            corrs=[corr("c", "cid", "o", "cid")],
        )
        with self.assertRaises(SchemaError):
            execute(plan, self.tables)

    def test_duplicate_inner_binding_rejected(self):
        sub = null_project_subplan()
        plan = exists_node(
            scan("customers", "c"),
            sub,
            [corr("c", "cid", "oc", "v"), corr("c", "cname", "oc", "v")],
        )
        with self.assertRaises(PlanError) as ctx:
            execute(plan, self.tables)
        self.assertIn("重复绑定", str(ctx.exception))

    def test_malformed_correlation_rejected(self):
        sub = null_project_subplan()
        bad_bindings = [
            {"outer_table": "c", "outer_column": "cid"},  # 缺内层字段
            {"outer_table": "c", "outer_column": "cid",
             "inner_alias": "oc", "inner_name": "v", "extra": 1},  # 多字段
            ["c", "cid", "oc", "v"],  # 不是 dict
            {"outer_table": "", "outer_column": "cid",
             "inner_alias": "oc", "inner_name": "v"},  # 空字符串
        ]
        for b in bad_bindings:
            with self.subTest(b=b):
                plan = exists_node(scan("customers", "c"), sub, [b])
                with self.assertRaises(PlanError):
                    execute(plan, self.tables)

    def test_correlations_must_be_list(self):
        plan = self._exists_with_subplan_expr(lit(True))
        plan["correlations"] = {"outer_table": "c"}
        with self.assertRaises(PlanError):
            execute(plan, self.tables)


class TestTwoLevelCorrelation(unittest.TestCase):
    def setUp(self):
        self.tables = corr_tables()

    def test_two_level_same_named_column(self):
        # teams.gid 与 players.gid 同名：经显式绑定 tp.gid 比较。
        # team 1（两行重复）有获奖队员 pid=10；team 2 有队员 pid=12 获奖；
        # team 3 无队员；gid NULL 与队员 NULL gid 比较为 UNKNOWN。
        res = execute(team_player_award_exists_plan(), self.tables)
        self.assertEqual(res["rows"], [[1], [1], [2]])
        self.assertEqual(bag(res), Counter({(1,): 2, (2,): 1}))

    def test_two_level_passthrough_requires_explicit_redeclaration(self):
        plan = team_player_award_exists_with_passthrough_plan()
        res = execute(plan, self.tables)
        # team1: 队员(10,gid1) 奖 aid=100/101 > 1 -> 存在；
        # team2: 队员(12,gid2) 奖 aid=102 > 2 -> 存在。
        self.assertEqual(bag(res), Counter({(1,): 2, (2,): 1}))

    def test_grandchild_cannot_see_outer_column_without_passthrough(self):
        # 孙计划直接引用未透传的 tp.gid -> SchemaError（作用域不跨声明透传列）。
        grand = {
            "op": "filter",
            "input": scan("awards", "a"),
            "expr": band(
                cmp("=", col("a", "pid"), col("ip", "pid")),
                cmp(">", col("a", "aid"), col("tp", "gid")),  # 未在孙层声明
            ),
        }
        inner = exists_node(
            scan("players", "p"),
            grand,
            [corr("p", "pid", "ip", "pid")],  # 只绑了 pid，没透传 tp.gid
        )
        sub = {"op": "filter", "input": inner,
               "expr": cmp("=", col("p", "gid"), col("tp", "gid"))}
        plan = exists_node(
            scan("teams", "t"), sub, [corr("t", "gid", "tp", "gid")]
        )
        with self.assertRaises(SchemaError) as ctx:
            execute(plan, self.tables)
        self.assertIn("tp.gid", str(ctx.exception))

    def test_three_level_nesting_rejected(self):
        # exists(exists(exists(...)))：第三层在编译期拒绝。
        level3 = exists_node(
            scan("awards", "a"),
            {"op": "filter", "input": scan("awards", "a2"),
             "expr": cmp("=", col("a2", "aid"), col("p3", "aid"))},
            [corr("a", "aid", "p3", "aid")],
        )
        level2 = exists_node(
            scan("players", "p"), level3, [corr("p", "pid", "p2", "pid")]
        )
        level1 = exists_node(
            scan("teams", "t"), level2, [corr("t", "gid", "p1", "gid")]
        )
        with self.assertRaises(PlanError) as ctx:
            execute(level1, self.tables)
        self.assertIn(str(MAX_CORRELATION_DEPTH), str(ctx.exception))


class TestCorrelationCacheKey(unittest.TestCase):
    def test_value_key_rules(self):
        # NULL 哨兵不与任何真实标量撞键（含 0、""、"None"）。
        nk = value_key(None)
        for other in (0, "", "None", False, 0.0):
            self.assertNotEqual(nk, value_key(other))
        # bool 带类型标签：True 与 1 不共享缓存条目。
        self.assertNotEqual(value_key(True), value_key(1))
        self.assertNotEqual(value_key(False), value_key(0))
        # int/float 按数学值同键（IS NOT DISTINCT FROM 口径）。
        self.assertEqual(value_key(1), value_key(1.0))
        # str 与数值隔开。
        self.assertNotEqual(value_key("1"), value_key(1))
        # 两个 NULL 必须同键（缓存才能正确复用）。
        self.assertEqual(value_key(None), value_key(None))

    def test_cache_distinguishes_bindings_by_source_qualified_name(self):
        # 同一节点两个绑定；outer (1,9) 与 (9,1) 值集合相同，
        # 但来源限定名不同 -> 键必须区分，前者存在、后者不存在。
        tables = {
            "outer": {"columns": ["v1", "v2"], "rows": [[1, 9], [9, 1]]},
            "inner": {"columns": ["k1", "k2"], "rows": [[1, 9]]},
        }
        sub = {
            "op": "filter",
            "input": scan("inner", "i"),
            "expr": band(
                cmp("=", col("i", "k1"), col("q", "v1")),
                cmp("=", col("i", "k2"), col("q", "v2")),
            ),
        }
        plan = exists_node(
            scan("outer", "u"),
            sub,
            [corr("u", "v1", "q", "v1"), corr("u", "v2", "q", "v2")],
        )
        res = execute(plan, tables)
        self.assertEqual(res["rows"], [[1, 9]])

    def test_cache_null_binding_not_shared_with_other_rows(self):
        # 行 (1, None) 比较得 UNKNOWN -> 不存在；行 (1, 2) 与内表 (1,2) 匹配。
        # NULL 必须以独立哨兵进入键，不能错误命中另一行的缓存结果。
        tables = {
            "outer": {"columns": ["v1", "v2"], "rows": [[1, None], [1, 2], [1, None]]},
            "inner": {"columns": ["k1", "k2"], "rows": [[1, 2]]},
        }
        sub = {
            "op": "filter",
            "input": scan("inner", "i"),
            "expr": band(
                cmp("=", col("i", "k1"), col("q", "v1")),
                cmp("=", col("i", "k2"), col("q", "v2")),
            ),
        }
        plan = exists_node(
            scan("outer", "u"),
            sub,
            [corr("u", "v1", "q", "v1"), corr("u", "v2", "q", "v2")],
        )
        res = execute(plan, tables)
        # 两个 NULL 行结果一致（缓存复用），且都不与 (1,2) 串键。
        self.assertEqual(res["rows"], [[1, 2]])

    def test_repeated_executions_are_independent(self):
        # 缓存是每次 run 的局部对象：连续两次 execute 结果必须一致。
        plan = customers_with_orders_plan(False)
        r1 = execute(plan, corr_tables())
        r2 = execute(plan, corr_tables())
        self.assertEqual(r1["rows"], r2["rows"])

    def test_failed_execution_does_not_poison_next(self):
        # 子计划里 bool 与 int 比较 -> 求值期 ExpressionError；
        # 同一进程紧接着执行一个正常计划必须得到正确结果。
        tables = {
            "outer": {"columns": ["v"], "rows": [[True]]},
            "inner": {"columns": ["k"], "rows": [[1]]},
        }
        bad_sub = {
            "op": "filter",
            "input": scan("inner", "i"),
            "expr": cmp("=", col("i", "k"), col("q", "v")),  # int = bool 报错
        }
        bad_plan = exists_node(
            scan("outer", "u"), bad_sub, [corr("u", "v", "q", "v")]
        )
        with self.assertRaises(ExpressionError):
            execute(bad_plan, tables)
        # 失败后：表数据未被修改，正常计划照常执行。
        good = execute(customers_with_orders_plan(False), corr_tables())
        self.assertEqual(good["rows"], [[1, "Ann"], [1, "Ann"], [2, "Bob"]])
        # 失败的入参表也未被污染
        self.assertEqual(tables["outer"]["rows"], [[True]])


class TestLimitsAndOldInterface(unittest.TestCase):
    def test_exists_nodes_count_toward_node_limit(self):
        # 5 个 exists 嵌套即 6 个节点，全部压到链上；MAX_NODES=10 时
        # 构造 6 个 scan 叶 + 5 个 exists = 11 节点必超限。
        leaf = scan("teams", "t")
        node = leaf
        for _ in range(6):
            node = exists_node(node, scan("awards", "a"), [])
        with self.assertRaises(PlanError) as ctx:
            execute(node, corr_tables())
        self.assertIn(str(MAX_NODES), str(ctx.exception))

    def test_old_join_and_project_interfaces_unchanged(self):
        # 旧接口回归：exists 子计划内仍可自由组合 filter/project/join/distinct，
        # 且相关参数与普通列在同一表达式中工作。
        tables = corr_tables()
        sub = {
            "op": "project",
            "input": {
                "op": "distinct",
                "input": {
                    "op": "filter",
                    "input": {
                        "op": "inner_join",
                        "left": scan("orders", "o"),
                        "right": scan("customers", "c2"),
                        "on": cmp("=", col("o", "cid"), col("c2", "cid")),
                    },
                    # 相关参数与实际连接列同时出现在谓词中
                    "expr": cmp("=", col("o", "cid"), col("oc", "cid")),
                },
            },
            "columns": [{"name": "x", "expr": col("o", "oid")}],
        }
        plan = exists_node(
            scan("customers", "c"), sub, [corr("c", "cid", "oc", "cid")]
        )
        res = execute(plan, tables)
        self.assertEqual(bag(res), Counter({(1, "Ann"): 2, (2, "Bob"): 1}))


class TestNaiveReferenceCrossCheck(unittest.TestCase):
    """与独立朴素参考（逐外层行执行，Counter 袋，不 import 核心）交叉核对。"""

    def test_cross_check_exists_family(self):
        tables = corr_tables()
        plans = [
            customers_with_orders_plan(False),
            customers_with_orders_plan(True),
            team_player_award_exists_plan(),
            team_player_award_exists_with_passthrough_plan(),
            exists_node(
                scan("customers", "c"),
                null_project_subplan(),
                [corr("c", "cid", "oc", "cid")],
            ),
            {  # 子计划内组合 filter + union_all（右袋列名按左模式定位）
                "op": "not_exists",
                "input": scan("teams", "t"),
                "subplan": {
                    "op": "filter",
                    "input": {
                        "op": "union_all",
                        "left": scan("players", "pl"),
                        "right": scan("players", "pr"),
                    },
                    "expr": cmp("=", col("pl", "gid"), col("q", "g")),
                },
                "correlations": [corr("t", "gid", "q", "g")],
            },
        ]
        for plan in plans:
            with self.subTest(op=plan["op"]):
                got = bag(execute(plan, tables))
                _schema, expected = naive_expected(plan, tables)
                self.assertEqual(got, expected)

    def test_cross_check_empty_inner(self):
        tables = dict(corr_tables())
        tables["orders"] = empty_orders_table()
        for negate in (False, True):
            plan = customers_with_orders_plan(negate)
            got = bag(execute(plan, tables))
            _schema, expected = naive_expected(plan, tables)
            self.assertEqual(got, expected)

    def test_cross_check_randomish_enumeration(self):
        # 小规模穷举：外层 0..5（含 None/True 不便于比较，这里仅 int+None），
        # 内表枚举若干子集，逐袋与朴素参考核对。
        bases = [
            [1, 1, 2, None, 3],
            [None, None, 2, 2],
            [1, 9, 9, 1],
        ]
        inner_choices = [
            [1, 2],
            [1],
            [None],
            [],
            [1, None, 3],
        ]
        for base in bases:
            for inner_rows in inner_choices:
                tables = {
                    "outer": {"columns": ["v"], "rows": [[v] for v in base]},
                    "inner": {"columns": ["k"], "rows": [[v] for v in inner_rows]},
                }
                for negate in (False, True):
                    sub = {
                        "op": "filter",
                        "input": scan("inner", "i"),
                        "expr": cmp("=", col("i", "k"), col("q", "v")),
                    }
                    plan = exists_node(
                        scan("outer", "u"), sub,
                        [corr("u", "v", "q", "v")], negate=negate,
                    )
                    got = bag(execute(plan, tables))
                    _schema, expected = naive_expected(plan, tables)
                    self.assertEqual(
                        got, expected,
                        msg=f"base={base} inner={inner_rows} not={negate}",
                    )


if __name__ == "__main__":
    unittest.main()
