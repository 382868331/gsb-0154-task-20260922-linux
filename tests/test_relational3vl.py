"""relational3vl 的必要测试。

期望值来源：
  1. 测试中手写的袋计数 Counter（主要依据，注释给出手工推导）；
  2. 本文件内 *独立实现* 的三值逻辑参考解释器 ref_run()（用 dict 行、
     与被测核心完全不同的代码路径，不 import relational3vl 来产生期望值）；
  3. 往返一致性只作为补充断言（见 test_roundtrip_supplementary）。
"""

import copy
import math
import unittest
from collections import Counter

import relational3vl as r3


# ---------------------------------------------------------------------------
# 独立参考实现：禁止调用被测核心。行用 {全限定列: 值} 的 dict 表示。
# ---------------------------------------------------------------------------

def _ref_eval(expr, row):
    op = expr["op"]
    if op == "col":
        return row[(expr["source"], expr["name"])]
    if op == "lit":
        return expr["value"]
    if op in ("eq", "ne", "lt", "le", "gt", "ge"):
        a = _ref_eval(expr["left"], row)
        b = _ref_eval(expr["right"], row)
        if a is None or b is None:
            return None
        return {
            "eq": a == b, "ne": a != b, "lt": a < b, "le": a <= b,
            "gt": a > b, "ge": a >= b,
        }[op]
    if op == "and":
        x, y = _ref_eval(expr["left"], row), _ref_eval(expr["right"], row)
        return False if (x is False or y is False) else (
            None if (x is None or y is None) else True)
    if op == "or":
        x, y = _ref_eval(expr["left"], row), _ref_eval(expr["right"], row)
        return True if (x is True or y is True) else (
            None if (x is None or y is None) else False)
    if op == "not":
        x = _ref_eval(expr["expr"], row)
        return None if x is None else (not x)
    if op == "is_null":
        return _ref_eval(expr["expr"], row) is None
    if op == "coalesce":
        for a in expr["args"]:
            v = _ref_eval(a, row)
            if v is not None:
                return v
        return None
    raise AssertionError(f"参考实现不认识的算子: {op}")


def _ref_run(node, tabs):
    t = node["type"]
    if t == "scan":
        tbl = tabs[node["table"]]
        sch = tuple(tuple(c) for c in tbl["columns"])
        rows = [dict(zip(sch, r)) for r in tbl["rows"]]
        return sch, rows
    if t == "filter":
        sch, rows = _ref_run(node["input"], tabs)
        return sch, [r for r in rows if _ref_eval(node["expr"], r) is True]
    if t == "project":
        sch0, rows = _ref_run(node["input"], tabs)
        sch = tuple((c["source"], c["name"]) for c in node["columns"])
        out = []
        for r in rows:
            out.append({sch[i]: _ref_eval(c["expr"], r)
                        for i, c in enumerate(node["columns"])})
        return sch, out
    if t in ("inner_join", "left_join"):
        ls, lr = _ref_run(node["left"], tabs)
        rs, rr = _ref_run(node["right"], tabs)
        sch = ls + rs
        out = []
        for l in lr:
            matched = False
            for r in rr:
                combo = {**l, **r}
                if _ref_eval(node["on"], combo) is True:
                    matched = True
                    out.append(combo)
            if t == "left_join" and not matched:
                pad = dict(l)
                for c in rs:
                    pad[c] = None
                out.append(pad)
        return sch, out
    if t == "union_all":
        ls, lr = _ref_run(node["left"], tabs)
        rs, rr = _ref_run(node["right"], tabs)
        assert ls == rs
        return ls, lr + rr
    if t == "distinct":
        sch, rows = _ref_run(node["input"], tabs)
        seen, out = set(), []
        for r in rows:
            k = tuple(r[c] for c in sch)
            if k not in seen:
                seen.add(k)
                out.append(r)
        return sch, out
    raise AssertionError(t)


def ref_bag(plan, tables):
    sch, rows = _ref_run(plan, tables)
    return sch, Counter(tuple(r[c] for c in sch) for r in rows)


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------

def col(source, name):
    return {"op": "col", "source": source, "name": name}


def lit(v):
    return {"op": "lit", "value": v}


def binop(op, a, b):
    return {"op": op, "left": a, "right": b}


def bag(result):
    sch = tuple(tuple(c) for c in result["columns"])
    return sch, Counter(tuple(r) for r in result["rows"])


def table(columns, rows):
    return {"columns": [list(c) for c in columns],
            "rows": [list(r) for r in rows]}


# 确定性示例数据 ----------------------------------------------------------------

def emp_dep_tables():
    # 员工: (id, dept)  dept 含一个 NULL；左表含重复行
    emp = table(
        [("e", "id"), ("e", "dept")],
        [(1, 10), (2, None), (3, 20), (1, 10), (4, 99)],
    )
    dep = table(
        [("d", "id"), ("d", "name")],
        [(10, "A"), (20, "B"), (10, "A2")],
    )
    return {"emp": emp, "dep": dep}


class CoreTests(unittest.TestCase):
    # -- scan / 顺序 / 防御性复制 -------------------------------------------

    def test_scan_preserves_order_and_copies(self):
        tabs = {"t": table([("s", "x")], [(1,), (2,), (1,)])}
        plan = {"type": "scan", "table": "t"}
        before = copy.deepcopy(tabs)
        res = r3.execute(plan, tabs)
        self.assertEqual(res["rows"], [[1], [2], [1]])
        res["rows"][0][0] = 999
        self.assertEqual(tabs, before, "结果必须是全新副本，不能别名输入")

    # -- filter 的三值逻辑 ---------------------------------------------------

    def test_filter_keeps_only_true_false_and_unknown_dropped(self):
        # id = 2: id=1 -> FALSE, id=2 -> TRUE, id=None -> UNKNOWN；只留 2
        tabs = {"t": table([("s", "id")], [(1,), (2,), (None,)])}
        plan = {"type": "filter",
                "input": {"type": "scan", "table": "t"},
                "expr": binop("eq", col("s", "id"), lit(2))}
        res = r3.execute(plan, tabs)
        self.assertEqual(res["rows"], [[2]])

    def test_null_equals_null_is_unknown_not_true(self):
        # NULL = NULL -> UNKNOWN；即使同一列与自己比较也是 UNKNOWN，被 filter 丢弃
        tabs = {"t": table([("s", "id")], [(None,), (1,)])}
        plan_eq = {"type": "filter", "input": {"type": "scan", "table": "t"},
                   "expr": binop("eq", col("s", "id"), col("s", "id"))}
        self.assertEqual(r3.execute(plan_eq, tabs)["rows"], [[1]])
        # 关键：NULL <> NULL 同样是 UNKNOWN（不是 TRUE），NULL 行也被丢弃
        plan_ne = {"type": "filter", "input": {"type": "scan", "table": "t"},
                   "expr": binop("ne", col("s", "id"), col("s", "id"))}
        self.assertEqual(r3.execute(plan_ne, tabs)["rows"], [])

    def test_filter_dept_not_equal_drops_null_employee(self):
        # dept <> 10: dept=20 -> TRUE；dept=10 -> FALSE；dept=None -> UNKNOWN
        tabs = {"t": table([("e", "dept")], [(10,), (20,), (None,)])}
        plan = {"type": "filter", "input": {"type": "scan", "table": "t"},
                "expr": binop("ne", col("e", "dept"), lit(10))}
        self.assertEqual(r3.execute(plan, tabs)["rows"], [[20]])

    # -- ON 与连接后 WHERE 的差异（左连接）-----------------------------------

    def _left_join_on(self, extra_on):
        on = binop("eq", col("e", "dept"), col("d", "id"))
        if extra_on is not None:
            on = binop("and", on, extra_on)
        return {"type": "left_join",
                "left": {"type": "scan", "table": "emp"},
                "right": {"type": "scan", "table": "dep"},
                "on": on}

    def test_on_vs_where_difference_with_left_join(self):
        tabs = emp_dep_tables()

        # 形态 A: ... LEFT JOIN dep ON e.dept=d.id AND d.name='A'
        plan_on = self._left_join_on(binop("eq", col("d", "name"), lit("A")))
        res_on = r3.execute(plan_on, tabs)
        # 手工推导:
        #  (1,10) 匹配 (10,A) TRUE; 对 (10,A2) name 条件 FALSE -> 1 行
        #  (2,NULL) 主条件 UNKNOWN -> 无匹配补 NULL
        #  (3,20) 对 (20,B) name='A' 为 FALSE -> 无匹配补 NULL
        #  (1,10) 重复一次 -> 再来 1 行
        #  (4,99) 无 dept 行 -> 补 NULL
        expected_on = [
            [1, 10, 10, "A"],
            [2, None, None, None],
            [3, 20, None, None],
            [1, 10, 10, "A"],
            [4, 99, None, None],
        ]
        self.assertEqual(res_on["rows"], expected_on)

        # 形态 B: LEFT JOIN dep ON e.dept=d.id, 然后 WHERE d.name='A'
        joined = self._left_join_on(None)
        plan_where = {"type": "filter", "input": joined,
                      "expr": binop("eq", col("d", "name"), lit("A"))}
        res_where = r3.execute(plan_where, tabs)
        # 内连接式命中: (1,10,A),(3,20,B),(1,10,A2),(1,10,A),(3,20,B)
        # 补 NULL 行的 d.name 为 NULL, ='A' 是 UNKNOWN 一律被 WHERE 剔除；
        # B / A2 的 name 比较为 FALSE。最终只剩两条 (1,10,A)。
        self.assertEqual(res_where["rows"],
                         [[1, 10, 10, "A"], [1, 10, 10, "A"]])

        # 同样的两种写法，对 inner_join 而言行数集合相同（只在 left_join 分叉）
        inner_base = {"type": "inner_join",
                      "left": {"type": "scan", "table": "emp"},
                      "right": {"type": "scan", "table": "dep"},
                      "on": binop("eq", col("e", "dept"), col("d", "id"))}
        inner_then_where = {"type": "filter", "input": inner_base,
                            "expr": binop("eq", col("d", "name"), lit("A"))}
        inner_on = {"type": "inner_join",
                    "left": {"type": "scan", "table": "emp"},
                    "right": {"type": "scan", "table": "dep"},
                    "on": binop("and",
                                binop("eq", col("e", "dept"), col("d", "id")),
                                binop("eq", col("d", "name"), lit("A")))}
        self.assertEqual(
            r3.execute(inner_then_where, tabs)["rows"],
            r3.execute(inner_on, tabs)["rows"],
        )

    # -- 空右表 / 重复左行 ----------------------------------------------------

    def test_left_join_empty_right_pads_every_left_row_once(self):
        tabs = {
            "l": table([("l", "k")], [(1,), (None,), (1,)]),
            "r": table([("r", "k"), ("r", "v")], []),
        }
        plan = {"type": "left_join",
                "left": {"type": "scan", "table": "l"},
                "right": {"type": "scan", "table": "r"},
                "on": binop("eq", col("l", "k"), col("r", "k"))}
        res = r3.execute(plan, tabs)
        # 每个左行出现一次（重复按重复次数），右侧两列全补 NULL
        self.assertEqual(res["rows"],
                         [[1, None, None], [None, None, None], [1, None, None]])

    def test_inner_join_empty_right_yields_empty(self):
        tabs = {
            "l": table([("l", "k")], [(1,)]),
            "r": table([("r", "k")], []),
        }
        plan = {"type": "inner_join",
                "left": {"type": "scan", "table": "l"},
                "right": {"type": "scan", "table": "r"},
                "on": binop("eq", col("l", "k"), col("r", "k"))}
        self.assertEqual(r3.execute(plan, tabs)["rows"], [])

    def test_duplicate_left_and_right_rows_multiply_bag(self):
        # 左 (1) x2，右 (1,'a') x3, 且 1=1 TRUE -> 2*3=6 条
        tabs = {
            "l": table([("l", "k")], [(1,), (1,)]),
            "r": table([("r", "k"), ("r", "v")],
                       [(1, "a"), (1, "a"), (1, "a")]),
        }
        plan = {"type": "inner_join",
                "left": {"type": "scan", "table": "l"},
                "right": {"type": "scan", "table": "r"},
                "on": binop("eq", col("l", "k"), col("r", "k"))}
        res = r3.execute(plan, tabs)
        self.assertEqual(len(res["rows"]), 6)
        self.assertEqual(bag(res)[1], Counter({(1, 1, "a"): 6}))
        # 确定顺序: 左行优先；每个左行下右行按右表顺序
        self.assertEqual(res["rows"], [[1, 1, "a"]] * 6)

    # -- union_all / distinct / project -------------------------------------

    def test_union_all_bag_semantics_and_order(self):
        tabs = {
            "a": table([("s", "x")], [(1,), (2,)]),
            "b": table([("s", "x")], [(2,), (3,), (3,)]),
        }
        plan = {"type": "union_all",
                "left": {"type": "scan", "table": "a"},
                "right": {"type": "scan", "table": "b"}}
        res = r3.execute(plan, tabs)
        # 先左后右；袋计数 1:1, 2:2, 3:2
        self.assertEqual(res["rows"], [[1], [2], [2], [3], [3]])
        self.assertEqual(bag(res)[1], Counter({(1,): 1, (2,): 2, (3,): 2}))

    def test_union_all_schema_mismatch_is_error(self):
        tabs = {
            "a": table([("s", "x")], [(1,)]),
            "b": table([("t", "x")], [(2,)]),  # 来源ID不同
        }
        plan = {"type": "union_all",
                "left": {"type": "scan", "table": "a"},
                "right": {"type": "scan", "table": "b"}}
        with self.assertRaises(r3.SchemaError):
            r3.execute(plan, tabs)

    def test_distinct_keeps_first_occurrence_and_treats_nulls_equal(self):
        # SQL 标准去重语义: NULL 与 NULL 视为重复
        tabs = {"t": table([("s", "x"), ("s", "y")],
                           [(1, "a"), (None, "z"), (1, "a"),
                            (None, "z"), (2, "b")])}
        plan = {"type": "distinct", "input": {"type": "scan", "table": "t"}}
        res = r3.execute(plan, tabs)
        self.assertEqual(res["rows"], [[1, "a"], [None, "z"], [2, "b"]])

    def test_project_renames_and_reorders_with_source_ids(self):
        tabs = {"t": table([("s", "a"), ("s", "b")], [(1, 2), (3, 4)])}
        plan = {
            "type": "project",
            "input": {"type": "scan", "table": "t"},
            "columns": [
                {"source": "out", "name": "b2",
                 "expr": col("s", "b")},
                {"source": "out", "name": "a2", "expr": col("s", "a")},
            ],
        }
        res = r3.execute(plan, tabs)
        self.assertEqual(res["columns"], [["out", "b2"], ["out", "a2"]])
        self.assertEqual(res["rows"], [[2, 1], [4, 3]])

    def test_project_duplicate_output_column_rejected(self):
        tabs = {"t": table([("s", "a")], [(1,)])}
        plan = {"type": "project", "input": {"type": "scan", "table": "t"},
                "columns": [
                    {"source": "o", "name": "x", "expr": col("s", "a")},
                    {"source": "o", "name": "x", "expr": col("s", "a")},
                ]}
        with self.assertRaises(r3.SchemaError):
            r3.execute(plan, tabs)

    def test_project_boolean_rules(self):
        tabs = {"t": table([("s", "a")], [(1,), (2,), (None,)])}
        # 比较 / is_null 在投影中“计算出”的布尔允许输出（SELECT a IS NULL）
        plan = {"type": "project", "input": {"type": "scan", "table": "t"},
                "columns": [
                    {"source": "o", "name": "is_one",
                     "expr": binop("eq", col("s", "a"), lit(1))},
                    {"source": "o", "name": "missing",
                     "expr": {"op": "is_null", "expr": col("s", "a")}},
                ]}
        res = r3.execute(plan, tabs)
        self.assertEqual(res["rows"], [[True, False], [False, False], [None, True]])
        # NULL 比较 -> UNKNOWN 行的比较列是 None，而不是 False
        # 但 lit 字面量中的 bool 永远拒绝（整数位禁止 bool）
        bad = {"type": "project", "input": {"type": "scan", "table": "t"},
               "columns": [
                   {"source": "o", "name": "flag",
                    "expr": {"op": "lit", "value": True}},
               ]}
        with self.assertRaises(r3.DataError):
            r3.execute(bad, tabs)
        # 布尔用于 filter 谓词合法；布尔不能当比较运算元（无布尔数据列可比）
        plan_f = {"type": "filter", "input": {"type": "scan", "table": "t"},
                  "expr": binop("eq", col("s", "a"), lit(1))}
        self.assertEqual(r3.execute(plan_f, tabs)["rows"], [[1]])

    # -- coalesce / is_null / not / and/or 三值真值表 -------------------------

    def test_coalesce_and_is_null(self):
        tabs = {"t": table([("s", "a"), ("s", "b"), ("s", "c")],
                           [(None, None, 7), (None, 5, 7), (3, None, 7),
                            (None, None, None)])}
        plan = {"type": "project", "input": {"type": "scan", "table": "t"},
                "columns": [
                    {"source": "o", "name": "first",
                     "expr": {"op": "coalesce", "args": [
                         col("s", "a"), col("s", "b"), col("s", "c")]}},
                    {"source": "o", "name": "all_null",
                     "expr": {"op": "is_null", "expr": {"op": "coalesce",
                              "args": [col("s", "a"), col("s", "b")]}}},
                ]}
        res = r3.execute(plan, tabs)
        self.assertEqual(res["rows"],
                         [[7, True], [5, False], [3, False], [None, True]])

    def test_3vl_truth_tables(self):
        # 行携带两个“逻辑原料”值 x,y（数字或 NULL），逻辑式为
        # (x = 1) AND/OR (y = 1)，以及 NOT (x = 1)。
        vals = [1, 0, None]  # =1 分别产生 TRUE / FALSE / UNKNOWN
        tabs = {"t": table([("s", "x"), ("s", "y")],
                           [(x, y) for x in vals for y in vals])}

        def logic_plan(combine):
            px = binop("eq", col("s", "x"), lit(1))
            py = binop("eq", col("s", "y"), lit(1))
            return {"type": "filter", "input": {"type": "scan", "table": "t"},
                    "expr": combine(px, py)}

        def truth(v):
            return {1: True, 0: False, None: None}[v]

        for combiner, ref3vl in (
            (lambda a, b: binop("and", a, b),
             lambda a, b: False if a is False or b is False else
             (None if a is None or b is None else True)),
            (lambda a, b: binop("or", a, b),
             lambda a, b: True if a is True or b is True else
             (None if a is None or b is None else False)),
        ):
            res = r3.execute(logic_plan(combiner), tabs)
            kept = {(r[0], r[1]) for r in res["rows"]}
            expect = {(x, y) for x in vals for y in vals
                      if ref3vl(truth(x), truth(y)) is True}
            self.assertEqual(kept, expect)

        # NOT: TRUE<->FALSE, UNKNOWN->UNKNOWN
        plan_not = {"type": "filter", "input": {"type": "scan", "table": "t"},
                    "expr": {"op": "not",
                             "expr": binop("eq", col("s", "x"), lit(1))}}
        res = r3.execute(plan_not, tabs)
        self.assertEqual({r[0] for r in res["rows"]}, {0})

    def test_comparison_operators_and_strings(self):
        tabs = {"t": table([("s", "a"), ("s", "b")],
                           [(1, 2), (2, 2), (3, 2),
                            ("a", "a"), ("a", "b")])}

        def rows_for(opname):
            plan = {"type": "filter", "input": {"type": "scan", "table": "t"},
                    "expr": binop(opname, col("s", "a"), col("s", "b"))}
            return [tuple(r) for r in r3.execute(plan, tabs)["rows"]]

        self.assertEqual(rows_for("lt"), [(1, 2), ("a", "b")])
        self.assertEqual(rows_for("le"), [(1, 2), (2, 2), ("a", "a"), ("a", "b")])
        self.assertEqual(rows_for("gt"), [(3, 2)])
        self.assertEqual(rows_for("ge"), [(2, 2), (3, 2), ("a", "a")])
        self.assertEqual(rows_for("eq"), [(2, 2), ("a", "a")])
        self.assertEqual(rows_for("ne"),
                         [(1, 2), (3, 2), ("a", "b")])

    def test_cross_type_comparison_is_error_not_false(self):
        tabs = {"t": table([("s", "a"), ("s", "b")], [(1, "a")])}
        plan = {"type": "filter", "input": {"type": "scan", "table": "t"},
                "expr": binop("eq", col("s", "a"), col("s", "b"))}
        with self.assertRaises(r3.QueryTypeError):
            r3.execute(plan, tabs)

    # -- 数值/布尔输入校验 ----------------------------------------------------

    def test_nan_and_infinity_rejected_in_data_and_literal(self):
        for bad in (math.nan, math.inf, -math.inf):
            tabs = {"t": table([("s", "x")], [(bad,)])}
            with self.assertRaises(r3.DataError):
                r3.execute({"type": "scan", "table": "t"}, tabs)
        plan = {"type": "filter", "input": {"type": "scan", "table": "t"},
                "expr": binop("eq", col("s", "x"), lit(math.nan))}
        tabs = {"t": table([("s", "x")], [(1.0,)])}
        with self.assertRaises(r3.DataError):
            r3.execute(plan, tabs)

    def test_bool_rejected_even_though_bool_is_int(self):
        tabs = {"t": table([("s", "x")], [(True,)])}
        with self.assertRaises(r3.DataError):
            r3.execute({"type": "scan", "table": "t"}, tabs)
        plan = {"type": "filter", "input": {"type": "scan", "table": "t"},
                "expr": binop("eq", col("s", "x"), lit(False))}
        with self.assertRaises(r3.DataError):
            r3.execute(plan, tabs)

    def test_finite_floats_are_supported(self):
        tabs = {"t": table([("s", "x")], [(1.5,), (2.5,)])}
        plan = {"type": "filter", "input": {"type": "scan", "table": "t"},
                "expr": binop("lt", col("s", "x"), lit(2.0))}
        self.assertEqual(r3.execute(plan, tabs)["rows"], [[1.5]])

    # -- 非法输入与可定位错误 -------------------------------------------------

    def test_illegal_column_raises_locatable_schema_error(self):
        tabs = {"t": table([("s", "id")], [(1,)])}
        plan = {"type": "filter", "input": {"type": "scan", "table": "t"},
                "expr": binop("eq", col("s", "missing"), lit(1))}
        with self.assertRaises(r3.SchemaError) as cm:
            r3.execute(plan, tabs)
        msg = str(cm.exception)
        self.assertIn("s.missing", msg)
        self.assertIn("s.id", msg)  # 给出可用列，便于定位

    def test_unknown_node_and_extra_keys_rejected(self):
        tabs = {"t": table([("s", "x")], [(1,)])}
        with self.assertRaises(r3.PlanError):
            r3.execute({"type": "bogus", "table": "t"}, tabs)
        with self.assertRaises(r3.PlanError):
            r3.execute({"type": "scan", "table": "t", "extra": 1}, tabs)
        with self.assertRaises(r3.PlanError):
            r3.execute({"type": "filter",
                        "input": {"type": "scan", "table": "t"},
                        "expr": binop("eq", col("s", "x"), lit(1)),
                        "junk": 0}, tabs)

    def test_missing_table_row_width_and_duplicate_columns(self):
        plan = {"type": "scan", "table": "nope"}
        with self.assertRaises(r3.DataError):
            r3.execute(plan, {})
        tabs = {"t": table([("s", "a"), ("s", "b")], [(1,)])}  # 行宽不符
        with self.assertRaises(r3.DataError):
            r3.execute({"type": "scan", "table": "t"}, tabs)
        tabs = {"t": {"columns": [["s", "a"], ["s", "a"]], "rows": []}}
        with self.assertRaises(r3.SchemaError):
            r3.execute({"type": "scan", "table": "t"}, tabs)

    def test_inputs_unchanged_after_failure(self):
        tabs = emp_dep_tables()
        before = copy.deepcopy(tabs)
        bad_plan = {"type": "filter",
                    "input": {"type": "left_join",
                              "left": {"type": "scan", "table": "emp"},
                              "right": {"type": "scan", "table": "dep"},
                              "on": binop("eq", col("e", "dept"),
                                          col("d", "nope"))},
                    "expr": lit(True)}
        with self.assertRaises(r3.QueryError):
            r3.execute(bad_plan, tabs)
        self.assertEqual(tabs, before)

    # -- 接口边界 -------------------------------------------------------------

    def test_table_row_limit_boundary(self):
        ok = {"t": table([("s", "x")], [(i,) for i in range(200)])}
        self.assertEqual(len(r3.execute({"type": "scan", "table": "t"}, ok)["rows"]), 200)
        too_many = {"t": table([("s", "x")], [(i,) for i in range(201)])}
        with self.assertRaises(r3.QueryLimitError):
            r3.execute({"type": "scan", "table": "t"}, too_many)

    def test_plan_node_limit_boundary(self):
        # 10 个节点: 1 scan + 9 filter，合法
        plan10 = {"type": "scan", "table": "t"}
        for _ in range(9):
            plan10 = {"type": "filter", "input": plan10,
                      "expr": binop("eq", col("s", "x"), col("s", "x"))}
        tabs = {"t": table([("s", "x")], [(1,), (None,)])}
        self.assertEqual(len(r3.execute(plan10, tabs)["rows"]), 1)
        # 第 11 个节点 -> 拒绝（预算耗尽类错误与语义无解分开）
        plan11 = {"type": "filter", "input": plan10,
                  "expr": binop("eq", col("s", "x"), col("s", "x"))}
        with self.assertRaises(r3.QueryLimitError):
            r3.execute(plan11, tabs)

    def test_limit_error_distinct_from_empty_result(self):
        # 空结果是合法语义（“无解”），不是资源错误
        tabs = {"t": table([("s", "x")], [])}
        res = r3.execute({"type": "scan", "table": "t"}, tabs)
        self.assertEqual(res["rows"], [])

    # -- 与独立参考的袋计数对照（多个固定计划）--------------------------------

    def test_bag_counts_match_independent_reference(self):
        tabs = emp_dep_tables()
        on_eq = binop("eq", col("e", "dept"), col("d", "id"))
        plans = {
            "scan": {"type": "scan", "table": "emp"},
            "filter_unknown": {"type": "filter",
                               "input": {"type": "scan", "table": "emp"},
                               "expr": binop("ne", col("e", "dept"), lit(99))},
            "inner": {"type": "inner_join",
                      "left": {"type": "scan", "table": "emp"},
                      "right": {"type": "scan", "table": "dep"}, "on": on_eq},
            "left": {"type": "left_join",
                     "left": {"type": "scan", "table": "emp"},
                     "right": {"type": "scan", "table": "dep"}, "on": on_eq},
            "left+where": {"type": "filter",
                           "input": {"type": "left_join",
                                     "left": {"type": "scan", "table": "emp"},
                                     "right": {"type": "scan", "table": "dep"},
                                     "on": on_eq},
                           "expr": {"op": "is_null",
                                    "expr": col("d", "name")}},
            "distinct": {"type": "distinct",
                         "input": {"type": "scan", "table": "emp"}},
            "union": {"type": "union_all",
                      "left": {"type": "distinct",
                               "input": {"type": "scan", "table": "emp"}},
                      "right": {"type": "scan", "table": "emp"}},
            "project+coalesce": {
                "type": "project",
                "input": {"type": "left_join",
                          "left": {"type": "scan", "table": "emp"},
                          "right": {"type": "scan", "table": "dep"}, "on": on_eq},
                "columns": [
                    {"source": "q", "name": "eid", "expr": col("e", "id")},
                    {"source": "q", "name": "dname",
                     "expr": {"op": "coalesce", "args": [
                         col("d", "name"), lit("?")]}},
                ]},
        }
        for label, plan in plans.items():
            with self.subTest(plan=label):
                sch, got = bag(r3.execute(plan, tabs))
                ref_sch, want = ref_bag(plan, tabs)
                self.assertEqual(sch, ref_sch)
                self.assertEqual(got, want, f"袋计数不一致: {label}")
                self.assertEqual(sum(got.values()), sum(want.values()))

    def test_hand_checked_counts_on_join_chain(self):
        # 再叠加一张奖金表，手工核对一个 join->filter->project->distinct 链
        tabs = emp_dep_tables()
        tabs["bonus"] = table([("b", "eid"), ("b", "amt")],
                              [(1, 5), (1, 5), (3, None)])
        plan = {
            "type": "distinct",
            "input": {
                "type": "project",
                "input": {
                    "type": "filter",
                    "input": {
                        "type": "inner_join",
                        "left": {"type": "scan", "table": "emp"},
                        "right": {"type": "scan", "table": "bonus"},
                        "on": binop("eq", col("e", "id"), col("b", "eid"))},
                    # amt > 4: (1,5)x2行 TRUE; (3,None) UNKNOWN 剔除
                    "expr": binop("gt", col("b", "amt"), lit(4))},
                "columns": [
                    {"source": "z", "name": "eid", "expr": col("e", "id")},
                    {"source": "z", "name": "amt", "expr": col("b", "amt")}]}}
        res = r3.execute(plan, tabs)
        # (1,5) 出现两次（左表 (1,10) 也重复），去重后只剩一条
        self.assertEqual(res["columns"], [["z", "eid"], ["z", "amt"]])
        self.assertEqual(res["rows"], [[1, 5]])

    # -- 补充：往返一致性（不能作为正确性主依据）------------------------------

    def test_roundtrip_supplementary(self):
        tabs = emp_dep_tables()
        base = {"type": "scan", "table": "emp"}
        # project 不改值、只换来源ID，袋计数应与原表一致（同构）
        proj = {"type": "project", "input": base,
                "columns": [
                    {"source": "p", "name": "id", "expr": col("e", "id")},
                    {"source": "p", "name": "dept", "expr": col("e", "dept")}]}
        res = r3.execute(proj, tabs)
        self.assertEqual(Counter(tuple(r) for r in res["rows"]),
                         Counter(tuple(r) for r in tabs["emp"]["rows"]))
        # distinct(union_all(x,x)) 的袋 = 2 * distinct(x) 的展开袋
        u = {"type": "union_all", "left": base, "right": base}
        d = {"type": "distinct", "input": u}
        res_d = r3.execute(d, tabs)
        raw = Counter(tuple(r) for r in tabs["emp"]["rows"])
        self.assertEqual(Counter(tuple(r) for r in res_d["rows"]),
                         Counter(raw.keys()))


if __name__ == "__main__":
    unittest.main()
