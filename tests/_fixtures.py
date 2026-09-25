"""独立袋计数参考与共享夹具。

本文件中的 ``naive_expected`` 是*独立*编写的朴素参考实现：
行用 ``dict[(source, col) -> value]`` 表示、袋用 Counter 表示，
代码路径与 ``rel3vl.engine`` 完全不同，且**不导入被测核心**。
它只作为小样本上的补充交叉校验；验收基准以测试内手工袋计数为准。
"""

from __future__ import annotations


# ---------------------------------------------------------------- 固定数据

def employees_table():
    # 刻意覆盖：普通行、同组重复部门、NULL 分数、NULL 部门
    return {
        "columns": ["id", "dept", "score"],
        "rows": [
            [1, "A", 10],
            [2, "A", None],
            [3, "B", 20],
            [4, None, 30],
        ],
    }


def departments_table():
    # 刻意含一个 NULL 主键部门，用于验证 NULL = NULL 不匹配
    return {
        "columns": ["did", "dname"],
        "rows": [
            ["A", "Alpha"],
            ["B", "Beta"],
            [None, "Mystery"],
        ],
    }


def employees_with_dup_table():
    t = employees_table()
    t = {"columns": t["columns"], "rows": t["rows"] + [[1, "A", 10]]}
    return t


def empty_departments_table():
    return {"columns": ["did", "dname"], "rows": []}


def bool_table():
    return {
        "columns": ["k", "flag"],
        "rows": [[1, True], [2, False], [3, None]],
    }


# ---------------------------------------------------------------- 计划构造

def scan(table, alias=None):
    node = {"op": "scan", "table": table}
    if alias is not None:
        node["as"] = alias
    return node


def col(source, name):
    return {"kind": "col", "table": source, "column": name}


def lit(v):
    return {"kind": "lit", "value": v}


def cmp(op, l, r):
    return {"kind": "cmp", "op": op, "left": l, "right": r}


def band(l, r):
    return {"kind": "and", "left": l, "right": r}


def bor(l, r):
    return {"kind": "or", "left": l, "right": r}


def bnot(e):
    return {"kind": "not", "expr": e}


def is_null(e):
    return {"kind": "is_null", "expr": e}


def coalesce(*args):
    return {"kind": "coalesce", "args": list(args)}


def emp_dept_join(outer=False, on=None):
    return {
        "op": "left_join" if outer else "inner_join",
        "left": scan("employees", "e"),
        "right": scan("departments", "d"),
        "on": on or cmp("=", col("e", "dept"), col("d", "did")),
    }


# 让 scan 来源 id 与列引用一致（employees -> e, departments -> d）
def scan_emp():
    return scan("employees", "e")


def scan_dep():
    return scan("departments", "d")


def join_plan(outer=False, on=None):
    return {
        "op": "left_join" if outer else "inner_join",
        "left": scan_emp(),
        "right": scan_dep(),
        "on": on or cmp("=", col("e", "dept"), col("d", "did")),
    }


def bind(from_source, from_column, as_source, as_column=None):
    """一条相关绑定：外层 from_source.from_column -> 子计划内 as_source.as_column。"""
    return {
        "from": {"table": from_source, "column": from_column},
        "as": as_source,
        "column": as_column if as_column is not None else from_column,
    }


def exists_plan(input_node, subplan, correlate, negate=False):
    return {
        "op": "not_exists" if negate else "exists",
        "input": input_node,
        "subplan": subplan,
        "correlate": correlate,
    }


def tables_fixture():
    return {"employees": employees_table(), "departments": departments_table()}


# ---------------------------------------------------------------- 独立朴素参考
#
# 不导入 rel3vl。支持测试用到的计划/表达式子集；袋 = Counter；行 = dict。

from collections import Counter  # noqa: E402


def _naive_tables(tables):
    out = {}
    for name, t in tables.items():
        out[name] = (list(t["columns"]), [list(r) for r in t["rows"]])
    return out


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
        pick = min if k == "and" else max
        return pick((a, b), key=lambda x: rank[x])
    # cmp
    a = _nev(expr["left"], row)
    b = _nev(expr["right"], row)
    if a is None or b is None:
        return None
    op = expr["op"]
    if op == "=":
        return a == b
    if op == "<>":
        return a != b
    return {
        "<": a < b,
        "<=": a <= b,
        ">": a > b,
        ">=": a >= b,
    }[op]


def _nrun(node, tabs):
    op = node["op"]
    if op == "scan":
        cols, rows = tabs[node["table"]]
        src = node.get("as", node["table"])
        schema = [(src, c) for c in cols]
        bag = Counter()
        for r in rows:
            bag[tuple(r)] += 1
        return schema, bag
    if op == "filter":
        schema, bag = _nrun(node["input"], tabs)
        out = Counter()
        for tup, n in bag.items():
            row = dict(zip(schema, tup))
            if _nev(node["expr"], row) is True:
                out[tup] += n
        return schema, out
    if op == "project":
        schema, bag = _nrun(node["input"], tabs)
        nschema = [(c.get("table", ""), c["name"]) for c in node["columns"]]
        out = Counter()
        for tup, n in bag.items():
            row = dict(zip(schema, tup))
            out[tuple(_nev(c["expr"], row) for c in node["columns"])] += n
        return nschema, out
    if op in ("inner_join", "left_join"):
        ls, lb = _nrun(node["left"], tabs)
        rs, rb = _nrun(node["right"], tabs)
        schema = ls + rs
        out = Counter()
        ritems = list(rb.elements())
        for lt, ln in lb.items():
            for _ in range(ln):
                matched = 0
                for rt in ritems:
                    row = dict(zip(schema, tuple(lt) + tuple(rt)))
                    if _nev(node["on"], row) is True:
                        out[tuple(lt) + tuple(rt)] += 1
                        matched += 1
                if op == "left_join" and matched == 0:
                    out[tuple(lt) + (None,) * len(rs)] += 1
        return schema, out
    if op == "union_all":
        ls, lb = _nrun(node["left"], tabs)
        rs, rb = _nrun(node["right"], tabs)
        return ls, lb + rb
    if op == "distinct":
        schema, bag = _nrun(node["input"], tabs)
        out = Counter()
        seen = set()
        null_key = object()  # 唯一哨兵，避免与任何真实标量冲突

        def tag(v):
            if v is None:
                return null_key
            # 类型标签防止 1 / True / 1.0 混组；数值按数学值同组。
            if isinstance(v, bool):
                return ("bool", v)
            if isinstance(v, (int, float)):
                return ("num", v)
            return (type(v).__name__, v)

        for tup, _n in bag.items():
            key = tuple(tag(v) for v in tup)
            if key not in seen:  # IS NOT DISTINCT FROM：NULL 与 NULL 同组
                seen.add(key)
                out[tuple(tup)] = 1
        return schema, out
    raise AssertionError(f"naive ref: unsupported op {op}")


def naive_expected(plan, tables):
    """返回 (schema, Counter[plain tuple])，与核心实现无关。"""
    tabs = _naive_tables(tables)
    schema, bag = _nrun(plan, tabs)
    return schema, Counter({tuple(k): v for k, v in bag.items()})


# ---------------------------------------------------------------- 逐行朴素参考
#
# 与上面的袋参考不同：这里用 list 逐行推进、保序，专门交叉校验
# exists / not_exists 的"外层重数与顺序保留、内层不复制外层行"语义。
# 同样不导入被测核心；env 是 {(source, col): value} 的相关绑定字典。

def _row_dict(schema, tup, env):
    # 本地列优先（核心实现中绑定与本地同名已在编译期拒绝，此处不会冲突）
    row = dict(env)
    row.update(zip(schema, tup))
    return row


def _nrows(node, tabs, env):
    op = node["op"]
    if op == "scan":
        cols, rows = tabs[node["table"]]
        src = node.get("as", node["table"])
        return [(src, c) for c in cols], [tuple(r) for r in rows]
    if op == "filter":
        schema, rows = _nrows(node["input"], tabs, env)
        return schema, [
            r for r in rows if _nev(node["expr"], _row_dict(schema, r, env)) is True
        ]
    if op == "project":
        schema, rows = _nrows(node["input"], tabs, env)
        nschema = [(c.get("table", ""), c["name"]) for c in node["columns"]]
        out = []
        for r in rows:
            row = _row_dict(schema, r, env)
            out.append(tuple(_nev(c["expr"], row) for c in node["columns"]))
        return nschema, out
    if op in ("inner_join", "left_join"):
        ls, lrows = _nrows(node["left"], tabs, env)
        rs, rrows = _nrows(node["right"], tabs, env)
        schema = ls + rs
        out = []
        for lr in lrows:
            matched = False
            for rr in rrows:
                joined = lr + rr
                if _nev(node["on"], _row_dict(schema, joined, env)) is True:
                    out.append(joined)
                    matched = True
            if op == "left_join" and not matched:
                out.append(lr + (None,) * len(rs))
        return schema, out
    if op == "union_all":
        ls, lrows = _nrows(node["left"], tabs, env)
        _rs, rrows = _nrows(node["right"], tabs, env)
        return ls, lrows + rrows
    if op == "distinct":
        schema, rows = _nrows(node["input"], tabs, env)
        out, seen = [], set()
        null_key = object()

        def tag(v):
            if v is None:
                return null_key
            if isinstance(v, bool):
                return ("bool", v)
            if isinstance(v, (int, float)):
                return ("num", v)
            return (type(v).__name__, v)

        for r in rows:
            key = tuple(tag(v) for v in r)
            if key not in seen:
                seen.add(key)
                out.append(r)
        return schema, out
    if op in ("exists", "not_exists"):
        schema, rows = _nrows(node["input"], tabs, env)
        out = []
        for r in rows:  # 逐外行：重数与顺序天然保留
            base = _row_dict(schema, r, env)
            sub_env = {
                (b["as"], b["column"]): base[(b["from"]["table"], b["from"]["column"])]
                for b in node["correlate"]
            }
            _s, sub_rows = _nrows(node["subplan"], tabs, sub_env)
            hit = len(sub_rows) > 0  # 只看有没有行，不看行内容是否全 NULL
            if hit != (op == "not_exists"):
                out.append(r)  # 内层匹配再多也只保留这一份外层行
        return schema, out
    raise AssertionError(f"naive rows ref: unsupported op {op}")


def naive_rows(plan, tables, env=None):
    """逐行参考：返回 (schema, list[tuple])，保序、保重数，与核心实现无关。"""
    tabs = _naive_tables(tables)
    return _nrows(plan, tabs, dict(env or {}))
