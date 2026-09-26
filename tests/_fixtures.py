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
    if op == "grouping_sets":
        # 朴素参考：逐个分组集分别执行（dict 袋 + 首现顺序），再按序拼接，
        # 重复分组集天然保留。与核心代码路径完全独立。
        schema, bag = _nrun(node["input"], tabs)
        gcols = [(g["table"], g["column"]) for g in node["group_by"]]
        out = Counter()
        for gset in node["sets"]:
            gpos = [i for i, (_, c) in enumerate(gcols) if c in gset]
            groups = {}
            order = []
            rows_in = []
            for tup, mult in bag.items():
                rows_in.extend([tup] * mult)
            for tup in rows_in:
                tup = tuple(tup)
                key = tuple(tup[i] for i in gpos)  # None 自然同键
                if key not in groups:
                    groups[key] = []
                    order.append(key)
                groups[key].append(tup)
            if not gpos and not order:  # 空输入 + 空分组集 -> 一行空组
                order.append(())
                groups[()] = []
            bitmap = "".join("1" if c in gset else "0" for _, c in gcols)
            for key in order:
                members = groups[key]
                prefix = [key[gpos.index(i)] if c in gset else None
                          for i, (_, c) in enumerate(gcols)]
                avals = []
                for agg in node["aggregates"]:
                    if agg["func"] == "count":
                        if "expr" in agg:
                            avals.append(sum(1 for r in members if _nev(agg["expr"], dict(zip(schema, r))) is not None))
                        else:
                            avals.append(len(members))
                    else:  # sum：NULL 跳过，全 NULL/空组 -> NULL
                        nums = [v for v in (_nev(agg["expr"], dict(zip(schema, r))) for r in members) if v is not None]
                        avals.append(sum(nums) if nums else None)
                out[tuple(prefix + [bitmap] + avals)] += 1
        oschema = gcols + [("", "grouping")] + [("", a["name"]) for a in node["aggregates"]]
        return oschema, out
    raise AssertionError(f"naive ref: unsupported op {op}")


def naive_expected(plan, tables):
    """返回 (schema, Counter[plain tuple])，与核心实现无关。"""
    tabs = _naive_tables(tables)
    schema, bag = _nrun(plan, tabs)
    return schema, Counter({tuple(k): v for k, v in bag.items()})
