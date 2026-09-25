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


# ----------------------------------------------------- 相关 EXISTS 专用数据

def customers_table():
    # cid 刻意覆盖：有订单/无订单/重复行/NULL；顺序即下列顺序。
    return {
        "columns": ["cid", "cname"],
        "rows": [
            [1, "Ann"],
            [1, "Ann"],   # 外层重复行：重数必须保留
            [2, "Bob"],
            [3, "Cyd"],    # 无订单
            [4, None],     # 无订单，名字 NULL
            [None, "Nemo"],  # cid NULL：与订单 NULL cid 比较为 UNKNOWN
        ],
    }


def orders_table():
    return {
        "columns": ["oid", "cid"],
        "rows": [
            [10, 1],
            [11, 1],      # 同一客户两个匹配：不得复制外层行
            [12, 2],
            [13, None],   # 订单客户未知：NULL = NULL 为 UNKNOWN
        ],
    }


def empty_orders_table():
    return {"columns": ["oid", "cid"], "rows": []}


def teams_table():
    return {
        "columns": ["gid"],
        "rows": [
            [1],
            [1],   # 重复组
            [2],
            [3],   # 没有队员
            [None],  # gid NULL：与队员 NULL gid 比较为 UNKNOWN
        ],
    }


def players_table():
    # 与 teams 同名列 gid（两层相关嵌套中的同名列场景）
    return {
        "columns": ["pid", "gid"],
        "rows": [
            [10, 1],
            [11, 1],
            [12, 2],
            [13, None],
        ],
    }


def awards_table():
    return {
        "columns": ["aid", "pid"],
        "rows": [
            [100, 10],
            [101, 10],  # 同一队员两个奖
            [102, 12],
        ],
    }


def corr_tables():
    return {
        "customers": customers_table(),
        "orders": orders_table(),
        "teams": teams_table(),
        "players": players_table(),
        "awards": awards_table(),
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


# ------------------------------------------- 相关 EXISTS/NOT EXISTS 计划构造

def corr(outer_table, outer_column, inner_alias, inner_name):
    """一条显式相关绑定。"""
    return {
        "outer_table": outer_table,
        "outer_column": outer_column,
        "inner_alias": inner_alias,
        "inner_name": inner_name,
    }


def exists_node(input_node, subplan, correlations, negate=False):
    return {
        "op": "not_exists" if negate else "exists",
        "input": input_node,
        "subplan": subplan,
        "correlations": list(correlations),
    }


def customers_with_orders_plan(negate=False):
    """客户 WHERE EXISTS / NOT EXISTS (订单 WHERE 订单.cid = 外层客户.cid)。"""
    return exists_node(
        scan("customers", "c"),
        {
            "op": "filter",
            "input": scan("orders", "o"),
            "expr": cmp("=", col("o", "cid"), col("oc", "cid")),
        },
        [corr("c", "cid", "oc", "cid")],
        negate=negate,
    )


def team_player_award_exists_plan():
    """两层相关嵌套（两层同名列 gid）：

    teams WHERE EXISTS (
      players 过滤 players.gid = 外层 teams.gid（tp.gid）且 EXISTS (
        awards 过滤 awards.pid = players.pid（ip.pid）
      )
    )

    孙计划只绑定子计划自己的列（players.pid）；第一层过滤比较的是
    两个不同来源的同名列 ``gid``。
    """
    inner = exists_node(
        scan("players", "p"),
        {
            "op": "filter",
            "input": scan("awards", "a"),
            "expr": cmp("=", col("a", "pid"), col("ip", "pid")),
        },
        [corr("p", "pid", "ip", "pid")],
    )
    depth1_subplan = {
        "op": "filter",
        "input": inner,
        # 同名列 gid：子计划实际列 p.gid vs 外层相关参数 tp.gid
        "expr": cmp("=", col("p", "gid"), col("tp", "gid")),
    }
    return exists_node(
        scan("teams", "t"),
        depth1_subplan,
        [corr("t", "gid", "tp", "gid")],
    )


def team_player_award_exists_with_passthrough_plan():
    """两层相关嵌套，孙计划同时引用：

    * 子计划外层输入的实际列 players.pid（孙 correlations 绑定为 ip.pid）；
    * 最外层 teams.gid —— 它在第一层绑定中以 tp.gid 出现，
      孙计划必须沿链路**再次显式声明**（tp.gid -> og.gid）才能用。

    计划（缩进表示子计划）::

        teams(t) WHERE EXISTS*1* [tp.gid <- t.gid]
          players(p) 过滤: p.gid = tp.gid 且 EXISTS*2* [ip.pid <- p.pid,
                                                         og.gid <- tp.gid]
            awards(a) 过滤: a.pid = ip.pid 且 a.aid > og.gid

    作用域不透传列：孙计划若直接写 t.gid 或未在自己的 correlations
    中声明 tp.gid，编译期即报 SchemaError。
    """
    grand_filter = {
        "op": "filter",
        "input": scan("awards", "a"),
        "expr": band(
            cmp("=", col("a", "pid"), col("ip", "pid")),
            cmp(">", col("a", "aid"), col("og", "gid")),
        ),
    }
    inner_exists = exists_node(
        scan("players", "p"),
        grand_filter,
        [
            corr("p", "pid", "ip", "pid"),    # 子计划外层的实际列
            corr("tp", "gid", "og", "gid"),   # 透传最外层 team gid
        ],
    )
    depth1_subplan = {
        "op": "filter",
        "input": inner_exists,
        # 同一表达式里既有子计划自己的实际列 p.gid，也有外层相关参数 tp.gid
        "expr": cmp("=", col("p", "gid"), col("tp", "gid")),
    }
    return exists_node(
        scan("teams", "t"),
        depth1_subplan,
        [corr("t", "gid", "tp", "gid")],
    )


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


def _nrun(node, tabs, frame=None):
    """逐节点朴素解释。

    ``frame`` 是当前相关作用域可见的外层绑定：``{(alias, name): value}``。
    与核心不同，这里不做编译期作用域校验（只对有效计划交叉核对结果），
    但求值路径同样是"逐外层行执行子计划"的直观写法，且不 import 核心。
    """
    frame = dict(frame or {})

    def view(schema, tup):
        # 实际列优先于外层绑定；核心已拒绝两者同名且被引用的计划。
        merged = dict(frame)
        merged.update(dict(zip(schema, tup)))
        return merged

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
        schema, bag = _nrun(node["input"], tabs, frame)
        out = Counter()
        for tup, n in bag.items():
            if _nev(node["expr"], view(schema, tup)) is True:
                out[tup] += n
        return schema, out
    if op == "project":
        schema, bag = _nrun(node["input"], tabs, frame)
        nschema = [(c.get("table", ""), c["name"]) for c in node["columns"]]
        out = Counter()
        for tup, n in bag.items():
            out[tuple(_nev(c["expr"], view(schema, tup)) for c in node["columns"])] += n
        return nschema, out
    if op in ("inner_join", "left_join"):
        ls, lb = _nrun(node["left"], tabs, frame)
        rs, rb = _nrun(node["right"], tabs, frame)
        schema = ls + rs
        out = Counter()
        ritems = list(rb.elements())
        for lt, ln in lb.items():
            for _ in range(ln):
                matched = 0
                for rt in ritems:
                    row = view(schema, tuple(lt) + tuple(rt))
                    if _nev(node["on"], row) is True:
                        out[tuple(lt) + tuple(rt)] += 1
                        matched += 1
                if op == "left_join" and matched == 0:
                    out[tuple(lt) + (None,) * len(rs)] += 1
        return schema, out
    if op in ("exists", "not_exists"):
        # 直观逐外层行执行：对每个（按重数展开的）外层行，
        # 用显式声明的绑定构造子帧，执行一次子计划，只看是否产生行。
        schema, bag = _nrun(node["input"], tabs, frame)
        out = Counter()
        for tup, n in bag.items():
            outer_view = view(schema, tup)
            child_frame = {}
            for b in node["correlations"]:
                child_frame[(b["inner_alias"], b["inner_name"])] = outer_view[
                    (b["outer_table"], b["outer_column"])
                ]
            _ss, sb = _nrun(node["subplan"], tabs, child_frame)
            # 存在性只看行数（含全 NULL 行），与子计划行内容无关。
            has_rows = sum(sb.values()) > 0
            keep = has_rows if op == "exists" else (not has_rows)
            if keep:
                out[tuple(tup)] += n   # 无论内层几个匹配，外层行只保留一份
        return schema, out
    if op == "union_all":
        ls, lb = _nrun(node["left"], tabs, frame)
        rs, rb = _nrun(node["right"], tabs, frame)
        return ls, lb + rb
    if op == "distinct":
        schema, bag = _nrun(node["input"], tabs, frame)
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
