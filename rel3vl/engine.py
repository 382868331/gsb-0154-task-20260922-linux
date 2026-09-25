"""关系查询计划的编译与袋语义执行。

计划是一个节点 dict 树，判别字段为 ``op``。每个节点有可选 ``id``
（任意字符串，用于错误定位）。结构：

* ``{"op": "scan", "table": "t", "as": "s"}``
* ``{"op": "filter", "input": N, "expr": E}``
* ``{"op": "project", "input": N, "columns": [{"name", "expr", "table"?}, ...]}``
* ``{"op": "inner_join", "left": N, "right": N, "on": E}``
* ``{"op": "left_join", "left": N, "right": N, "on": E}``
* ``{"op": "union_all", "left": N, "right": N}``
* ``{"op": "distinct", "input": N}``
* ``{"op": "exists", "input": N, "subplan": M, "correlate": [B, ...]}``
* ``{"op": "not_exists", ...}``（同上，语义取反）

``exists`` / ``not_exists`` 是带显式相关列绑定的过滤节点：对外层每一行，
把 ``correlate`` 声明的外层列值绑定进子计划后执行之；子计划**产生至少一行**
（只看有没有行，不看行中字段是否为 NULL）则存在性为真。
``exists`` 保留存在性为真的外层行，``not_exists`` 保留为假的外层行；
外层行的重数与顺序原样保留，内层匹配再多也不会复制外层行。

绑定形态::

    {"from": {"table": "e", "column": "dept"}, "as": "o", "column": "dept"}

含义：把当前外层行中 ``e.dept`` 的值，以限定名 ``o.dept`` 暴露给子计划。
子计划只能引用自己的列与**已声明**的绑定列；引用未绑定的外层来源 id
会在编译期抛 :class:`SchemaError`（先验证列作用域，不存在即错误，
绝不当作 NULL）。绑定目标 ``(as, column)`` 不得重复，也不得与子计划
表达式处的本地列同名（不允许遮蔽）。相关嵌套最多
``MAX_CORRELATION_DEPTH`` 层；子计划节点计入 ``MAX_NODES`` 总上限。

执行模型：

* 关系 = 带重复行的元组袋，用 ``list[tuple]`` 表示，顺序确定；
* 全部节点先编译（表结构校验、模式推导、表达式编译、上限检查），
  编译期任何错误都在处理任何行之前抛出，因此不存在部分变更；
* 列以 ``(source_id, name)`` 限定：scan 的来源 id 默认为表名，
  自连接时用 ``"as"`` 指定不同来源 id；
* 相关子计划在编译期把绑定列追加为"环境作用域"，求值时扩展行 =
  本地行 + 绑定值，表达式列下标在编译期一次性确定。
"""

from __future__ import annotations

from .errors import PlanError, SchemaError, TableError
from .expressions import compile_expr
from .values import distinct_key, validate_scalar

MAX_ROWS_PER_TABLE = 200
MAX_NODES = 10
MAX_CORRELATION_DEPTH = 2

_CHILD_KEYS = {
    "scan": (),
    "filter": ("input",),
    "project": ("input",),
    "inner_join": ("left", "right"),
    "left_join": ("left", "right"),
    "union_all": ("left", "right"),
    "distinct": ("input",),
    "exists": ("input", "subplan"),
    "not_exists": ("input", "subplan"),
}
_REQUIRED_KEYS = {
    "scan": ("table",),
    "filter": ("input", "expr"),
    "project": ("input", "columns"),
    "inner_join": ("left", "right", "on"),
    "left_join": ("left", "right", "on"),
    "union_all": ("left", "right"),
    "distinct": ("input",),
    "exists": ("input", "subplan", "correlate"),
    "not_exists": ("input", "subplan", "correlate"),
}
_ALLOWED_NODE_KEYS = {
    "scan": {"op", "id", "table", "as"},
    "filter": {"op", "id", "input", "expr"},
    "project": {"op", "id", "input", "columns"},
    "inner_join": {"op", "id", "left", "right", "on"},
    "left_join": {"op", "id", "left", "right", "on"},
    "union_all": {"op", "id", "left", "right"},
    "distinct": {"op", "id", "input"},
    "exists": {"op", "id", "input", "subplan", "correlate"},
    "not_exists": {"op", "id", "input", "subplan", "correlate"},
}


# ---------------------------------------------------------------- 输入表校验

def validate_tables(tables):
    """校验 ``tables`` 映射并返回规范化副本 ``{name: (columns, rows)}``。

    表：``{"columns": ["a", ...], "rows": [[...], ...]}``。
    行宽必须一致；列名是非空 str 且表内唯一；行数 ≤ 200；
    每个标量经 :func:`rel3vl.values.validate_scalar` 校验。
    """
    if not isinstance(tables, dict):
        raise TableError(f"[tables]: 必须是 dict（表名 -> 表），得到 {type(tables).__name__}")
    norm = {}
    for tname, table in tables.items():
        if not isinstance(tname, str) or not tname:
            raise TableError("[tables]: 表名必须是非空字符串")
        prefix = f"[table:{tname}]"
        if not isinstance(table, dict):
            raise TableError(f"{prefix}: 表必须是 dict，得到 {type(table).__name__}")
        if set(table) - {"columns", "rows"}:
            raise TableError(f"{prefix}: 表只允许字段 columns/rows，得到 {sorted(table)}")
        if "columns" not in table or "rows" not in table:
            raise TableError(f"{prefix}: 表缺少 columns 或 rows")
        columns = table["columns"]
        rows = table["rows"]
        if not isinstance(columns, list) or not columns:
            raise TableError(f"{prefix}: columns 必须是非空 list")
        seen = set()
        for c in columns:
            if not isinstance(c, str) or not c:
                raise TableError(f"{prefix}: 列名必须是非空字符串，得到 {c!r}")
            if c in seen:
                raise TableError(f"{prefix}: 列名重复：{c!r}")
            seen.add(c)
        if not isinstance(rows, list):
            raise TableError(f"{prefix}: rows 必须是 list")
        if len(rows) > MAX_ROWS_PER_TABLE:
            raise TableError(
                f"{prefix}: 行数 {len(rows)} 超过上限 {MAX_ROWS_PER_TABLE}"
            )
        norm_rows = []
        for ri, row in enumerate(rows):
            if not isinstance(row, (list, tuple)):
                raise TableError(f"{prefix} row {ri}: 行必须是 list/tuple")
            if len(row) != len(columns):
                raise TableError(
                    f"{prefix} row {ri}: 行宽 {len(row)} 与列数 {len(columns)} 不一致"
                )
            norm_rows.append(
                tuple(
                    validate_scalar(v, f"{prefix} row {ri} column {c!r}")
                    for c, v in zip(columns, row)
                )
            )
        norm[tname] = (tuple(columns), norm_rows)
    return norm


# ---------------------------------------------------------------- 计划编译

class _Compiled:
    __slots__ = ("label", "schema", "run")

    def __init__(self, label, schema, run):
        # schema: tuple[(source_id, column_name)]（仅本地列，不含环境绑定）；
        # run: (env: tuple = ()) -> list[tuple]
        # env 是相关绑定值序列，与编译时的 env_scope 一一对应；
        # 顶层计划 env 为空，直接 run() 即可。
        self.label = label
        self.schema = schema
        self.run = run


def _col_index(schema):
    return {(s, c): i for i, (s, c) in enumerate(schema)}


def _scope_index(local_schema, env_scope, where):
    """表达式可见作用域：本地列在前、环境绑定列在后，下标与运行时
    ``row + env`` 的扩展行布局一致。绑定列与本地列同名即拒绝（不许遮蔽）。"""
    idx = _col_index(local_schema)
    base = len(local_schema)
    for j, (s, c) in enumerate(env_scope):
        if (s, c) in idx:
            raise SchemaError(
                f"{where}: 相关绑定列 {s}.{c} 与本地列同名冲突；"
                f"请给绑定改用其它 'as'/'column'"
            )
        idx[(s, c)] = base + j
    return idx


def compile_plan(plan, tables):
    """编译整棵计划树，返回根 :class:`_Compiled`。编译失败不执行任何节点。"""
    if not isinstance(plan, dict):
        raise PlanError("[plan]: 根节点必须是 dict")

    # 第一遍：结构遍历，编号、计数、环检测、子节点收集。
    order = []          # (node, depth-first 序号)
    seen_objs = {}      # id(node) -> 序号

    def walk(node):
        if not isinstance(node, dict):
            raise PlanError("[plan]: 节点必须是 dict")
        if id(node) in seen_objs:
            raise PlanError(
                f"[plan]: 节点对象被重复引用（不允许共享子树/环），"
                f"首次出现于 node#{seen_objs[id(node)]}"
            )
        num = len(order)
        seen_objs[id(node)] = num
        op = node.get("op")
        if not isinstance(op, str):
            raise PlanError(f"[node#{num}]: 缺少字符串字段 'op'")
        if op not in _CHILD_KEYS:
            raise PlanError(
                f"[node#{num}]: 未知 op={op!r}（允许 {sorted(_CHILD_KEYS)}）"
            )
        allowed = _ALLOWED_NODE_KEYS[op]
        extra = set(node) - allowed
        if extra:
            raise PlanError(f"[node#{num}]({op}): 未知字段 {sorted(extra)}")
        if "id" in node and not isinstance(node["id"], str):
            raise PlanError(f"[node#{num}]({op}): 可选字段 'id' 必须是字符串")
        for key in _REQUIRED_KEYS[op]:
            if key not in node:
                raise PlanError(f"[node#{num}]({op}): 缺少必填字段 {key!r}")
        for key in _CHILD_KEYS[op]:
            if not isinstance(node[key], dict):
                raise PlanError(
                    f"[node#{num}]({op}): {key!r} 必须是计划节点 dict"
                )
        order.append(node)
        for key in _CHILD_KEYS[op]:
            walk(node[key])
        return num

    root_num = walk(plan)
    if len(order) > MAX_NODES:
        raise PlanError(f"[plan]: 节点数 {len(order)} 超过上限 {MAX_NODES}")

    compiled_by_num = {}

    def build(node, env_scope, corr_depth):
        # env_scope: tuple[(source_id, column_name)]，本子树可见的相关绑定列；
        # corr_depth: 当前已处于多少层 exists/not_exists 子计划之内。
        num = seen_objs[id(node)]
        if num in compiled_by_num:
            return num
        op = node["op"]

        if op in ("exists", "not_exists"):
            if corr_depth >= MAX_CORRELATION_DEPTH:
                raise PlanError(
                    f"[node#{num}]({op}): 相关嵌套超过 "
                    f"{MAX_CORRELATION_DEPTH} 层上限"
                )
            child = compiled_by_num[build(node["input"], env_scope, corr_depth)]
            bindings = _validate_correlate(node, num, child.schema, env_scope)
            # 子计划只能看到声明的绑定列，不能越过该作用域取外层其它列。
            sub_env = tuple((b["as"], b["column"]) for b in bindings)
            sub = compiled_by_num[build(node["subplan"], sub_env, corr_depth + 1)]
            comp = _compile_exists(
                node, num, child, sub, bindings, env_scope, op == "not_exists"
            )
            compiled_by_num[num] = comp
            return num

        child_nums = []
        for key in _CHILD_KEYS[op]:
            child_nums.append(build(node[key], env_scope, corr_depth))
        children = [compiled_by_num[n] for n in child_nums]

        if op == "scan":
            comp = _compile_scan(node, num, tables)
        elif op == "filter":
            comp = _compile_filter(node, num, children[0], env_scope)
        elif op == "project":
            comp = _compile_project(node, num, children[0], env_scope)
        elif op in ("inner_join", "left_join"):
            comp = _compile_join(
                node, num, children[0], children[1], op == "left_join", env_scope
            )
        elif op == "union_all":
            comp = _compile_union(node, num, children[0], children[1])
        else:  # distinct
            comp = _compile_distinct(node, num, children[0])
        compiled_by_num[num] = comp
        return num

    build(plan, (), 0)
    return compiled_by_num[root_num]


def _compile_scan(node, num, tables):
    tname = node.get("table")
    if not isinstance(tname, str) or not tname:
        raise PlanError(f"[node#{num}](scan): 'table' 必须是非空字符串")
    if tname not in tables:
        raise PlanError(
            f"[node#{num}](scan): 表 {tname!r} 不在 tables 中（可用 {sorted(tables)}）"
        )
    source = node.get("as", tname)
    if not isinstance(source, str) or not source:
        raise PlanError(f"[node#{num}](scan): 'as' 必须是非空字符串")
    columns, rows = tables[tname]
    schema = tuple((source, c) for c in columns)
    snapshot = list(rows)  # 编译期快照：执行结果不受 tables 对象事后修改影响

    def run(env=()):  # scan 无表达式，忽略环境绑定
        return [tuple(r) for r in snapshot]

    return _Compiled(f"[node#{num}](scan:{tname})", schema, run)


def _compile_filter(node, num, child, env_scope):
    where = f"[node#{num}/expr](filter)"
    pred = compile_expr(node["expr"], _scope_index(child.schema, env_scope, where), where)

    def run(env=()):
        out = []
        for row in child.run(env):
            v = pred(row + env)
            # 三值逻辑：仅保留 TRUE；FALSE 与 UNKNOWN(NULL) 都丢弃。
            if v is True:
                out.append(row)
        return out

    return _Compiled(child.label + "|filter", child.schema, run)


def _compile_project(node, num, child, env_scope):
    cols = node.get("columns")
    if not isinstance(cols, list) or not cols:
        raise PlanError(f"[node#{num}](project): 'columns' 必须是非空 list")
    where = f"[node#{num}/expr](project)"
    idx = _scope_index(child.schema, env_scope, where)
    compiled_cols = []
    out_schema = []
    seen = set()
    for i, spec in enumerate(cols):
        if not isinstance(spec, dict):
            raise PlanError(f"[node#{num}](project) column#{i}: 必须是 dict")
        if set(spec) - {"name", "expr", "table"}:
            raise PlanError(f"[node#{num}](project) column#{i}: 含未知字段 {sorted(spec)}")
        name = spec.get("name")
        if not isinstance(name, str) or not name:
            raise PlanError(f"[node#{num}](project) column#{i}: 'name' 必须是非空字符串")
        source = spec.get("table", "")
        if not isinstance(source, str):
            raise PlanError(f"[node#{num}](project) column#{i}: 'table' 必须是字符串")
        qname = (source, name)
        if qname in seen:
            raise SchemaError(
                f"[node#{num}](project): 输出列 {source + '.' if source else ''}{name} 重复"
            )
        seen.add(qname)
        if "expr" not in spec:
            raise PlanError(f"[node#{num}](project) column#{i}: 缺少 'expr'")
        f = compile_expr(spec["expr"], idx, f"{where} column {name!r}")
        compiled_cols.append(f)
        out_schema.append(qname)

    def run(env=()):
        out = []
        for row in child.run(env):
            out.append(tuple(f(row + env) for f in compiled_cols))
        return out

    return _Compiled(child.label + "|project", tuple(out_schema), run)


def _compile_join(node, num, left, right, outer, env_scope):
    where = f"[node#{num}/expr]({'left_join' if outer else 'inner_join'})"
    schema = tuple(left.schema) + tuple(right.schema)
    # source_id 必须能消除歧义：不同来源允许同名，但同 (source,name) 禁止。
    if set(left.schema) & set(right.schema):
        raise SchemaError(
            f"[node#{num}]: 左右来源限定列完全相同 {sorted(set(left.schema) & set(right.schema))}；"
            f"请给其中一个 scan 设置不同的 'as'"
        )
    pred = compile_expr(node["on"], _scope_index(schema, env_scope, where), where)
    n_right = len(right.schema)
    nulls = (None,) * n_right
    tag = "left_join" if outer else "inner_join"

    def run(env=()):
        lr = left.run(env)
        rr = right.run(env)
        out = []
        for lrow in lr:
            matched = False
            for rrow in rr:
                joined = lrow + rrow
                # 连接谓词与 filter 相同：仅 TRUE 匹配；FALSE/UNKNOWN 不匹配。
                if pred(joined + env) is True:
                    out.append(joined)
                    matched = True
            if outer and not matched:
                # 仅当左行对所有右行都无 TRUE 匹配时补一行全 NULL。
                out.append(lrow + nulls)
        return out

    return _Compiled(f"({left.label}) {tag} ({right.label})", schema, run)


def _compile_union(node, num, left, right):
    ln = [c for _, c in left.schema]
    rn = [c for _, c in right.schema]
    if ln != rn:
        raise SchemaError(
            f"[node#{num}](union_all): 两边列名/顺序不一致：{ln} vs {rn}"
        )
    # UNION ALL 的输出模式取左侧（标准 SQL 同名规则），列数相同。
    schema = tuple(left.schema)

    def run(env=()):
        # 左袋全部行后接右袋全部行
        return list(left.run(env)) + list(right.run(env))

    return _Compiled(f"({left.label}) UNION ALL ({right.label})", schema, run)


def _compile_distinct(node, num, child):
    schema = child.schema

    def run(env=()):
        kept = []
        seen = set()
        for row in child.run(env):
            # IS NOT DISTINCT FROM 语义：NULL 与 NULL 同组；哈希键 O(n) 判定，
            # 保留每个等价组的首次出现，顺序确定。
            key = distinct_key(row)
            if key not in seen:
                seen.add(key)
                kept.append(row)
        return kept

    return _Compiled(child.label + "|distinct", schema, run)


# ---------------------------------------------------------------- 相关存在性

def _validate_correlate(node, num, input_schema, env_scope):
    """校验 ``correlate`` 绑定列表，返回规范化绑定 dict 序列。

    先验证列作用域：``from`` 必须能解析到"输入模式 + 本层环境"中真实存在
    的列，不存在即 :class:`SchemaError`，绝不把不存在的列当 NULL。
    """
    where = f"[node#{num}]({node['op']})"
    corr = node["correlate"]
    if not isinstance(corr, list):
        raise PlanError(f"{where}: 'correlate' 必须是 list")
    visible = _scope_index(input_schema, env_scope, where)
    bindings = []
    seen_targets = set()
    for i, b in enumerate(corr):
        bw = f"{where}/correlate#{i}"
        if not isinstance(b, dict):
            raise PlanError(f"{bw}: 绑定必须是 dict，得到 {type(b).__name__}")
        extra = set(b) - {"from", "as", "column"}
        if extra:
            raise PlanError(f"{bw}: 含未知字段 {sorted(extra)}")
        for key in ("from", "as", "column"):
            if key not in b:
                raise PlanError(f"{bw}: 缺少必填字段 {key!r}")
        frm = b["from"]
        if not isinstance(frm, dict) or set(frm) != {"table", "column"}:
            raise PlanError(f"{bw}: 'from' 必须是含 table/column 的 dict")
        ft, fc = frm["table"], frm["column"]
        if not isinstance(ft, str) or not isinstance(fc, str) or not fc:
            raise PlanError(f"{bw}: from.table/from.column 必须是字符串且列名非空")
        if (ft, fc) not in visible:
            available = ", ".join(f"{s}.{c}" for s, c in visible) or "<无列>"
            raise SchemaError(
                f"{bw}: 绑定来源 {ft}.{fc} 不在可见作用域内（可用列：{available}）"
            )
        alias, name = b["as"], b["column"]
        if not isinstance(alias, str) or not alias:
            raise PlanError(f"{bw}: 'as' 必须是非空字符串")
        if not isinstance(name, str) or not name:
            raise PlanError(f"{bw}: 'column' 必须是非空字符串")
        if (alias, name) in seen_targets:
            raise SchemaError(f"{bw}: 绑定目标 {alias}.{name} 重复")
        seen_targets.add((alias, name))
        bindings.append({"from": {"table": ft, "column": fc}, "as": alias, "column": name})
    return bindings


def _compile_exists(node, num, child, sub, bindings, env_scope, negate):
    tag = "not_exists" if negate else "exists"
    where = f"[node#{num}]({tag})"
    idx = _scope_index(child.schema, env_scope, where)
    # 绑定来源列在扩展行（本地行 + 本层环境）中的下标，编译期一次确定。
    from_idx = tuple(idx[(b["from"]["table"], b["from"]["column"])] for b in bindings)

    def run(env=()):
        # memo 键只含绑定值经 distinct_key 规范化后的形式：
        #   * NULL 映射为唯一哨兵（NULL 与 NULL 同键，且不与任何真值混淆）；
        #   * bool/int/float/str 带类型标签，True 与 1 不混键，1 与 1.0 同键
        #     （与比较语义一致：数值按数学值相等，存在性结果必然相同）；
        #   * 来源 id 不进键 —— 绑定列表在编译期固定于本闭包，
        #     同一 memo 内不存在"同值不同来源"的二义。
        # memo 是 run 的局部变量：每次执行独立；若本次执行中途抛错，
        # 整个 memo 随调用帧丢弃，不会污染下一次执行。
        memo = {}
        out = []
        for row in child.run(env):
            ext = row + env
            bound = tuple(ext[i] for i in from_idx)
            key = distinct_key(bound)
            hit = memo.get(key)
            if hit is None:
                # 存在性只看子计划是否产生行，不看行中字段是否为 NULL。
                hit = bool(sub.run(bound))
                memo[key] = hit
            if hit is not negate:
                # 外层行按原重数与顺序保留；内层匹配数不影响输出重数。
                out.append(row)
        return out

    return _Compiled(child.label + f"|{tag}", child.schema, run)


# ---------------------------------------------------------------- 公开入口

def execute(plan, tables):
    """执行查询计划。

    :param plan: 计划节点 dict 树（见模块文档）。
    :param tables: ``{表名: {"columns": [...], "rows": [[...], ...]}}``。
    :returns: ``{"columns": [{"source", "name"}], "rows": [[...], ...]}``。
    :raises QueryError: 所有非法输入的基类，子类见 :mod:`rel3vl.errors`。
    """
    norm_tables = validate_tables(tables)
    root = compile_plan(plan, norm_tables)
    rows = root.run()
    return {
        "columns": [{"source": s, "name": c} for s, c in root.schema],
        "rows": [list(r) for r in rows],
    }
