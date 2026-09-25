"""关系查询计划的编译与袋语义执行（含相关 EXISTS / NOT EXISTS）。

计划是一个节点 dict 树，判别字段为 ``op``。每个节点有可选 ``id``
（任意字符串，用于错误定位）。结构：

* ``{"op": "scan", "table": "t", "as": "s"}``
* ``{"op": "filter", "input": N, "expr": E}``
* ``{"op": "project", "input": N, "columns": [{"name", "expr", "table"?}, ...]}``
* ``{"op": "inner_join", "left": N, "right": N, "on": E}``
* ``{"op": "left_join", "left": N, "right": N, "on": E}``
* ``{"op": "union_all", "left": N, "right": N}``
* ``{"op": "distinct", "input": N}``
* ``{"op": "exists" / "not_exists", "input": N, "subplan": N,
   "correlations": [绑定, ...]}``

相关存在性过滤节点::

    {
      "op": "exists",                # 或 "not_exists"
      "input": <外层计划>,
      "subplan": <子计划>,
      "correlations": [
        {"outer_table": "e", "outer_column": "id",
         "inner_alias": "ou", "inner_name": "emp_id"},
        ...
      ],
    }

语义约定：

* 对 ``input`` 的**每一行**，用该行的相关列值参数化执行一次 ``subplan``；
  ``exists`` 保留子计划**至少产生一行**的外层行，``not_exists`` 反之。
* 存在性只看子计划是否产生行：子计划产生全 NULL 行也算存在；
  子计划内部的比较仍走三值逻辑（UNKNOWN 比较不构成匹配条件）。
* 每个外层行在输出中至多出现一次（保留其重数与顺序），
  内层多个匹配不会复制外层行。
* 相关列必须在 ``correlations`` 中**显式声明**：子计划以
  ``(inner_alias, inner_name)`` 限定名引用外层值；未声明的外层列、
  越过声明作用域的列引用一律在编译期报 :class:`SchemaError`，
  绝不会被静默当作 NULL。
* 相关绑定最多嵌套两层（外层 -> 子计划 -> 孙计划，
  见 :data:`MAX_CORRELATION_DEPTH`）；孙计划的绑定既可以引用子计划
  自身的列，也可以显式透传外层相关参数。

执行模型：

* 关系 = 带重复行的元组袋，用 ``list[tuple]`` 表示，顺序确定；
* 全部节点先编译（表结构校验、模式推导、表达式编译、作用域与上限检查），
  编译期任何错误都在处理任何行之前抛出，因此不存在部分变更；
* 列以 ``(source_id, name)`` 限定：scan 的来源 id 默认为表名，
  自连接时用 ``"as"`` 指定不同来源 id；
* 运行时每个编译节点的 ``run(frame)`` 接收一个**不可变映射**形式的
  相关帧（外层参数名 -> 当前外层行取值）；根节点以空帧启动。
"""

from __future__ import annotations

from types import MappingProxyType

from .errors import PlanError, SchemaError, TableError
from .expressions import compile_expr
from .values import distinct_key, validate_scalar, value_key

MAX_ROWS_PER_TABLE = 200
MAX_NODES = 10
# 相关绑定的最大嵌套层数：exists/not_exists 的子计划最多再嵌一层（共两层）。
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
    "exists": ("input", "subplan", "correlations"),
    "not_exists": ("input", "subplan", "correlations"),
}
_ALLOWED_NODE_KEYS = {
    "scan": {"op", "id", "table", "as"},
    "filter": {"op", "id", "input", "expr"},
    "project": {"op", "id", "input", "columns"},
    "inner_join": {"op", "id", "left", "right", "on"},
    "left_join": {"op", "id", "left", "right", "on"},
    "union_all": {"op", "id", "left", "right"},
    "distinct": {"op", "id", "input"},
    "exists": {"op", "id", "input", "subplan", "correlations"},
    "not_exists": {"op", "id", "input", "subplan", "correlations"},
}
_CORR_KEYS = {"outer_table", "outer_column", "inner_alias", "inner_name"}


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
        # schema: tuple[(source_id, column_name)]；
        # run: (frame: 只读 {param_name: value}) -> list[tuple]
        self.label = label
        self.schema = schema
        self.run = run


def _col_index(schema):
    return {(s, c): i for i, (s, c) in enumerate(schema)}


def _empty_frame():
    # 根执行帧：空的只读映射（types.MappingProxyType 防止任何节点误写）。
    return {}


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
        corr = node.get("correlations")
        if corr is not None and not isinstance(corr, list):
            raise PlanError(f"[node#{num}]({op}): 'correlations' 必须是 list")
        order.append(node)
        for key in _CHILD_KEYS[op]:
            walk(node[key])
        return num

    root_num = walk(plan)
    if len(order) > MAX_NODES:
        raise PlanError(f"[plan]: 节点数 {len(order)} 超过上限 {MAX_NODES}")

    compiled_by_num = {}

    # frame:
    #   depth  —— 当前节点所处的相关子计划层数（0 = 最外层）；
    #   params —— 表达式编译器可见的外层参数说明
    #             {"cols": {(alias, name): param_name}, "names": {param_name}}。
    def build(node, frame):
        num = seen_objs[id(node)]
        key = (num, frame["depth"], tuple(sorted(frame["params"]["names"])))
        if key in compiled_by_num:
            return
        op = node["op"]

        if op == "scan":
            comp = _compile_scan(node, num, tables)
        elif op == "filter":
            child = _built(node["input"], frame)
            comp = _compile_filter(node, num, child, frame["params"])
        elif op == "project":
            child = _built(node["input"], frame)
            comp = _compile_project(node, num, child, frame["params"])
        elif op in ("inner_join", "left_join"):
            left = _built(node["left"], frame)
            right = _built(node["right"], frame)
            comp = _compile_join(node, num, left, right, op == "left_join",
                                 frame["params"])
        elif op == "union_all":
            left = _built(node["left"], frame)
            right = _built(node["right"], frame)
            comp = _compile_union(node, num, left, right)
        elif op == "distinct":
            child = _built(node["input"], frame)
            comp = _compile_distinct(node, num, child)
        else:  # exists / not_exists
            comp = _compile_exists(node, num, frame, _built, op == "not_exists")
        compiled_by_num[key] = comp

    def _built(child_node, frame):
        build(child_node, frame)
        num = seen_objs[id(child_node)]
        key = (num, frame["depth"], tuple(sorted(frame["params"]["names"])))
        return compiled_by_num[key]

    root_frame = {"depth": 0, "params": {"cols": {}, "names": set()}}
    build(plan, root_frame)
    root_key = (root_num, 0, ())
    return compiled_by_num[root_key]


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

    def run(frame):
        return [tuple(r) for r in snapshot]

    return _Compiled(f"[node#{num}](scan:{tname})", schema, run)


def _compile_filter(node, num, child, params):
    where = f"[node#{num}/expr](filter)"
    pred = compile_expr(node["expr"], _col_index(child.schema), where, params=params)

    def run(frame):
        out = []
        for row in child.run(frame):
            v = pred(row, frame)
            # 三值逻辑：仅保留 TRUE；FALSE 与 UNKNOWN(NULL) 都丢弃。
            if v is True:
                out.append(row)
        return out

    return _Compiled(child.label + "|filter", child.schema, run)


def _compile_project(node, num, child, params):
    cols = node.get("columns")
    if not isinstance(cols, list) or not cols:
        raise PlanError(f"[node#{num}](project): 'columns' 必须是非空 list")
    where = f"[node#{num}/expr](project)"
    idx = _col_index(child.schema)
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
        f = compile_expr(spec["expr"], idx, f"{where} column {name!r}", params=params)
        compiled_cols.append(f)
        out_schema.append(qname)

    def run(frame):
        out = []
        for row in child.run(frame):
            out.append(tuple(f(row, frame) for f in compiled_cols))
        return out

    return _Compiled(child.label + "|project", tuple(out_schema), run)


def _compile_join(node, num, left, right, outer, params):
    where = f"[node#{num}/expr]({'left_join' if outer else 'inner_join'})"
    schema = tuple(left.schema) + tuple(right.schema)
    # source_id 必须能消除歧义：不同来源允许同名，但同 (source,name) 禁止。
    if set(left.schema) & set(right.schema):
        raise SchemaError(
            f"[node#{num}]: 左右来源限定列完全相同 {sorted(set(left.schema) & set(right.schema))}；"
            f"请给其中一个 scan 设置不同的 'as'"
        )
    pred = compile_expr(node["on"], _col_index(schema), where, params=params)
    n_right = len(right.schema)
    nulls = (None,) * n_right
    tag = "left_join" if outer else "inner_join"

    def run(frame):
        lr = left.run(frame)
        rr = right.run(frame)
        out = []
        for lrow in lr:
            matched = False
            for rrow in rr:
                joined = lrow + rrow
                # 连接谓词与 filter 相同：仅 TRUE 匹配；FALSE/UNKNOWN 不匹配。
                if pred(joined, frame) is True:
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

    def run(frame):
        return list(left.run(frame)) + list(right.run(frame))  # 先左袋后右袋

    return _Compiled(f"({left.label}) UNION ALL ({right.label})", schema, run)


def _compile_distinct(node, num, child):
    schema = child.schema

    def run(frame):
        kept = []
        seen = set()
        for row in child.run(frame):
            # IS NOT DISTINCT FROM 语义：NULL 与 NULL 同组；哈希键 O(n) 判定，
            # 保留每个等价组的首次出现，顺序确定。
            key = distinct_key(row)
            if key not in seen:
                seen.add(key)
                kept.append(row)
        return kept

    return _Compiled(child.label + "|distinct", schema, run)


# -------------------------------------------------------- 相关 EXISTS/NOT EXISTS

def _compile_exists(node, num, frame, build_child, negate):
    """编译相关存在性过滤节点。

    编译期职责（全部在处理任何行之前完成）：

    1. 层数检查（最多 :data:`MAX_CORRELATION_DEPTH` 层相关嵌套）；
    2. 逐条校验 ``correlations`` 声明：字段齐全、字符串非空、
       内层限定名不重复；外层限定名必须能在**外层输入实际列**或
       **当前帧已声明的外层参数**中解析，否则 :class:`SchemaError`
       （不把缺失列当 NULL）；
    3. 以仅含本节点绑定的新帧编译子计划 —— 子计划看不到未声明的
       外层列；孙计划若需外层值，必须在自己的 correlations 中
       再次显式声明（作用域不透传列，只透传已声明的参数名供绑定引用）。
    """
    tag = "not_exists" if negate else "exists"
    where = f"[node#{num}]({tag})"
    if frame["depth"] >= MAX_CORRELATION_DEPTH:
        raise PlanError(
            f"{where}: 相关嵌套层数超过上限 {MAX_CORRELATION_DEPTH}"
            f"（当前第 {frame['depth'] + 1} 层子计划）"
        )

    outer = build_child(node["input"], frame)
    outer_idx = _col_index(outer.schema)
    parent_param_cols = frame["params"]["cols"]

    raw_corrs = node["correlations"]
    bindings = []       # (inner_qname, param_name, extractor(row, frame))
    inner_cols = {}     # {(inner_alias, inner_name): param_name}
    inner_names = set()
    for i, b in enumerate(raw_corrs):
        bp = f"{where} correlation#{i}"
        if not isinstance(b, dict):
            raise PlanError(f"{bp}: 绑定必须是 dict")
        if set(b) != _CORR_KEYS:
            extra = set(b) - _CORR_KEYS
            missing = _CORR_KEYS - set(b)
            raise PlanError(
                f"{bp}: 绑定字段必须恰好为 {sorted(_CORR_KEYS)}"
                + (f"；多余 {sorted(extra)}" if extra else "")
                + (f"；缺少 {sorted(missing)}" if missing else "")
            )
        ot, oc = b["outer_table"], b["outer_column"]
        ia, inm = b["inner_alias"], b["inner_name"]
        for fname, fv in (
            ("outer_table", ot), ("outer_column", oc),
            ("inner_alias", ia), ("inner_name", inm),
        ):
            if not isinstance(fv, str) or not fv:
                raise PlanError(f"{bp}: {fname!r} 必须是非空字符串，得到 {fv!r}")
        outer_q = (ot, oc)
        inner_q = (ia, inm)
        if inner_q in inner_cols:
            raise PlanError(f"{bp}: 内层限定名 {ia}.{inm} 被重复绑定")
        # 外层来源解析：外层输入的实际列与当前帧的上层参数不允许同名，
        # 否则绑定引用有歧义；两者都没有 -> 编译期 SchemaError
        # （绝不静默当 NULL）。
        in_cols = outer_q in outer_idx
        in_params = outer_q in parent_param_cols
        if in_cols and in_params:
            raise SchemaError(
                f"{bp}: 外层名 {ot}.{oc} 同时是外层输入的实际列与上层相关参数，"
                f"绑定引用有歧义；请调整 scan 的 'as' 或上层绑定的 inner_alias"
            )
        if in_cols:
            pos = outer_idx[outer_q]
            extractor = lambda row, parent_frame, pos=pos: row[pos]
        elif in_params:
            pname = parent_param_cols[outer_q]
            extractor = lambda row, parent_frame, pname=pname: parent_frame[pname]
        else:
            avail_cols = ", ".join(f"{s}.{c}" for s, c in outer.schema) or "<无列>"
            avail_par = ", ".join(f"{s}.{c}" for s, c in parent_param_cols) or "<无上层参数>"
            raise SchemaError(
                f"{bp}: 外层列 {ot}.{oc} 不存在；"
                f"外层输入可用列：{avail_cols}；当前帧已声明参数：{avail_par}"
            )
        pname = f"__corr_node{num}_b{i}__"
        inner_cols[inner_q] = pname
        inner_names.add(pname)
        bindings.append((inner_q, pname, extractor))

    sub_params = {"cols": inner_cols, "names": inner_names}
    sub_frame = {"depth": frame["depth"] + 1, "params": sub_params}
    sub = build_child(node["subplan"], sub_frame)
    label = f"{outer.label} {tag.upper()} ({sub.label})"

    def run(parent_frame):
        outer_rows = outer.run(parent_frame)
        # 相关键缓存仅活在本次 run 调用内：
        # * 键 = 逐个绑定的 (inner_alias, inner_name, 值键) 元组，
        #   来源限定名进入键，两个不同绑定即便值相同也不会互相误用；
        # * NULL 经 values.value_key 编码为模块级唯一哨兵，
        #   不与任何真实标量（含 0、""、"None"）撞键；
        #   bool 带类型标签，1 与 True 不混，1 与 1.0 同键；
        # * dict 是调用栈上的局部对象，执行结束（含异常向外传播）
        #   后即回收 —— 一次失败的执行不可能污染下一次 execute。
        cache = {}
        miss = object()  # 区分"未缓存"与缓存值 False
        out = []
        for row in outer_rows:
            key = tuple(
                (ia, inm, value_key(extract(row, parent_frame)))
                for (ia, inm), _pname, extract in bindings
            )
            cached = cache.get(key, miss)
            if cached is miss:
                sub_frame_values = MappingProxyType({
                    pname: extract(row, parent_frame)
                    for _q, pname, extract in bindings
                })
                produced = sub.run(sub_frame_values)
                # 存在性只看"有没有行"：行中字段是否全为 NULL 无关；
                # 内层多行也不会把外层行复制多份。
                cached = bool(produced)
                cache[key] = cached
            keep = (not cached) if negate else cached
            if keep:
                out.append(row)
        return out

    return _Compiled(label, outer.schema, run)


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
    rows = root.run(_empty_frame())
    return {
        "columns": [{"source": s, "name": c} for s, c in root.schema],
        "rows": [list(r) for r in rows],
    }
