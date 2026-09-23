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

执行模型：

* 关系 = 带重复行的元组袋，用 ``list[tuple]`` 表示，顺序确定；
* 全部节点先编译（表结构校验、模式推导、表达式编译、上限检查），
  编译期任何错误都在处理任何行之前抛出，因此不存在部分变更；
* 列以 ``(source_id, name)`` 限定：scan 的来源 id 默认为表名，
  自连接时用 ``"as"`` 指定不同来源 id。
"""

from __future__ import annotations

from collections import Counter

from .aggregates import AGG_FUNCS, FUNC_NEEDS_EXPR, compute_aggs, validate_measure
from .errors import ExpressionError, PlanError, SchemaError, TableError
from .expressions import compile_expr
from .values import distinct_key, validate_scalar

MAX_ROWS_PER_TABLE = 200
MAX_NODES = 10

_CHILD_KEYS = {
    "scan": (),
    "filter": ("input",),
    "project": ("input",),
    "inner_join": ("left", "right"),
    "left_join": ("left", "right"),
    "union_all": ("left", "right"),
    "distinct": ("input",),
    "group_by": ("input",),
    "window": ("input",),
}
_REQUIRED_KEYS = {
    "scan": ("table",),
    "filter": ("input", "expr"),
    "project": ("input", "columns"),
    "inner_join": ("left", "right", "on"),
    "left_join": ("left", "right", "on"),
    "union_all": ("left", "right"),
    "distinct": ("input",),
    "group_by": ("input", "aggs"),
    "window": ("input", "time", "width", "aggs"),
}
_ALLOWED_NODE_KEYS = {
    "scan": {"op", "id", "table", "as"},
    "filter": {"op", "id", "input", "expr"},
    "project": {"op", "id", "input", "columns"},
    "inner_join": {"op", "id", "left", "right", "on"},
    "left_join": {"op", "id", "left", "right", "on"},
    "union_all": {"op", "id", "left", "right"},
    "distinct": {"op", "id", "input"},
    "group_by": {"op", "id", "input", "keys", "aggs"},
    "window": {"op", "id", "input", "time", "width", "keys", "aggs"},
}


# ---------------------------------------------------------------- 输入表校验

def validate_tables(tables, *, limit=MAX_ROWS_PER_TABLE):
    """校验 ``tables`` 映射并返回规范化副本 ``{name: (columns, rows)}``。

    表：``{"columns": ["a", ...], "rows": [[...], ...]}``。
    行宽必须一致；列名是非空 str 且表内唯一；行数 ≤ ``limit``
    （批量入口默认 200，增量入口传更大的上限）；
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
        if len(rows) > limit:
            raise TableError(
                f"{prefix}: 行数 {len(rows)} 超过上限 {limit}"
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
    __slots__ = ("label", "schema", "run", "num", "op", "children", "detail")

    def __init__(self, label, schema, run, *, num=-1, op="", children=(), detail=None):
        # schema: tuple[(source_id, column_name)]；run: () -> list[tuple]
        self.label = label
        self.schema = schema
        self.run = run
        # 以下元数据供增量解释器（rel3vl.stream）复用同一套编译产物：
        self.num = num            # 深度优先节点序号
        self.op = op              # 原始 op 名
        self.children = tuple(children)  # 子节点 _Compiled
        self.detail = detail or {}      # op 相关编译细节（谓词闭包等）


def _col_index(schema):
    return {(s, c): i for i, (s, c) in enumerate(schema)}


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

    def build(node):
        num = seen_objs[id(node)]
        if num in compiled_by_num:
            return compiled_by_num[num]
        op = node["op"]
        child_nums = []
        for key in _CHILD_KEYS[op]:
            child_nums.append(build(node[key]))
        children = [compiled_by_num[n] for n in child_nums]

        if op == "scan":
            comp = _compile_scan(node, num, tables)
        elif op == "filter":
            comp = _compile_filter(node, num, children[0])
        elif op == "project":
            comp = _compile_project(node, num, children[0])
        elif op in ("inner_join", "left_join"):
            comp = _compile_join(node, num, children[0], children[1], op == "left_join")
        elif op == "union_all":
            comp = _compile_union(node, num, children[0], children[1])
        elif op == "group_by":
            comp = _compile_group_by(node, num, children[0])
        elif op == "window":
            comp = _compile_window(node, num, children[0])
        else:  # distinct
            comp = _compile_distinct(node, num, children[0])
        comp.num = num
        comp.op = op
        comp.children = tuple(children)
        compiled_by_num[num] = comp
        return num

    build(plan)
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

    def run():
        return [tuple(r) for r in snapshot]

    return _Compiled(f"[node#{num}](scan:{tname})", schema, run,
                     detail={"table": tname, "source": source,
                             "columns": columns})


def _compile_filter(node, num, child):
    where = f"[node#{num}/expr](filter)"
    pred = compile_expr(node["expr"], _col_index(child.schema), where)

    def run():
        out = []
        for row in child.run():
            v = pred(row)
            # 三值逻辑：仅保留 TRUE；FALSE 与 UNKNOWN(NULL) 都丢弃。
            if v is True:
                out.append(row)
        return out

    return _Compiled(child.label + "|filter", child.schema, run,
                     detail={"pred": pred})


def _compile_project(node, num, child):
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
        f = compile_expr(spec["expr"], idx, f"{where} column {name!r}")
        compiled_cols.append(f)
        out_schema.append(qname)

    def run():
        out = []
        for row in child.run():
            out.append(tuple(f(row) for f in compiled_cols))
        return out

    return _Compiled(child.label + "|project", tuple(out_schema), run,
                     detail={"proj_funcs": compiled_cols})


def _compile_join(node, num, left, right, outer):
    where = f"[node#{num}/expr]({'left_join' if outer else 'inner_join'})"
    schema = tuple(left.schema) + tuple(right.schema)
    # source_id 必须能消除歧义：不同来源允许同名，但同 (source,name) 禁止。
    if set(left.schema) & set(right.schema):
        raise SchemaError(
            f"[node#{num}]: 左右来源限定列完全相同 {sorted(set(left.schema) & set(right.schema))}；"
            f"请给其中一个 scan 设置不同的 'as'"
        )
    pred = compile_expr(node["on"], _col_index(schema), where)
    n_right = len(right.schema)
    nulls = (None,) * n_right
    tag = "left_join" if outer else "inner_join"

    def run():
        lr = left.run()
        rr = right.run()
        out = []
        for lrow in lr:
            matched = False
            for rrow in rr:
                joined = lrow + rrow
                # 连接谓词与 filter 相同：仅 TRUE 匹配；FALSE/UNKNOWN 不匹配。
                if pred(joined) is True:
                    out.append(joined)
                    matched = True
            if outer and not matched:
                # 仅当左行对所有右行都无 TRUE 匹配时补一行全 NULL。
                out.append(lrow + nulls)
        return out

    return _Compiled(f"({left.label}) {tag} ({right.label})", schema, run,
                     detail={"pred": pred, "outer": outer, "nulls": nulls,
                             "n_left": len(left.schema), "n_right": n_right})


def _compile_union(node, num, left, right):
    ln = [c for _, c in left.schema]
    rn = [c for _, c in right.schema]
    if ln != rn:
        raise SchemaError(
            f"[node#{num}](union_all): 两边列名/顺序不一致：{ln} vs {rn}"
        )
    # UNION ALL 的输出模式取左侧（标准 SQL 同名规则），列数相同。
    schema = tuple(left.schema)

    def run():
        return list(left.run()) + list(right.run())  # 左袋全部行后接右袋全部行

    return _Compiled(f"({left.label}) UNION ALL ({right.label})", schema, run)


def _compile_distinct(node, num, child):
    schema = child.schema

    def run():
        kept = []
        seen = set()
        for row in child.run():
            # IS NOT DISTINCT FROM 语义：NULL 与 NULL 同组；哈希键 O(n) 判定，
            # 保留每个等价组的首次出现，顺序确定。
            key = distinct_key(row)
            if key not in seen:
                seen.add(key)
                kept.append(row)
        return kept

    return _Compiled(child.label + "|distinct", schema, run)


# ------------------------------------------------------------ group_by / window

def _sort_tag(v):
    """跨类型可排序标签：仅用于聚合结果的确定输出顺序。"""
    if v is None:
        return (0, 0)
    if isinstance(v, bool):
        return (1, int(v))
    if isinstance(v, (int, float)):
        return (2, v)
    return (3, v)


def _compile_agg_specs(node, num, input_schema):
    """编译 group_by/window 共用的 keys 与 aggs 规格。

    返回 dict：
      key_funcs  : list[row -> value]
      key_schema : list[(source, name)]（source 固定 ""）
      measure_funcs : list[row -> int|None]（需要表达式的聚合的输入）
      spec_funcs : list[(name, func, measure_index|None)]
      out_schema : keys 输出列 + 聚合输出列（window 另加窗口列）
    """
    where = f"[node#{num}]"
    idx = _col_index(input_schema)
    keys = node.get("keys", [])
    if not isinstance(keys, list):
        raise PlanError(f"{where}(group_by): 'keys' 必须是 list（可为空列表）")
    key_funcs, key_schema, seen_names = [], [], set()
    for i, spec in enumerate(keys):
        if not isinstance(spec, dict) or set(spec) - {"name", "expr", "table"}:
            raise PlanError(f"{where} keys#{i}: 必须是含 name/expr 的 dict")
        name = spec.get("name")
        if not isinstance(name, str) or not name:
            raise PlanError(f"{where} keys#{i}: 'name' 必须是非空字符串")
        if name in seen_names:
            raise SchemaError(f"{where}: 分组键列名重复：{name!r}")
        seen_names.add(name)
        f = compile_expr(spec["expr"], idx, f"{where} keys[{name!r}]/expr")
        key_funcs.append(f)
        key_schema.append((spec.get("table", ""), name))

    aggs = node.get("aggs")
    if not isinstance(aggs, list) or not aggs:
        raise PlanError(f"{where}: 'aggs' 必须是非空 list")
    measure_funcs, measure_src, spec_funcs, agg_schema = [], [], [], []
    for i, spec in enumerate(aggs):
        if not isinstance(spec, dict) or set(spec) - {"name", "func", "expr", "table"}:
            raise PlanError(f"{where} aggs#{i}: 必须是含 name/func 的 dict")
        name = spec.get("name")
        if not isinstance(name, str) or not name:
            raise PlanError(f"{where} aggs#{i}: 'name' 必须是非空字符串")
        if name in seen_names:
            raise SchemaError(f"{where}: 聚合输出列名与已有列重复：{name!r}")
        seen_names.add(name)
        func = spec.get("func")
        if func not in AGG_FUNCS:
            raise PlanError(
                f"{where} aggs#{i}: 未知聚合函数 {func!r}（允许 {AGG_FUNCS}）"
            )
        if func in FUNC_NEEDS_EXPR:
            if "expr" not in spec:
                raise PlanError(f"{where} aggs#{i}({func}): 缺少 'expr'")
            raw_f = compile_expr(spec["expr"], idx, f"{where} aggs[{name!r}]/expr")
            mwhere = f"{where} aggs[{name!r}]({func})"

            def f(row, _raw=raw_f, _w=mwhere):
                return validate_measure(_raw(row), _w)

            measure_funcs.append(f)
            spec_funcs.append((name, func, len(measure_funcs) - 1))
        else:  # count_star 不接受 expr
            if "expr" in spec:
                raise PlanError(f"{where} aggs#{i}(count_star): 不接受 'expr' 字段")
            spec_funcs.append((name, "count_star", None))
        agg_schema.append((spec.get("table", ""), name))

    return {
        "key_funcs": key_funcs,
        "key_schema": key_schema,
        "measure_funcs": measure_funcs,
        "spec_funcs": spec_funcs,
        "out_schema": tuple(key_schema) + tuple(agg_schema),
    }


def _measure_row(row, funcs):
    return tuple(f(row) for f in funcs)


def _compile_group_by(node, num, child):
    spec = _compile_agg_specs(node, num, child.schema)
    schema = spec["out_schema"]

    def run():
        groups = {}  # key_tuple -> Counter[measure_tuple]
        for row in child.run():
            k = tuple(f(row) for f in spec["key_funcs"])
            m = _measure_row(row, spec["measure_funcs"])
            groups.setdefault(k, Counter())[m] += 1
        out = []
        for k in sorted(groups, key=lambda t: tuple(_sort_tag(v) for v in t)):
            vals = compute_aggs(groups[k], spec["spec_funcs"])
            out.append(k + vals)
        return out

    return _Compiled(child.label + "|group_by", schema, run,
                     detail={"group_spec": spec})


def _validate_event_time(v, where):
    """窗口事件时间：非 NULL、非 bool 的整数（逻辑时钟）。"""
    if isinstance(v, bool) or not isinstance(v, int) or v is None:
        raise ExpressionError(
            f"{where}: 事件时间表达式必须求出整数（逻辑时钟），"
            f"得到 {type(v).__name__}={v!r}"
        )
    return v


def _compile_window(node, num, child):
    where = f"[node#{num}](window)"
    width = node.get("width")
    if isinstance(width, bool) or not isinstance(width, int) or width <= 0:
        raise PlanError(f"{where}: 'width' 必须是正整数（逻辑时间单位）")
    idx = _col_index(child.schema)
    time_f = compile_expr(node["time"], idx, f"{where}/time")
    spec = _compile_agg_specs(node, num, child.schema)
    # 输出列：分组键 + window_start/window_end + 聚合。
    schema = (
        tuple(spec["key_schema"])
        + (("", "window_start"), ("", "window_end"))
        + tuple(c for c in spec["out_schema"][len(spec["key_schema"]):])
    )
    nkeys = len(spec["key_schema"])
    agg_offset = nkeys + 2

    def run():
        buckets = {}  # (start, key) -> Counter[measure]
        for row in child.run():
            t = _validate_event_time(time_f(row), where)
            start = (t // width) * width
            k = tuple(f(row) for f in spec["key_funcs"])
            m = _measure_row(row, spec["measure_funcs"])
            buckets.setdefault((start, k), Counter())[m] += 1
        out = []
        for bk in sorted(buckets, key=lambda bk: (bk[0], tuple(_sort_tag(v) for v in bk[1]))):
            start, k = bk
            vals = compute_aggs(buckets[bk], spec["spec_funcs"])
            out.append(k + (start, start + width) + vals)
        return out

    detail = {"group_spec": spec, "time_f": time_f, "width": width,
              "nkeys": nkeys, "agg_offset": agg_offset}
    return _Compiled(child.label + f"|window(w={width})", schema, run,
                     detail=detail)


# ---------------------------------------------------------------- 公开入口

def execute(plan, tables, *, max_rows=MAX_ROWS_PER_TABLE):
    """执行查询计划。

    :param plan: 计划节点 dict 树（见模块文档）。
    :param tables: ``{表名: {"columns": [...], "rows": [[...], ...]}}``。
    :param max_rows: 每张表的行数上限，默认 :data:`MAX_ROWS_PER_TABLE` (200)。
        增量对照等内部场景可显式传更大的值；旧调用不传参时行为完全不变。
    :returns: ``{"columns": [{"source", "name"}], "rows": [[...], ...]}``。
    :raises QueryError: 所有非法输入的基类，子类见 :mod:`rel3vl.errors`。
    """
    norm_tables = validate_tables(tables, limit=max_rows)
    root = compile_plan(plan, norm_tables)
    rows = root.run()
    return {
        "columns": [{"source": s, "name": c} for s, c in root.schema],
        "rows": [list(r) for r in rows],
    }
