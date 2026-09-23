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
* ``{"op": "aggregate", "input": N, "groups": [...], "aggs": [...]}``
* ``{"op": "window", "input": N, "time_col": C, "size": S,
   "slide"?: S, "allowed_lateness"?: 0}``

执行模型：

* 关系 = 带重复行的元组袋，用 ``list[tuple]`` 表示，顺序确定；
* 全部节点先编译（表结构校验、模式推导、表达式编译、上限检查），
  编译期任何错误都在处理任何行之前抛出，因此不存在部分变更；
* 列以 ``(source_id, name)`` 限定：scan 的来源 id 默认为表名，
  自连接时用 ``"as"`` 指定不同来源 id。
"""

from __future__ import annotations

from .errors import PlanError, SchemaError, TableError
from .expressions import compile_expr
from .aggregation import AGG_FUNCS, GroupTable
from .values import distinct_key, validate_scalar

# 旧批量入口 execute(plan, tables) 的契约上限（保持不变）。
MAX_ROWS_PER_TABLE = 200
MAX_NODES = 10
# 增量入口与新算子使用的规模上限（本任务要求 ≥10000 输入行 / 100 批）。
MAX_STREAM_ROWS = 100_000
MAX_STREAM_BATCHES = 10_000
MAX_STREAM_NODES = 50

_CHILD_KEYS = {
    "scan": (),
    "filter": ("input",),
    "project": ("input",),
    "inner_join": ("left", "right"),
    "left_join": ("left", "right"),
    "union_all": ("left", "right"),
    "distinct": ("input",),
    "aggregate": ("input",),
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
    "aggregate": ("input", "aggs"),
    "window": ("input", "time_col", "size"),
}
_ALLOWED_NODE_KEYS = {
    "scan": {"op", "id", "table", "as"},
    "filter": {"op", "id", "input", "expr"},
    "project": {"op", "id", "input", "columns"},
    "inner_join": {"op", "id", "left", "right", "on"},
    "left_join": {"op", "id", "left", "right", "on"},
    "union_all": {"op", "id", "left", "right"},
    "distinct": {"op", "id", "input"},
    "aggregate": {"op", "id", "input", "groups", "aggs"},
    "window": {"op", "id", "input", "time_col", "size", "slide",
               "allowed_lateness"},
}


# ---------------------------------------------------------------- 输入表校验

def validate_tables(tables, max_rows=MAX_ROWS_PER_TABLE):
    """校验 ``tables`` 映射并返回规范化副本 ``{name: (columns, rows)}``。

    表：``{"columns": ["a", ...], "rows": [[...], ...]}``。
    行宽必须一致；列名是非空 str 且表内唯一；行数 ≤ ``max_rows``
    （旧入口默认 200；增量入口传 :data:`MAX_STREAM_ROWS`）；
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
        if len(rows) > max_rows:
            raise TableError(
                f"{prefix}: 行数 {len(rows)} 超过上限 {max_rows}"
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
    __slots__ = ("label", "schema", "run", "window")

    def __init__(self, label, schema, run):
        # schema: tuple[(source_id, column_name)]；run: () -> list[tuple]
        self.label = label
        self.schema = schema
        self.run = run
        self.window = None  # 仅 window 节点设置：{size, slide, lateness, time_idx}


def _col_index(schema):
    return {(s, c): i for i, (s, c) in enumerate(schema)}


def compile_plan(plan, tables, max_nodes=MAX_NODES):
    """编译整棵计划树，返回根 :class:`_Compiled`。编译失败不执行任何节点。

    ``tables`` 为 :func:`validate_tables` 返回的规范化表映射。
    ``max_nodes`` 控制节点数上限（旧入口 10，增量入口
    :data:`MAX_STREAM_NODES`）。
    """
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
    if len(order) > max_nodes:
        raise PlanError(f"[plan]: 节点数 {len(order)} 超过上限 {max_nodes}")

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
        elif op == "aggregate":
            comp = _compile_aggregate(node, num, children[0])
        elif op == "window":
            comp = _compile_window(node, num, children[0])
        else:  # distinct
            comp = _compile_distinct(node, num, children[0])
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

    return _Compiled(f"[node#{num}](scan:{tname})", schema, run)


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

    return _Compiled(child.label + "|filter", child.schema, run)


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

    return _Compiled(child.label + "|project", tuple(out_schema), run)


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
    equi = _extract_equi_keys(node["on"], tuple(left.schema), tuple(right.schema))

    def run_nested(lr, rr):
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

    if equi is None:
        def run():
            return run_nested(left.run(), right.run())
    else:
        first_li, first_ri = equi[0]

        def family(v):
            if v is None:
                return None
            if isinstance(v, bool):
                return "b"
            if isinstance(v, (int, float)):
                return "n"
            return "s"

        # = 在哪些（非 NULL）类型族之间抛 ExpressionError：
        # bool 与 num/str 不兼容；num 与 str 不兼容；同族不抛。
        incompatible = {frozenset(("b", "n")), frozenset(("b", "s")),
                        frozenset(("n", "s"))}

        def run():
            lr = left.run()
            rr = right.run()
            # 类型卫生预检：嵌套循环会对每一对调用完整 ON，故只要两侧在某个
            # 等值键上同时出现互不兼容的非 NULL 类型族值，就必抛 ExpressionError。
            # 预检在产出任何结果行之前抛出，异常类型与条件和嵌套循环一致。
            lfams = [{family(r[li]) for r in lr} - {None} for li, _ in equi]
            rfams = [{family(r[ri]) for r in rr} - {None} for _, ri in equi]
            for j in range(len(equi)):
                for a in sorted(lfams[j]):
                    for b in sorted(rfams[j]):
                        if a != b and frozenset((a, b)) in incompatible:
                            joined_for_type = next(
                                r for r in lr if family(r[equi[j][0]]) == a
                            ) + next(r for r in rr if family(r[equi[j][1]]) == b)
                            pred(joined_for_type)  # 必然抛 ExpressionError
            # 等值分桶（首键），桶内按 rr 出现序；NULL/UNKNOWN 不匹配，不入桶。
            buckets = {}
            for rrow in rr:
                v = rrow[first_ri]
                if v is None:
                    continue
                buckets.setdefault(distinct_key((v,)), []).append(rrow)
            out = []
            for lrow in lr:
                lv = lrow[first_li]
                cands = buckets.get(distinct_key((lv,))) if lv is not None else None
                if cands is None:
                    if outer:
                        out.append(lrow + nulls)
                    continue
                matched = False
                # 桶内保持 rr 顺序；候选之外的右行对该左行必为 FALSE/UNKNOWN，
                # 不产生输出，故输出序列与嵌套循环完全一致。
                for rrow in cands:
                    joined = lrow + rrow
                    if pred(joined) is True:
                        out.append(joined)
                        matched = True
                if outer and not matched:
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


# ---------------------------------------------------------------- 聚合与窗口

def _qname(spec, i, where):
    if not isinstance(spec, dict):
        raise PlanError(f"{where}#{i}: 必须是 dict")
    name = spec.get("name")
    if not isinstance(name, str) or not name:
        raise PlanError(f"{where}#{i}: 'name' 必须是非空字符串")
    source = spec.get("table", "")
    if not isinstance(source, str):
        raise PlanError(f"{where}#{i}: 'table' 必须是字符串")
    extra = set(spec) - {"name", "table", "expr", "func", "arg"}
    if extra:
        raise PlanError(f"{where}#{i}: 含未知字段 {sorted(extra)}")
    return source, name


def _compile_aggregate(node, num, child):
    where = f"[node#{num}](aggregate)"
    groups_spec = node.get("groups", [])
    aggs_spec = node["aggs"]
    if groups_spec is None:
        groups_spec = []
    if not isinstance(groups_spec, list):
        raise PlanError(f"{where}: 'groups' 必须是 list（空 list=全局聚合）")
    if not isinstance(aggs_spec, list) or not aggs_spec:
        raise PlanError(f"{where}: 'aggs' 必须是非空 list")
    idx = _col_index(child.schema)
    out_schema = []
    seen = set()
    group_funcs = []
    for i, spec in enumerate(groups_spec):
        source, name = _qname(spec, i, where + " group")
        if "expr" not in spec:
            raise PlanError(f"{where} group#{i}: 缺少 'expr'")
        f = compile_expr(spec["expr"], idx, f"{where}/expr group {name!r}")
        group_funcs.append(f)
        qn = (source, name)
        if qn in seen:
            raise SchemaError(f"{where}: 输出列 {source}.{name} 重复")
        seen.add(qn)
        out_schema.append(qn)
    specs = []  # (func, has_arg)
    agg_funcs = []
    for i, spec in enumerate(aggs_spec):
        source, name = _qname(spec, i, where + " agg")
        func = spec.get("func")
        if func not in AGG_FUNCS:
            raise PlanError(f"{where} agg#{i}: func 必须是 {AGG_FUNCS}，got {func!r}")
        has_arg = "arg" in spec
        if not has_arg and func != "count":
            raise PlanError(f"{where} agg#{i}: {func} 必须提供 'arg' 表达式")
        f = compile_expr(spec["arg"], idx, f"{where}/expr agg {name!r}") if has_arg else (lambda row: None)
        specs.append((func, has_arg))
        agg_funcs.append(f)
        qn = (source, name)
        if qn in seen:
            raise SchemaError(f"{where}: 输出列 {source}.{name} 重复")
        seen.add(qn)
        out_schema.append(qn)
    is_global = not group_funcs

    def run():
        gt = GroupTable(specs, global_group=is_global, where=where)
        order = []
        for row in child.run():
            key = tuple(f(row) for f in group_funcs)
            gkey = () if is_global else distinct_key(key)
            args = tuple(f(row) for f in agg_funcs)
            if gkey not in gt.groups:
                order.append(gkey)
            gt.update(gkey, key, args, 1)
        if is_global:
            # 全局聚合：空输入也恰好输出一行（count=0，其余为 NULL）。
            if () not in gt.groups:
                zero = tuple(0 if func == "count" else None
                             for func, _ in specs)
                return [zero]
            return [gt.result_row((), ())]
        return gt.result_rows(order)

    return _Compiled(child.label + "|aggregate", tuple(out_schema), run)


def _compile_window(node, num, child):
    from .windows import assign_starts, validate_window_spec

    where = f"[node#{num}](window)"
    size, slide, lateness = validate_window_spec(
        node.get("size"), node.get("slide"), node.get("allowed_lateness"), where
    )
    tc = node.get("time_col")
    if not isinstance(tc, dict):
        raise PlanError(
            f"{where}: time_col 必须是列引用 {{'kind':'col','table','column'}}"
            f"（或简写 {{'table','column'}}）")
    if tc.get("kind", "col") != "col":
        raise PlanError(f"{where}: time_col 必须是单列引用 col（不支持表达式）")
    tc_source = tc.get("table")
    tc_name = tc.get("column")
    allowed = {"kind", "table", "column"}
    if set(tc) - allowed or not isinstance(tc_name, str):
        raise PlanError(
            f"{where}: time_col 只允许字段 kind/table/column，got {sorted(tc)}")
    idx = _col_index(child.schema)
    t_idx = idx.get((tc_source, tc_name))
    if t_idx is None:
        raise SchemaError(
            f"{where}: 时间列 {tc_source}.{tc_name} 不存在；"
            f"可用列：{sorted(child.schema)}"
        )
    for qn in (("", "window_start"), ("", "window_end")):
        if qn in set(child.schema):
            raise SchemaError(f"{where}: 输入已含保留列名 {qn}，无法追加窗口列")
    schema = tuple(child.schema) + (("", "window_start"), ("", "window_end"))

    def run():
        out = []
        for row in child.run():
            t = row[t_idx]
            for start in assign_starts(t, size, slide):
                out.append(row + (start, start + size))
        return out

    # 窗口密封参数挂在编译对象上供增量入口读取。
    comp = _Compiled(child.label + f"|window(size={size},slide={slide})", schema, run)
    comp.window = {"size": size, "slide": slide, "lateness": lateness, "time_idx": t_idx}
    return comp


# ---------------------------------------------------------------- 等值键提取

def _extract_equi_keys(raw_expr, left_schema, right_schema):
    """从 ON 原始表达式中提取左右列等值合取支。

    返回 ``[(li, ri), ...]`` 及剩余表达式（无法分析时返回 ``None``）。
    仅识别顶层 AND 链上的 ``col = col``；任何一边不在对应分支模式中即放弃。
    """
    left_set = set(left_schema)
    right_set = set(right_schema)
    keys = []

    def is_col(e):
        return isinstance(e, dict) and e.get("kind") == "col"

    def side(e):
        if not is_col(e):
            return None
        qn = (e.get("table"), e.get("column"))
        if qn in left_set and qn not in right_set:
            return 0, left_schema.index(qn)
        if qn in right_set and qn not in left_set:
            return 1, right_schema.index(qn)
        return None

    def visit(e):
        if not isinstance(e, dict):
            return False
        if e.get("kind") == "and":
            return visit(e.get("left")) and visit(e.get("right"))
        if e.get("kind") == "cmp" and e.get("op") == "=":
            a, b = e.get("left"), e.get("right")
            sa, sb = side(a), side(b)
            if sa is None or sb is None or sa[0] == sb[0]:
                return False
            if sa[0] == 0:
                keys.append((sa[1], sb[1]))
            else:
                keys.append((sb[1], sa[1]))
            return True
        return False

    ok = visit(raw_expr)
    return keys if ok and keys else None


# ---------------------------------------------------------------- 公开入口

def execute(plan, tables, *, max_rows=MAX_ROWS_PER_TABLE, max_nodes=MAX_NODES):
    """执行查询计划（批量解释器）。

    :param plan: 计划节点 dict 树（见模块文档）。
    :param tables: ``{表名: {"columns": [...], "rows": [[...], ...]}}``。
    :param max_rows: 每表行数上限，缺省保持旧契约 200；增量对照入口传
      :data:`MAX_STREAM_ROWS`。
    :param max_nodes: 计划节点数上限，缺省保持旧契约 10。
    :returns: ``{"columns": [{"source", "name"}], "rows": [[...], ...]}``。
    :raises QueryError: 所有非法输入的基类，子类见 :mod:`rel3vl.errors`。
    """
    norm_tables = validate_tables(tables, max_rows=max_rows)
    root = compile_plan(plan, norm_tables, max_nodes=max_nodes)
    rows = root.run()
    return {
        "columns": [{"source": s, "name": c} for s, c in root.schema],
        "rows": [list(r) for r in rows],
    }
