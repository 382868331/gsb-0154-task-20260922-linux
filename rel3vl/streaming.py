"""带行 ID 的增删流增量解释器（袋语义、事件时间滚动窗口、水位）。

把与 :func:`rel3vl.execute` 同格式的计划树作为有向算子图增量维护：
输入是按**行 ID** 的插入/删除流，输出是根结果袋的差量变更流。

固定语义（缺省即契约）：

* 变更 ``{"id", "op": "insert"|"delete", "row"?}``：id 不可变（改值 =
  删旧 + 插新）；同批同 id 一次；插入已存在 id / 删除未知 id 抛
  :class:`StreamError`；
* 袋重数：重复内容按不同 id 各计一重；join 输出身份为 ``(左id, 右id)``
  与外连接补 NULL 行 ``(左id, None)``；匹配数 0↔正 时撤回/补出 NULL 行；
* 水位单调不减；同批**先按批前水位接收数据，再推进水位并报告新密封窗口**；
* 窗口 ``[start, end)`` 在 ``end <= watermark - allowed_lateness`` 时密封；
  落入已密封窗口的更新整批拒绝、状态不变；
* 阈值内迟到允许；任何一层结果变化同时输出撤回 ``-`` 与新增 ``+``；
* 单批原子：只读试算阶段做完全部校验与差量计算，提交阶段不允许抛异常；
* changelog 同批先全部 ``-`` 后全部 ``+``，按行内容全序排序，确定性可复现。

每个节点在批次结果的 ``touched`` 中记录实际触碰的状态键
（等值键桶、分组键、窗口起点、配对身份、PAD 翻转身份），
可据此核对"未对无关组做全量扫描"。
"""

from __future__ import annotations

import json
from collections import Counter

from .aggregation import AggAccumulator
from .engine import (
    MAX_STREAM_BATCHES,
    MAX_STREAM_NODES,
    MAX_STREAM_ROWS,
    _extract_equi_keys,
    compile_plan,
)
from .errors import ExpressionError, PlanError, SchemaError, StreamError
from .expressions import compile_expr
from .values import _NULL_KEY, distinct_key, validate_scalar
from .windows import assign_starts, validate_event_time, validate_window_spec

PAD = None          # 外连接补 NULL 行的右身份哨兵
ABSENT = object()   # 试算视图中"该身份不存在"
SNAPSHOT_VERSION = 1

# 管道类一元节点（身份直接继承 scan 行 id）。
_PIPE_OPS = ("scan", "filter", "project")


# ---------------------------------------------------------------- 确定性排序

def sort_key(v):
    """混合标量全序：NULL < bool < num < str（num 按数值，1 与 1.0 同位）。"""
    if v is None:
        return (0,)
    if isinstance(v, bool):
        return (1, int(v))
    if isinstance(v, (int, float)):
        return (2, float(v))
    return (3, v)


def row_sort_key(row):
    return tuple(sort_key(v) for v in row)


def id_key(v):
    if isinstance(v, bool):
        raise StreamError(f"行 id 不接受 bool（与 int 歧义），got {v!r}")
    if isinstance(v, int):
        return (1, v)
    if isinstance(v, str) and v:
        return (2, v)
    raise StreamError(f"行 id 必须是非空 int/str，got {v!r}")


def ident_key(v):
    """任意节点身份的确定排序键（递归展开 tuple/str/int/特殊标记）。"""
    if v is None:
        return (0,)
    if isinstance(v, bool):
        return (1, int(v))
    if isinstance(v, int):
        return (2, v)
    if isinstance(v, str):
        return (3, v)
    if isinstance(v, tuple):
        return (4, tuple(_jsonable_key(x) for x in v))
    return (5, str(v))


def _jsonable_key(v):
    # tuple 内可能是 distinct_key 的标记组（含 _NULL_KEY object）；
    # 用 repr 兜底，保证可排序即可。
    if isinstance(v, (tuple, list)):
        return ("t", tuple(_jsonable_key(x) for x in v))
    if v is None:
        return ("n",)
    if isinstance(v, bool):
        return ("b", int(v))
    if isinstance(v, (int, float)):
        return ("f", float(v))
    if isinstance(v, str):
        return ("s", v)
    return ("o", repr(v))


# ---------------------------------------------------------------- 增量算子节点

class _Node:
    def __init__(self, idx, op, schema, children):
        self.idx = idx
        self.op = op
        self.schema = tuple(schema) if schema is not None else None
        self.children = list(children)


def _col_index(schema):
    return {(s, c): i for i, (s, c) in enumerate(schema)}


def _compile(plan, table_columns):
    """编译为增量算子图。计划已通过 compile_plan 的完整静态校验。

    节点编号与 ``engine.compile_plan`` 的 DFS 先序一致（错误信息 node#k
    对齐）；返回 ``(nodes, root_idx, postorder)``，postorder 保证孩子先于父。
    """
    child_keys = {
        "scan": (), "filter": ("input",), "project": ("input",),
        "inner_join": ("left", "right"), "left_join": ("left", "right"),
        "union_all": ("left", "right"), "distinct": ("input",),
        "aggregate": ("input",), "window": ("input",),
    }
    numbering = {}
    order = []

    def number(node):
        idx = len(order)
        order.append(node)
        numbering[id(node)] = idx
        for k in child_keys[node["op"]]:
            number(node[k])

    number(plan)
    nodes = {}

    def build(node):
        op = node["op"]
        kids = [build(node[k]) for k in child_keys[op]]
        idx = numbering[id(node)]
        ch = [nodes[k] for k in kids]
        if op == "scan":
            tname = node["table"]
            if tname not in table_columns:
                raise PlanError(
                    f"[plan](scan): 流表 {tname!r} 未在 inputs 中声明"
                    f"（已声明 {sorted(table_columns)}）")
            source = node.get("as", tname)
            n = _Node(idx, "scan",
                      tuple((source, c) for c in table_columns[tname]), [])
            n.table = tname
        else:
            n = _Node(idx, op, None, ch)
            if op == "filter":
                n.schema = ch[0].schema
                n.pred = compile_expr(node["expr"], _col_index(n.schema),
                                      f"[node#{idx}/expr](filter)")
            elif op == "project":
                schema, funcs = [], []
                imap = _col_index(ch[0].schema)
                for spec in node["columns"]:
                    funcs.append(compile_expr(spec["expr"], imap,
                                              f"[node#{idx}/expr](project) {spec['name']!r}"))
                    schema.append((spec.get("table", ""), spec["name"]))
                n.schema, n.funcs = tuple(schema), funcs
            elif op in ("inner_join", "left_join"):
                n.schema = ch[0].schema + ch[1].schema
                n.outer = op == "left_join"
                n.pred = compile_expr(node["on"], _col_index(n.schema),
                                      f"[node#{idx}/expr]({op})")
                n.equi = _extract_equi_keys(node["on"], ch[0].schema, ch[1].schema)
                n.nright = len(ch[1].schema)
            elif op == "union_all":
                n.schema = ch[0].schema
            elif op == "distinct":
                n.schema = ch[0].schema
            elif op == "aggregate":
                where = f"[node#{idx}](aggregate)"
                imap = _col_index(ch[0].schema)
                gfuncs, schema = [], []
                for spec in node.get("groups") or []:
                    gfuncs.append(compile_expr(spec["expr"], imap,
                                               f"{where}/expr group {spec['name']!r}"))
                    schema.append((spec.get("table", ""), spec["name"]))
                specs, afuncs = [], []
                for spec in node["aggs"]:
                    has_arg = "arg" in spec
                    specs.append((spec["func"], has_arg))
                    afuncs.append(
                        compile_expr(spec["arg"], imap,
                                     f"{where}/expr agg {spec['name']!r}")
                        if has_arg else (lambda row: None))
                    schema.append((spec.get("table", ""), spec["name"]))
                n.schema = tuple(schema)
                n.group_funcs, n.agg_funcs = gfuncs, afuncs
                n.agg_specs, n.is_global = specs, not gfuncs
            else:  # window
                where = f"[node#{idx}](window)"
                size, slide, lateness = validate_window_spec(
                    node.get("size"), node.get("slide"),
                    node.get("allowed_lateness"), where)
                tc = node["time_col"]
                if tc.get("kind", "col") != "col":
                    raise PlanError(f"{where}: time_col 必须是单列引用 col")
                t_idx = _col_index(ch[0].schema).get(
                    (tc.get("table"), tc.get("column")))
                if t_idx is None:
                    raise SchemaError(
                        f"{where}: 时间列 {tc.get('table')}.{tc.get('column')} 不存在")
                n.schema = ch[0].schema + (("", "window_start"), ("", "window_end"))
                n.size, n.slide, n.lateness = size, slide, lateness
                n.time_idx = t_idx
        nodes[idx] = n
        return idx

    root = build(plan)
    postorder = [numbering[id(nd)] for nd in order]  # order 即先序；构建后按依赖排序
    # 以构建完成顺序（build 是后序递归）重建后序列表：
    post = []

    def postvisit(nd):
        for k in child_keys[nd["op"]]:
            postvisit(nd[k])
        post.append(numbering[id(nd)])

    postvisit(plan)
    return nodes, root, post


# ---------------------------------------------------------------- 处理器

class IncrementalProcessor:
    """增量维护一棵计划的有状态处理器。

    :param plan: 与 :func:`execute` 同格式的计划。
    :param inputs: ``{表名: [列名, ...]}`` 声明输入流与列模式。
    """

    def __init__(self, plan, inputs, *, max_rows=MAX_STREAM_ROWS,
                 max_nodes=MAX_STREAM_NODES, max_batches=MAX_STREAM_BATCHES):
        self._plan = plan
        self._max_rows = max_rows
        self._max_batches = max_batches
        if not isinstance(inputs, dict) or not inputs:
            raise StreamError("[stream]: inputs 必须是非空 dict {表名: [列名,...]}")
        table_columns = {}
        for tname, cols in inputs.items():
            if not isinstance(tname, str) or not tname:
                raise StreamError("[stream]: 输入表名必须是非空 str")
            if not isinstance(cols, (list, tuple)) or not cols:
                raise StreamError(f"[stream]: 输入 {tname!r} 必须声明非空列名 list")
            if any(not isinstance(c, str) or not c for c in cols):
                raise StreamError(f"[stream]: 输入 {tname!r} 列名必须是非空 str")
            if len(set(cols)) != len(cols):
                raise StreamError(f"[stream]: 输入 {tname!r} 列名重复")
            table_columns[tname] = tuple(cols)
        self._columns = table_columns
        # 先复用批量入口的全部静态校验（结构/表达式/模式/上限）。
        from .engine import validate_tables
        norm = validate_tables(
            {t: {"columns": list(cs), "rows": []}
             for t, cs in table_columns.items()},
            max_rows=max_rows)
        compile_plan(plan, norm, max_nodes=max_nodes)
        self.nodes, self.root_idx, self._postorder = _compile(plan, table_columns)
        self.schema = self.nodes[self.root_idx].schema
        self._init_state()

    # ------------------------------------------------------------ 持久状态
    def _init_state(self):
        self.watermark = 0
        self.batch_seq = 0
        self.total_rows = 0
        self.rows = {t: {} for t in self._columns}          # scan: {id: row}
        self.dist_mult = {}    # distinct idx: {content_gkey: mult}
        # distinct idx: {gkey: {ident: row}} 成员身份->行（决定"首现行"代表）
        self.dist_members = {}
        self.join_items = {}   # join idx: {(lid, rid/PAD): row}
        self.join_buckets = {} # join idx: {"l"/"r": {joinkey: [(id, row)]}}
        self.join_keyof = {}   # join idx: {"l"/"r": {id: joinkey}}
        self.join_pair_l = {}  # join idx: {lid: {rid}}  当前非 PAD 配对（左索引）
        self.join_pair_r = {}  # join idx: {rid: {lid}}  当前非 PAD 配对（右索引）
        self.join_pads = {}    # join idx: {lid}       当前产出 PAD 行的左身份
        # join idx: {"l"/"r": [ {family: {cid: row}} 每个等值列 ] }
        # 用于复现批量嵌套循环的跨类型比较异常（bool/int、str/num 同侧并存）。
        self.join_fams = {}
        self.agg_groups = {}   # agg idx: {gkey: {"raw","acc","n"}}
        self.agg_order = {}    # agg idx: [gkey 首现序]
        self.wm_frontier = {}  # window idx: 已报告密封的 end 上界
        for i, n in self.nodes.items():
            if n.op == "distinct":
                self.dist_mult[i] = {}
                self.dist_members[i] = {}
            elif n.op in ("inner_join", "left_join"):
                self.join_items[i] = {}
                self.join_buckets[i] = {"l": {}, "r": {}}
                self.join_keyof[i] = {"l": {}, "r": {}}
                self.join_pair_l[i] = {}
                self.join_pair_r[i] = {}
                self.join_pads[i] = set()
                # 每侧每个等值列的类型族计数（复现批量嵌套循环的跨类型异常）
                self.join_fams[i] = {"l": [Counter() for _ in n.equi or ()],
                                     "r": [Counter() for _ in n.equi or ()]}
            elif n.op == "aggregate":
                self.agg_groups[i] = {}
                self.agg_order[i] = []
                if n.is_global:
                    self.agg_groups[i][()] = {
                        "raw": (), "acc": AggAccumulator(n.agg_specs), "n": 0}
                    self.agg_order[i].append(())
            elif n.op == "window":
                self.wm_frontier[i] = 0

    # ================================================================ 批次
    def apply_batch(self, batch):
        """原子应用一批，返回根结果袋 changelog 与触碰键记录。"""
        if not isinstance(batch, dict):
            raise StreamError("[stream]: 批次必须是 dict")
        if set(batch) - {"watermark", "changes"}:
            raise StreamError(
                f"[stream]: 批次只允许 watermark/changes，得到 {sorted(batch)}")
        if self.batch_seq >= self._max_batches:
            raise StreamError(f"[stream]: 批次数超过上限 {self._max_batches}")
        new_wm = batch.get("watermark", self.watermark)
        if isinstance(new_wm, bool) or not isinstance(new_wm, int) or new_wm < 0:
            raise StreamError(f"[stream]: watermark 必须是非负 int，got {new_wm!r}")
        if new_wm < self.watermark:
            raise StreamError(
                f"[stream]: 水位不可回退：{self.watermark} -> {new_wm}")

        leaf = self._parse_changes(batch.get("changes"))  # 只读校验
        ctx = self._trial(leaf, new_wm)                    # 只读试算（可能拒绝）
        # ---------------- 提交（以下不再做任何可失败操作） ----------------
        for t, changes in leaf.items():
            tab = self.rows[t]
            for ch in changes:
                if ch["op"] == "insert":
                    tab[ch["id"]] = ch["row"]
                    self.total_rows += 1
                else:
                    del tab[ch["id"]]
                    self.total_rows -= 1
        for i in self._postorder:
            n = self.nodes[i]
            if n.op == "distinct":
                self._commit_distinct(n, ctx)
            elif n.op in ("inner_join", "left_join"):
                self._commit_join(n, ctx)
            elif n.op == "aggregate":
                self._commit_aggregate(n, ctx)
        self.watermark = new_wm
        sealed_now = self._advance_sealing()
        self.batch_seq += 1
        return {
            "watermark": new_wm,
            "batch_seq": self.batch_seq,
            "changes": self._emit(ctx["delta"][self.root_idx]),
            "sealed_windows": sealed_now,
            "touched": {i: ctx["touched"][i] for i in sorted(ctx["touched"])},
        }

    # ------------------------------------------------------------ 叶子校验
    def _parse_changes(self, raw):
        if raw is None:
            return {}
        if not isinstance(raw, dict):
            raise StreamError("[stream]: changes 必须是 dict {表名: [变更,...]}")
        leaf = {}
        for tname, changes in raw.items():
            if tname not in self._columns:
                raise StreamError(
                    f"[stream]: 变更引用未声明表 {tname!r}（已声明 {sorted(self._columns)}）")
            if not isinstance(changes, list):
                raise StreamError(f"[stream]: 表 {tname!r} 的变更必须是 list")
            cols = self._columns[tname]
            parsed, seen_ids = [], set()
            cur = self.rows[tname]
            for j, ch in enumerate(changes):
                if not isinstance(ch, dict):
                    raise StreamError(f"[stream:{tname}] #{j}: 变更必须是 dict")
                if set(ch) - {"id", "op", "row"}:
                    raise StreamError(f"[stream:{tname}] #{j}: 只允许 id/op/row")
                cid, cop = ch.get("id"), ch.get("op")
                id_key(cid)
                if cop not in ("insert", "delete"):
                    raise StreamError(
                        f"[stream:{tname}] #{j}: op 必须是 insert/delete，got {cop!r}")
                if cid in seen_ids:
                    raise StreamError(f"[stream:{tname}] #{j}: 同批 id {cid!r} 重复")
                seen_ids.add(cid)
                exists = cid in cur
                where = f"[stream:{tname}] id={cid!r}"
                if cop == "insert":
                    if exists:
                        raise StreamError(f"{where}: id 已存在，不能重复 insert")
                    if "row" not in ch:
                        raise StreamError(f"{where}: insert 必须带 row")
                    parsed.append({"id": cid, "op": "insert",
                                   "row": self._validate_row(ch["row"], cols, tname)})
                else:
                    if not exists:
                        raise StreamError(f"{where}: id 不存在，不能 delete")
                    if "row" in ch and tuple(ch["row"]) != cur[cid]:
                        raise StreamError(
                            f"{where}: delete 携带 row 与当前不一致："
                            f"{list(ch['row'])} vs {list(cur[cid])}")
                    parsed.append({"id": cid, "op": "delete", "row": cur[cid]})
            n_ins = sum(c["op"] == "insert" for c in parsed)
            n_del = len(parsed) - n_ins
            if len(cur) + n_ins - n_del > self._max_rows:
                raise StreamError(
                    f"[stream:{tname}]: 批后行数超过上限 {self._max_rows}")
            leaf[tname] = parsed
        return leaf

    def _validate_row(self, row, cols, tname):
        if not isinstance(row, (list, tuple)):
            raise StreamError(f"[stream:{tname}]: row 必须是 list/tuple")
        if len(row) != len(cols):
            raise StreamError(
                f"[stream:{tname}]: 行宽 {len(row)} 与列数 {len(cols)} 不一致")
        return tuple(validate_scalar(v, f"[stream:{tname}] column {c!r}")
                     for c, v in zip(cols, row))

    # ================================================================ 试算
    def _trial(self, leaf, new_wm):
        """只读阶段：计算每个节点的 delta/受影响身份/批后视图与触碰键。

        ctx 字段：
          post[i]   scan 批后 {id: row}
          aff[i]    批后可能变化的本节点输出身份集合
          new[i]    受影响身份的批后行（ABSENT 表示批后不存在）
          old[i]    受影响身份的批前行（ABSENT 表示批前不存在）
          delta[i]  Counter[行内容]（正新增/负撤回）
          touched[i] 实际触碰的状态键记录
        """
        ctx = {"post": {}, "aff": {}, "new": {}, "old": {},
               "delta": {}, "touched": {}, "before_post": None}
        for i, n in self.nodes.items():
            if n.op == "scan":
                cur = self.rows[n.table]
                post = dict(cur)
                aff = set()
                d = Counter()
                for ch in leaf.get(n.table, []):
                    aff.add(ch["id"])
                    if ch["op"] == "insert":
                        post[ch["id"]] = ch["row"]
                        d[ch["row"]] += 1
                    else:
                        d[cur[ch["id"]]] -= 1
                        del post[ch["id"]]
                ctx["post"][i] = post
                ctx["aff"][i] = aff
                ctx["new"][i] = {x: post.get(x, ABSENT) for x in aff}
                ctx["old"][i] = {x: cur.get(x, ABSENT) for x in aff}
                ctx["delta"][i] = d
                ctx["touched"][i] = {"op": "scan", "table": n.table,
                                     "rows": _dump_ids(aff)}

        for i in self._postorder:
            n = self.nodes[i]
            if n.op == "scan":
                continue
            if n.op in ("filter", "project"):
                self._trial_pipe(n, ctx)
            elif n.op == "union_all":
                self._trial_union(n, ctx)
            elif n.op == "distinct":
                self._trial_distinct(n, ctx)
            elif n.op in ("inner_join", "left_join"):
                self._trial_join(n, ctx)
            elif n.op == "aggregate":
                self._trial_aggregate(n, ctx)
            else:
                self._trial_window(n, ctx, new_wm)
        return ctx

    # ---------------- 身份 -> 行 的批前/批后解析 ----------------
    def _resolve_before(self, n, ident, ctx=None):
        if ctx is not None:
            if ctx["before_post"] is None:
                ctx["before_post"] = self._before_post()
            post = ctx["before_post"]
        else:
            post = self._before_post()
        return self._resolve(n, ident, post)

    def _before_post(self):
        # 批前试算视图：scan 即持久表（同一 dict 对象，只读）。
        return {i: self.rows[n.table] for i, n in self.nodes.items()
                if n.op == "scan"}

    def _resolve(self, n, ident, post):
        """在给定 scan 视图 post 下解析节点 n 的身份 ident 对应行。"""
        op = n.op
        if op == "scan":
            return post[n.idx].get(ident, ABSENT)
        if op == "filter":
            r = self._resolve(n.children[0], ident, post)
            return r if r is not ABSENT and n.pred(r) is True else ABSENT
        if op == "project":
            r = self._resolve(n.children[0], ident, post)
            return tuple(f(r) for f in n.funcs) if r is not ABSENT else ABSENT
        if op == "distinct":
            members = self.dist_members[n.idx].get(ident)
            if not members:
                return ABSENT
            return members[min(members, key=ident_key)]
        if op == "union_all":
            side, cid = ident
            return self._resolve(n.children[0 if side == "l" else 1], cid, post)
        if op in ("inner_join", "left_join"):
            return self.join_items[n.idx].get(ident, ABSENT)
        if op == "aggregate":
            slot = self.agg_groups[n.idx].get(ident)
            return tuple(slot["raw"]) + slot["acc"].result() if slot else ABSENT
        if op == "window":
            cid, start = ident
            r = self._resolve(n.children[0], cid, post)
            if r is ABSENT:
                return ABSENT
            return r + (start, start + n.size)
        raise AssertionError(op)


    def _resolve_after(self, n, ident, ctx):
        """批后解析；受影响身份用试算结果，其余回退持久状态。"""
        if ident in ctx["new"].get(n.idx, {}):
            return ctx["new"][n.idx][ident]
        return self._resolve_before(n, ident)

    # ---------------- 管道 / union ----------------
    def _trial_pipe(self, n, ctx):
        c = n.children[0]
        olds, news, d = {}, {}, Counter()
        for cid in ctx["aff"][c.idx]:
            old = self._resolve_before(n, cid)
            new = self._after_pipe(n, cid, ctx)
            olds[cid], news[cid] = old, new
            if old is not ABSENT:
                d[old] -= 1
            if new is not ABSENT:
                d[new] += 1
        # 子值变化但本节点输出行未变（如投影不含变化列、过滤态未翻转）时，
        # 该身份不向下游传播，避免被误当作新事件（下游有 window 时尤甚）。
        aff = {cid for cid in ctx["aff"][c.idx] if olds[cid] != news[cid]}
        ctx["aff"][n.idx] = aff
        ctx["old"][n.idx] = {k: olds[k] for k in aff}
        ctx["new"][n.idx] = {k: news[k] for k in aff}
        d = Counter({r: m for r, m in d.items() if m != 0})
        ctx["delta"][n.idx] = d
        ctx["touched"][n.idx] = {"op": n.op, "rows": _dump_ids(aff)}

    def _after_pipe(self, n, cid, ctx):
        c = n.children[0]
        r = ctx["new"][c.idx].get(cid) if cid in ctx["new"].get(c.idx, {}) else None
        if cid in ctx["new"].get(c.idx, {}):
            r = ctx["new"][c.idx][cid]
        else:
            r = self._resolve_before(c, cid)
        if r is ABSENT:
            return ABSENT
        if n.op == "filter":
            return r if n.pred(r) is True else ABSENT
        return tuple(f(r) for f in n.funcs)

    def _trial_union(self, n, ctx):
        aff0 = set()
        olds, news, d = {}, {}, Counter()
        for side, c in (("l", n.children[0]), ("r", n.children[1])):
            for cid in ctx["aff"][c.idx]:
                ident = (side, cid)
                aff0.add(ident)
                old = self._resolve_before(n, ident)
                new = self._resolve_after(c, cid, ctx)
                olds[ident], news[ident] = old, new
                if old is not ABSENT:
                    d[old] -= 1
                if new is not ABSENT:
                    d[new] += 1
        aff = {x for x in aff0 if olds[x] != news[x]}
        ctx["aff"][n.idx], ctx["old"][n.idx], ctx["new"][n.idx] = (
            aff, {x: olds[x] for x in aff}, {x: news[x] for x in aff})
        d = Counter({r: m for r, m in d.items() if m != 0})
        ctx["delta"][n.idx] = d
        ctx["touched"][n.idx] = {
            "op": "union_all",
            "left_rows": _dump_ids(ctx["aff"][n.children[0].idx]),
            "right_rows": _dump_ids(ctx["aff"][n.children[1].idx])}

    # ---------------- distinct ----------------
    def _trial_distinct(self, n, ctx):
        c = n.children[0]
        members = self.dist_members[n.idx]   # {gkey: {ident: row}}
        olds_map, news_map = ctx["old"][c.idx], ctx["new"][c.idx]
        # 叠加层：只复制被触碰组的成员表。
        overlay = {}

        def group_after(gk):
            if gk not in overlay:
                base = members.get(gk)
                overlay[gk] = dict(base) if base is not None else {}
            return overlay[gk]

        d, aff = Counter(), set()
        olds, news = {}, {}
        for cid in ctx["aff"][c.idx]:
            old_row = olds_map.get(cid, ABSENT)
            new_row = news_map.get(cid, ABSENT)
            if old_row is new_row is ABSENT:
                continue
            gk_before = distinct_key(old_row) if old_row is not ABSENT else None
            gk_after = distinct_key(new_row) if new_row is not ABSENT else None
            if gk_before is not None:
                group_after(gk_before).pop(cid, None)
            if gk_after is not None:
                group_after(gk_after)[cid] = new_row
        for gk, after_members in overlay.items():
            before_members = members.get(gk, {})
            old_rep = self._distinct_rep(before_members)
            new_rep = self._distinct_rep(after_members)
            if old_rep is new_rep is ABSENT:
                continue
            aff.add(gk)
            olds[gk], news[gk] = old_rep, new_rep
            if old_rep is not ABSENT:
                d[old_rep] -= 1
            if new_rep is not ABSENT:
                d[new_rep] += 1
        ctx["aff"][n.idx], ctx["old"][n.idx], ctx["new"][n.idx] = aff, olds, news
        ctx["delta"][n.idx] = d
        ctx["touched"][n.idx] = {
            "op": "distinct",
            "groups": _dump_keys(aff),
            "rows": _dump_ids(ctx["aff"][c.idx])}

    @staticmethod
    def _distinct_rep(member_map):
        """distinct 输出代表 = 成员最小身份对应行（对齐批量"首现行"语义）。"""
        if not member_map:
            return ABSENT
        cid = min(member_map, key=ident_key)
        return member_map[cid]

    # ---------------- join ----------------
    @staticmethod
    def _val_family(v):
        if v is None:
            return None
        if isinstance(v, bool):
            return "b"
        if isinstance(v, (int, float)):
            return "n"
        return "s"

    _FAM_INCOMPAT = {frozenset(("b", "n")), frozenset(("b", "s")),
                     frozenset(("n", "s"))}

    def _join_key(self, n, row, side):
        if n.equi is None:
            return ("__all__",)
        idxs = [pair[0 if side == "l" else 1] for pair in n.equi]
        vals = tuple(row[i] for i in idxs)
        if any(v is None for v in vals):
            return None  # NULL 键不匹配（与 ON 中 = 的 UNKNOWN 语义一致）
        return distinct_key(vals)

    def _trial_join(self, n, ctx):
        lc, rc = n.children[0], n.children[1]
        al, ar = set(ctx["aff"][lc.idx]), set(ctx["aff"][rc.idx])

        # 批后类型族计数（叠加本批变化），复现批量等值哈希路径的跨类型预检：
        # 同一等值列两侧并存互不兼容族（bool/int、bool/str、num/str）即报错，
        # 即使它们具体值不同——与 execute 的嵌套循环/哈希口径一致。
        if n.equi:
            fam_after = {}
            for side, child, aff_ids in (("l", lc, al), ("r", rc, ar)):
                cols_fam = [cnt.copy() for cnt in self.join_fams[n.idx][side]]
                idxs = [pair[0 if side == "l" else 1] for pair in n.equi]
                for cid in aff_ids:
                    old_row = self._resolve_before(child, cid)
                    new_row = self._resolve_after(child, cid, ctx)
                    for j, ix in enumerate(idxs):
                        if old_row is not ABSENT:
                            fo = self._val_family(old_row[ix])
                            if fo:
                                cols_fam[j][fo] -= 1
                        if new_row is not ABSENT:
                            fn = self._val_family(new_row[ix])
                            if fn:
                                cols_fam[j][fn] += 1
                fam_after[side] = cols_fam
            for j in range(len(n.equi)):
                lf = {f for f, c in fam_after["l"][j].items() if c > 0}
                rf = {f for f, c in fam_after["r"][j].items() if c > 0}
                for a in lf:
                    for b in rf:
                        if a != b and frozenset((a, b)) in self._FAM_INCOMPAT:
                            raise ExpressionError(
                                f"[node#{n.idx}/expr]({'left_join' if n.outer else 'inner_join'}): "
                                f"等值列 #{j} 两侧类型族不兼容：{a} vs {b}"
                                f"（bool 不视作 int，str 与数值不可隐式比较）")

        # 批后桶视图：持久桶（始终按身份有序）叠加受影响身份的删/插。
        def side_view(child, aff_ids, side):
            persisted = self.join_buckets[n.idx][side]
            moved = {}
            adds_by_key = {}
            for cid in aff_ids:
                old_row = self._resolve_before(child, cid)
                new_row = self._resolve_after(child, cid, ctx)
                ok = self._join_key(n, old_row, side) if old_row is not ABSENT else None
                nk = self._join_key(n, new_row, side) if new_row is not ABSENT else None
                moved[cid] = (ok, nk, new_row)
                if nk is not None and new_row is not ABSENT:
                    adds_by_key.setdefault(nk, []).append((cid, new_row))
            cache = {}

            def view(key):
                if key is None:
                    return []
                if key in cache:
                    return cache[key]
                # 持久桶有序：剔除受影响身份（保持序），再与有序新增归并。
                base = [p for p in persisted.get(key, ()) if p[0] not in moved]
                adds = sorted(adds_by_key.get(key, ()), key=lambda p: ident_key(p[0]))
                if adds:
                    merged, bi, ai = [], 0, 0
                    while bi < len(base) and ai < len(adds):
                        if ident_key(base[bi][0]) <= ident_key(adds[ai][0]):
                            merged.append(base[bi]); bi += 1
                        else:
                            merged.append(adds[ai]); ai += 1
                    merged.extend(base[bi:]); merged.extend(adds[ai:])
                    base = merged
                cache[key] = base
                return base

            return view, moved

        lview, lmoved = side_view(lc, al, "l")
        rview, rmoved = side_view(rc, ar, "r")

        pair_l, pair_r, pads = (self.join_pair_l[n.idx],
                                self.join_pair_r[n.idx], self.join_pads[n.idx])
        affected_lids = set(al)
        touched_lkeys, touched_rkeys = set(), set()
        for cid, (ok, nk, _) in lmoved.items():
            touched_lkeys.update(k for k in (ok, nk) if k is not None)
        for cid, (ok, nk, _) in rmoved.items():
            touched_rkeys.update(k for k in (ok, nk) if k is not None)
            # 受影响右身份波及：批前与之配对的左身份 + 批后同桶的左身份。
            affected_lids.update(pair_r.get(cid, ()))
            for key in (ok, nk):
                if key is not None:
                    affected_lids.update(x for x, _ in lview(key))
        if n.equi is None:
            # 无等值键：受影响右行波及全部左身份（退化全枚举，仅小域参考用）。
            affected_lids.update(cid for cid, _ in self._all_identities(lc))

        d = Counter()
        pair_aff = set()
        pad_switches = []
        items = self.join_items[n.idx]
        nulls = (None,) * n.nright

        for lid in sorted(affected_lids, key=ident_key):
            lrow_after = self._resolve_after(lc, lid, ctx)
            # 批前配对：经左身份配对索引直接取，不全表扫描 items。
            before_rows = Counter()
            for rid in pair_l.get(lid, ()):
                before_rows[items[(lid, rid)]] += 1
                pair_aff.add((lid, rid))
            # 批后配对：探测批后右桶。
            after_rows = Counter()
            if lrow_after is not ABSENT:
                key = self._join_key(n, lrow_after, "l")
                if key is not None:
                    touched_lkeys.add(key)  # 桶被实际触碰（即使变空）
                for rid, rrow in rview(key):
                    if n.pred(lrow_after + rrow) is True:
                        after_rows[lrow_after + rrow] += 1
                        pair_aff.add((lid, rid))
                        if key is not None:
                            touched_rkeys.add(key)
            old_pad = lid in pads
            new_pad = (n.outer and lrow_after is not ABSENT
                       and sum(after_rows.values()) == 0)
            for row, m in before_rows.items():
                d[row] -= m
            for row, m in after_rows.items():
                d[row] += m
            if old_pad and not new_pad:
                # 匹配数 0->正（或左行删除）：撤回旧补 NULL 行。
                prow = items.get((lid, PAD))
                if prow is None:
                    pl = self._resolve_before(lc, lid)
                    prow = pl + nulls
                d[prow] -= 1
                pair_aff.add((lid, PAD))
                pad_switches.append(lid)
            elif new_pad and not old_pad:
                d[lrow_after + nulls] += 1  # 正->0：补出 NULL 行
                pair_aff.add((lid, PAD))
                pad_switches.append(lid)

        # 受影响右身份自身的行变化已包含在上面的桶视图重算中；但桶键未变、
        # 仅右行内容变化（如 project 子节点改值）时，lview/rview 已覆盖。
        olds = {p: items.get(p, ABSENT) for p in pair_aff}
        news = {}
        for p in pair_aff:
            lid, rid = p
            if rid is PAD:
                lr = self._resolve_after(lc, lid, ctx)
                news[p] = (lr + nulls if (n.outer and lr is not ABSENT
                           and self._after_match_count(n, lid, lr, rview) == 0)
                           else ABSENT)
            else:
                lr = self._resolve_after(lc, lid, ctx)
                rr = self._resolve_after(rc, rid, ctx)
                if lr is not ABSENT and rr is not ABSENT and n.pred(lr + rr) is True:
                    news[p] = lr + rr
                else:
                    news[p] = ABSENT
        ctx["old"][n.idx] = olds
        ctx["new"][n.idx] = news
        # 只保留真正变化的输出身份：右行插入会令一批左身份的旧配对被重算，
        # 但这些配对内容未变（delta 中 -/+ 已抵消），绝不能沿链路传播，
        # 否则下游 window 会把未变化的旧事件误当作更新而撞密封检查。
        kept = {p for p in pair_aff if olds[p] != news[p]}
        ctx["aff"][n.idx] = kept
        ctx["old"][n.idx] = {p: olds[p] for p in kept}
        ctx["new"][n.idx] = {p: news[p] for p in kept}
        d = Counter({row: m for row, m in d.items() if m != 0})
        ctx["delta"][n.idx] = d
        ctx["touched"][n.idx] = {
            "op": "left_join" if n.outer else "inner_join",
            "left_keys": _dump_keys(touched_lkeys),
            "right_keys": _dump_keys(touched_rkeys),
            "pairs": _dump_pairs(kept - {(x, PAD) for x in pad_switches}),
            "pair_count": len(kept),
            "pad_switched": _dump_ids(set(pad_switches))}

    def _after_match_count(self, n, lid, lrow, rview):
        key = self._join_key(n, lrow, "l")
        return sum(1 for _, rrow in rview(key) if n.pred(lrow + rrow) is True)

    def _all_identities(self, n):
        """枚举节点当前全部 (身份, 行)（仅非等值 join 退化路径与重建使用）。"""
        if n.op in _PIPE_OPS:
            post = self._before_post()
            out = []
            if n.op == "scan":
                return list(post[n.idx].items())
            for cid in post[self._scan_leaf(n).idx]:
                r = self._resolve(n, cid, post)
                if r is not ABSENT:
                    out.append((cid, r))
            return out
        if n.op == "union_all":
            return [(("l", c), r) for c, r in self._all_identities(n.children[0])] + \
                   [(("r", c), r) for c, r in self._all_identities(n.children[1])]
        if n.op == "distinct":
            out = []
            for gk, member_map in self.dist_members[n.idx].items():
                if member_map:
                    out.append((gk, member_map[min(member_map, key=ident_key)]))
            return out
        if n.op in ("inner_join", "left_join"):
            return list(self.join_items[n.idx].items())
        if n.op == "aggregate":
            return [(gk, tuple(s["raw"]) + s["acc"].result())
                    for gk, s in self.agg_groups[n.idx].items()]
        if n.op == "window":
            out = []
            for cid, row in self._all_identities(n.children[0]):
                t = row[n.time_idx]
                for s in assign_starts(t, n.size, n.slide):
                    out.append(((cid, s), row + (s, s + n.size)))
            return out
        raise AssertionError(n.op)

    def _scan_leaf(self, n):
        while n.op not in ("scan", "union_all", "inner_join", "left_join",
                           "aggregate", "window", "distinct"):
            n = n.children[0]
        return n

    # ---------------- aggregate ----------------
    def _group_parts(self, n, row):
        if n.is_global:
            return (), ()
        raw = tuple(f(row) for f in n.group_funcs)
        return distinct_key(raw), raw

    def _trial_aggregate(self, n, ctx):
        c = n.children[0]
        groups_tab = self.agg_groups[n.idx]
        # 复制所有被触碰组的累加器（试算用，不碰持久状态）。
        trial_accs = {}
        trial_raw = {}

        def acc_for(gk, raw):
            if gk not in trial_accs:
                tpl = AggAccumulator(n.agg_specs)
                slot = groups_tab.get(gk)
                if slot is not None:
                    tpl.count_all = slot["acc"].count_all
                    tpl.states = [
                        s.copy() if isinstance(s, Counter)
                        else (list(s) if isinstance(s, list) else s)
                        for s in slot["acc"].states]
                    trial_raw[gk] = slot["raw"]
                else:
                    trial_raw[gk] = raw
                trial_accs[gk] = tpl
            return trial_accs[gk]

        # 子输出袋差量直接按组应用（对任意子节点类型都成立）。
        gset = set()
        for row, dm in ctx["delta"][c.idx].items():
            if dm == 0:
                continue
            gk, raw = self._group_parts(n, row)
            if n.is_global and dm == 0:
                continue
            acc_for(gk, raw).apply(tuple(f(row) for f in n.agg_funcs), dm)
            gset.add(gk)
        # 全局聚合：即使子 delta 净值为零但行有进出（不同行导致聚合值变化），
        # Counter 已按内容聚合，dm 净值即袋变化；sum/avg 等对净值正确。
        d, aff, olds, news = Counter(), set(), {}, {}
        touched_groups = []
        for gk in gset:
            old_slot = groups_tab.get(gk)
            new_acc = trial_accs[gk]
            old_row = (tuple(old_slot["raw"]) + old_slot["acc"].result()
                       if old_slot is not None else ABSENT)
            if new_acc.count_all == 0 and not n.is_global:
                new_row = ABSENT
            else:
                new_row = tuple(trial_raw[gk]) + new_acc.result()
            aff.add(gk)
            olds[gk], news[gk] = old_row, new_row
            if old_row is not ABSENT:
                d[old_row] -= 1
            if new_row is not ABSENT:
                d[new_row] += 1
            touched_groups.append(list(trial_raw[gk]))
        # 触碰分组按内容全序输出，避免 dict 哈希序影响跨进程确定性。
        touched_groups.sort(key=lambda g: json.dumps(g, ensure_ascii=False,
                                                     default=str))
        ctx["aff"][n.idx], ctx["old"][n.idx], ctx["new"][n.idx] = aff, olds, news
        ctx["delta"][n.idx] = d
        ctx["touched"][n.idx] = {"op": "aggregate", "groups": touched_groups}

    # ---------------- window ----------------
    def _starts_of(self, n, row):
        t = row[n.time_idx]
        validate_event_time(t, f"[node#{n.idx}](window)")
        return assign_starts(t, n.size, n.slide)

    def _trial_window(self, n, ctx, new_wm):
        c = n.children[0]
        d, aff = Counter(), set()
        olds, news = {}, {}
        touched_starts = set()
        # 子身份路径：受影响身份逐个解析批前/批后行（对任意子类型成立）。
        for cid in ctx["aff"][c.idx]:
            old_row = self._resolve_before(c, cid)
            new_row = self._resolve_after(c, cid, ctx)
            old_starts = self._starts_of(n, old_row) if old_row is not ABSENT else ()
            new_starts = self._starts_of(n, new_row) if new_row is not ABSENT else ()
            # 密封判定用**批前**水位：本批推进密封的窗口仍接收本批数据。
            for s in set(old_starts) | set(new_starts):
                self._reject_if_sealed(n, s, cid)
                touched_starts.add(s)
            for s in old_starts:
                ident = (cid, s)
                aff.add(ident)
                out = old_row + (s, s + n.size)
                olds[ident] = out
                d[out] -= 1
            for s in new_starts:
                ident = (cid, s)
                aff.add(ident)
                out = new_row + (s, s + n.size)
                news[ident] = out
                d[out] += 1
            for s in set(old_starts) & set(new_starts):
                # 同一窗口身份批前批后都在：行内容可能被子层改变。
                ident = (cid, s)
                olds[ident] = old_row + (s, s + n.size)
                news[ident] = new_row + (s, s + n.size)
        # 受影响窗口身份缺省另一侧为 ABSENT（不能回退到持久解析，否则
        # 已删除身份会被误判为仍存在）。
        for ident in aff:
            olds.setdefault(ident, ABSENT)
            news.setdefault(ident, ABSENT)
        ctx["aff"][n.idx], ctx["old"][n.idx], ctx["new"][n.idx] = aff, olds, news
        ctx["delta"][n.idx] = d
        ctx["touched"][n.idx] = {
            "op": "window",
            "window_starts": sorted(touched_starts),
            "events": _dump_ids(ctx["aff"][c.idx])}

    def _reject_if_sealed(self, n, start, who):
        end = start + n.size
        if end <= self.watermark - n.lateness:
            raise StreamError(
                f"[node#{n.idx}](window): 更新 {_format_ident(who)} 落入已密封窗口 "
                f"[{start}, {end})：watermark={self.watermark}, "
                f"allowed_lateness={n.lateness}；超过保留期限，整批拒绝、状态不变")

    # ================================================================ 提交
    def _commit_distinct(self, n, ctx):
        mult_tab = self.dist_mult[n.idx]
        members = self.dist_members[n.idx]
        c = n.children[0]
        for cid in ctx["aff"][c.idx]:
            old_row = ctx["old"][c.idx].get(cid, ABSENT)
            new_row = ctx["new"][c.idx].get(cid, ABSENT)
            if old_row is new_row is ABSENT:
                continue
            if old_row is not ABSENT:
                gk = distinct_key(old_row)
                members[gk].pop(cid, None)
                mult_tab[gk] -= 1
                if mult_tab[gk] == 0:
                    del mult_tab[gk]
                    members.pop(gk, None)
            if new_row is not ABSENT:
                gk = distinct_key(new_row)
                if gk not in members:
                    members[gk] = {}
                if cid not in members[gk]:
                    members[gk][cid] = new_row
                    mult_tab[gk] = mult_tab.get(gk, 0) + 1
                else:
                    members[gk][cid] = new_row

    def _commit_join(self, n, ctx):
        items = self.join_items[n.idx]
        buckets = self.join_buckets[n.idx]
        keyof = self.join_keyof[n.idx]
        pair_l, pair_r, pads = (self.join_pair_l[n.idx],
                                self.join_pair_r[n.idx], self.join_pads[n.idx])
        fams = self.join_fams[n.idx]
        # 1) 桶索引（经身份->桶键索引 O(1) 摘除；桶始终有序，插入走归并）。
        for side, child in (("l", n.children[0]), ("r", n.children[1])):
            tab, kof = buckets[side], keyof[side]
            fam_cols = fams[side]
            idxs = [pair[0 if side == "l" else 1] for pair in (n.equi or ())]
            adds = []
            olds_map, news_map = ctx["old"][child.idx], ctx["new"][child.idx]
            for cid in ctx["aff"][child.idx]:
                old_row = olds_map.get(cid, ABSENT)
                new_row = news_map.get(cid, ABSENT)
                old_key = kof.pop(cid, None)
                if old_key is not None:
                    bucket = tab[old_key]
                    bucket[:] = [p for p in bucket if p[0] != cid]
                    if not bucket:
                        del tab[old_key]
                # 类型族计数维护
                for j, ix in enumerate(idxs):
                    if old_row is not ABSENT:
                        fo = self._val_family(old_row[ix])
                        if fo:
                            fam_cols[j][fo] -= 1
                    if new_row is not ABSENT:
                        fn = self._val_family(new_row[ix])
                        if fn:
                            fam_cols[j][fn] += 1
                if new_row is not ABSENT:
                    new_key = self._join_key(n, new_row, side)
                    if new_key is not None:
                        adds.append((new_key, cid, new_row))
                        kof[cid] = new_key
            adds.sort(key=lambda t: (tuple(_jsonable_key(t[0])), ident_key(t[1])))
            for new_key, cid, new_row in adds:
                bucket = tab.get(new_key)
                pair = (cid, new_row)
                pk = ident_key(cid)
                if bucket is None:
                    tab[new_key] = [pair]
                    continue
                lo, hi = 0, len(bucket)
                while lo < hi:
                    mid = (lo + hi) // 2
                    if ident_key(bucket[mid][0]) < pk:
                        lo = mid + 1
                    else:
                        hi = mid
                bucket.insert(lo, pair)
        # 2) 受影响输出身份：先拆旧关系/PAD，再按批后身份表重建。
        for ident in ctx["aff"][n.idx]:
            lid, rid = ident
            if rid is PAD:
                pads.discard(lid)
            else:
                if lid in pair_l:
                    pair_l[lid].discard(rid)
                    if not pair_l[lid]:
                        del pair_l[lid]
                if rid in pair_r:
                    pair_r[rid].discard(lid)
                    if not pair_r[rid]:
                        del pair_r[rid]
            items.pop(ident, None)
        for ident, row in ctx["new"][n.idx].items():
            if row is ABSENT:
                continue
            lid, rid = ident
            items[ident] = row
            if rid is PAD:
                pads.add(lid)
            else:
                pair_l.setdefault(lid, set()).add(rid)
                pair_r.setdefault(rid, set()).add(lid)

    def _commit_aggregate(self, n, ctx):
        c = n.children[0]
        groups_tab = self.agg_groups[n.idx]
        order = self.agg_order[n.idx]
        for row, dm in ctx["delta"][c.idx].items():
            if dm == 0:
                continue
            gk, raw = self._group_parts(n, row)
            slot = groups_tab.get(gk)
            if slot is None:
                slot = {"raw": raw, "acc": AggAccumulator(n.agg_specs), "n": 0}
                groups_tab[gk] = slot
                order.append(gk)
            slot["acc"].apply(tuple(f(row) for f in n.agg_funcs), dm)
            slot["n"] += dm
            if slot["n"] == 0 and not n.is_global:
                del groups_tab[gk]
                order.remove(gk)

    def _advance_sealing(self):
        """水位推进后报告新密封窗口；只推进每节点的密封前沿，不扫事件。"""
        out = []
        for i, n in self.nodes.items():
            if n.op != "window":
                continue
            limit = self.watermark - n.lateness
            prev = self.wm_frontier[i]
            if limit <= prev:
                continue
            # 起点网格为 slide 的整数倍（事件时间非负，最早起点 0）。
            s = ((prev - n.size) // n.slide + 1) * n.slide if prev > 0 else 0
            while s < 0:
                s += n.slide
            s = max(s, 0)
            while s + n.size <= limit:
                if s + n.size > prev:
                    out.append({"node": i, "window_start": s,
                                "window_end": s + n.size})
                s += n.slide
            self.wm_frontier[i] = max(prev, limit)
        out.sort(key=lambda e: (e["node"], e["window_start"]))
        return out

    # ---------------------------------------------------------------- 输出
    def _emit(self, delta):
        out = []
        for row, mult in delta.items():
            if mult > 0:
                out.append({"op": "+", "row": list(row), "mult": mult})
            elif mult < 0:
                out.append({"op": "-", "row": list(row), "mult": -mult})
        out.sort(key=lambda c: (0 if c["op"] == "-" else 1, row_sort_key(c["row"])))
        return out

    # ================================================================ 物化
    def current_result(self):
        """物化为根结果袋（按行内容全序展开重数，确定性顺序）。"""
        bag = Counter()
        for _, row in self._all_identities(self.nodes[self.root_idx]):
            bag[row] += 1
        out = []
        for row in sorted(bag, key=row_sort_key):
            out.extend([list(row)] * bag[row])
        return out

    def current_bag(self):
        bag = Counter()
        for _, row in self._all_identities(self.nodes[self.root_idx]):
            bag[row] += 1
        return bag

    # ================================================================ 快照
    def snapshot(self):
        return {
            "version": SNAPSHOT_VERSION,
            "watermark": self.watermark,
            "batch_seq": self.batch_seq,
            "total_rows": self.total_rows,
            "rows": {t: [[cid, list(r)] for cid, r in sorted(
                tab.items(), key=lambda kv: id_key(kv[0]))]
                for t, tab in self.rows.items()},
            "wm_frontier": {str(i): f for i, f in self.wm_frontier.items()},
        }

    def dumps_snapshot(self):
        return json.dumps(self.snapshot(), ensure_ascii=False, sort_keys=True)

    def restore(self, snap):
        if not isinstance(snap, dict) or snap.get("version") != SNAPSHOT_VERSION:
            raise StreamError("[stream]: 快照版本不合法")
        wm, seq = snap["watermark"], snap["batch_seq"]
        if isinstance(wm, bool) or not isinstance(wm, int) or wm < 0:
            raise StreamError("[stream]: 快照水位非法")
        if not isinstance(seq, int) or seq < 0:
            raise StreamError("[stream]: 快照批次号非法")
        self._init_state()
        for t, items in snap["rows"].items():
            if t not in self._columns:
                raise StreamError(f"[stream]: 快照含未声明表 {t!r}")
            cols = self._columns[t]
            for cid, raw_row in items:
                id_key(cid)
                self.rows[t][cid] = self._validate_row(raw_row, cols, t)
                self.total_rows += 1
        # 从基表按确定 id 序重放，重建 distinct/join/aggregate 全部派生状态。
        self._rebuild_derived()
        self.watermark = wm
        self.batch_seq = seq
        for k, f in snap.get("wm_frontier", {}).items():
            self.wm_frontier[int(k)] = f
        return self

    def loads_snapshot(self, text):
        return self.restore(json.loads(text))

    def _rebuild_derived(self):
        """从基表袋按节点拓扑重建全部派生状态（distinct/join/aggregate）。"""
        # 节点按 DFS 先序编号，孩子编号恒大于父；降序即孩子在先。
        for i in self._postorder:
            n = self.nodes[i]
            if n.op == "distinct":
                mult_tab, members = self.dist_mult[i], self.dist_members[i]
                for cid, row in sorted(self._all_identities(n.children[0]),
                                       key=lambda p: ident_key(p[0])):
                    gk = distinct_key(row)
                    if gk not in members:
                        members[gk] = {}
                    if cid not in members[gk]:
                        members[gk][cid] = row
                        mult_tab[gk] = mult_tab.get(gk, 0) + 1
            elif n.op in ("inner_join", "left_join"):
                self._rebuild_join(n)
            elif n.op == "aggregate":
                groups_tab, order = self.agg_groups[i], self.agg_order[i]
                for _, row in self._all_identities(n.children[0]):
                    gk, raw = ((), ()) if n.is_global else (
                        distinct_key(tuple(f(row) for f in n.group_funcs)),
                        tuple(f(row) for f in n.group_funcs))
                    slot = groups_tab.get(gk)
                    if slot is None:
                        slot = {"raw": raw, "acc": AggAccumulator(n.agg_specs), "n": 0}
                        groups_tab[gk] = slot
                        order.append(gk)
                    slot["acc"].apply(tuple(f(row) for f in n.agg_funcs), 1)
                    slot["n"] += 1

    def _rebuild_join(self, n):
        items = self.join_items[n.idx]
        buckets = self.join_buckets[n.idx]
        keyof = self.join_keyof[n.idx]
        pair_l, pair_r, pads = (self.join_pair_l[n.idx],
                                self.join_pair_r[n.idx], self.join_pads[n.idx])
        fams = self.join_fams[n.idx]
        items.clear()
        buckets["l"].clear()
        buckets["r"].clear()
        keyof["l"].clear()
        keyof["r"].clear()
        pair_l.clear()
        pair_r.clear()
        pads.clear()
        for side_cols in (fams["l"], fams["r"]):
            for c in side_cols:
                c.clear()
        l_items = self._all_identities(n.children[0])
        r_items = self._all_identities(n.children[1])

        def add_fam(side_cols, idxs, row):
            for j, ix in enumerate(idxs):
                f = self._val_family(row[ix])
                if f:
                    side_cols[j][f] += 1

        l_idx = [p[0] for p in (n.equi or ())]
        r_idx = [p[1] for p in (n.equi or ())]
        for lid, lrow in l_items:
            add_fam(fams["l"], l_idx, lrow)
        for rid, rrow in r_items:
            add_fam(fams["r"], r_idx, rrow)
        for lid, lrow in l_items:
            key = self._join_key(n, lrow, "l")
            if key is not None:
                buckets["l"].setdefault(key, []).append((lid, lrow))
                keyof["l"][lid] = key
        for rid, rrow in r_items:
            key = self._join_key(n, rrow, "r")
            if key is not None:
                buckets["r"].setdefault(key, []).append((rid, rrow))
                keyof["r"][rid] = key
        for tab in (buckets["l"], buckets["r"]):
            for key in tab:
                tab[key].sort(key=lambda p: ident_key(p[0]))
        nulls = (None,) * n.nright
        for lid, lrow in sorted(l_items, key=lambda p: ident_key(p[0])):
            key = self._join_key(n, lrow, "l")
            cnt = 0
            if key is not None:
                for rid, rrow in buckets["r"].get(key, ()):
                    if n.pred(lrow + rrow) is True:
                        items[(lid, rid)] = lrow + rrow
                        pair_l.setdefault(lid, set()).add(rid)
                        pair_r.setdefault(rid, set()).add(lid)
                        cnt += 1
            if n.outer and cnt == 0:
                items[(lid, PAD)] = lrow + nulls
                pads.add(lid)


# ---------------------------------------------------------------- 触碰键序列化

def _dump_ids(ids):
    return sorted((_jsonify_ident(v) for v in ids),
                  key=lambda v: json.dumps(v, ensure_ascii=False, default=str))


def _dump_keys(keys):
    # join/group 的哈希键是 distinct_key 风格 tuple（含标记）；
    # 给出 JSON 友好的嵌套 list 形式，保持次序确定。
    out = []
    for k in keys:
        out.append(_jsonify_key(k))
    return sorted(out, key=lambda v: json.dumps(v, ensure_ascii=False, sort_keys=True,
                                                default=str))


def _jsonify_key(k):
    if not isinstance(k, tuple):
        return k
    out = []
    for part in k:
        if part is _NULL_KEY:
            out.append(None)
        elif isinstance(part, tuple) and len(part) == 2 and part[0] in ("b", "n", "s"):
            out.append(part[1])
        else:
            out.append(part)
    return out


def _dump_pairs(pairs):
    return sorted((_jsonify_ident(a), _jsonify_ident(b)) for a, b in pairs)


def _jsonify_ident(v):
    if v is _NULL_KEY:
        return None
    if isinstance(v, tuple):
        if len(v) == 2 and v[0] in ("b", "n", "s"):
            return v[1]  # distinct_key 的类型标记解包
        return [_jsonify_ident(x) for x in v]
    if isinstance(v, list):
        return [_jsonify_ident(x) for x in v]
    return v


def _format_ident(v):
    """错误消息中的可读身份渲染（NULL 哨兵显示为 None）。"""
    return repr(_jsonify_ident(v))
