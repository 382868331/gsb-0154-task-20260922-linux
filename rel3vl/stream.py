"""增删流增量解释器（袋语义、行 ID、事件时间窗口、水位、快照）。

公开入口：:class:`IncrementalExecutor`。它与批量 :func:`rel3vl.execute`
共用同一套计划/表达式编译与聚合实现（:mod:`rel3vl.engine`、
:mod:`rel3vl.aggregates`），因此增量结果与"把每批累计输入交给批量解释器"
的口径逐行一致（测试以独立批量重放做对照，参考实现不调用被测增量代码）。

变更流模型
----------

* 每个基表行带**调用方给定的稳定字符串行 ID**；袋多重数 = 不同 ID 数，
  相同内容的行按重数真实计入。
* 一个批次 = 一组增删记录 + 可选的新水位；整批先在影子状态上计算，
  任何非法记录（含迟到、3VL 求值类型错）都在提交前抛出，**状态不变**。
* 根节点每批只发布一次净变更：``{"row": [...], "delta": +n/-n}``；
  聚合值改变表现为"旧结果行 -1、新结果行 +1"（撤回 + 新增），不只追加。
* 逻辑水位单调不减；事件时间 t 满足 ``t >= watermark - allowed_lateness``
  才接受；更早的更新抛 :class:`rel3vl.errors.LateDataError`，且不改状态。
* 当 ``s + width <= watermark - allowed_lateness`` 时窗口 [s, s+width) 关闭：
  结果行冻结保留，桶状态释放，之后进入该窗口的任何增删一律拒绝。
"""

from __future__ import annotations

import bisect
import json
from collections import Counter

from .aggregates import compute_aggs
from .engine import (
    _CHILD_KEYS,
    _col_index,
    _measure_row,
    _sort_tag,
    compile_plan,
    validate_tables,
)
from .errors import (
    ChangeError,
    LateDataError,
    PlanError,
    SnapshotError,
    WatermarkError,
)
from .values import distinct_key, validate_scalar

SNAPSHOT_VERSION = 1

# 增量入口规模上限（批量 execute() 的 200 行默认上限保持不变）。
STREAM_MAX_ROWS_PER_TABLE = 100_000
STREAM_MAX_BATCH_CHANGES = 100_000


# ---------------------------------------------------------------- 小工具

def _drop_zero(bag, keys):
    for k in keys:
        if bag.get(k) == 0:
            del bag[k]


def _join_hash_key(row, positions):
    """等值连接键；任一分量为 NULL 返回 None（NULL=NULL 为 UNKNOWN，不匹配）。"""
    if any(row[i] is None for i in positions):
        return None
    return tuple(row[i] for i in positions)


def _sort_rows_key(row):
    return tuple(_sort_tag(v) for v in row)


def _key_repr(k):
    """确定、可读的状态键表示（touched 观测用，不参与任何判定）。"""
    return json.dumps(k, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), default=str)


def _bag_json(bag):
    return [[list(row), mult] for row, mult in sorted(bag.items(),
                                                      key=lambda kv: _sort_rows_key(kv[0]))]


def _bag_from_json(data):
    return Counter({tuple(row): mult for row, mult in data})


def _split_equi_conjuncts(raw_on, left_schema, right_schema):
    """把 ON 拆成左右分列的 ``col = col`` 等值合取项；无法拆分返回 None。

    拆成时用等值项建哈希索引（NULL 不进索引），但仍以编译后的完整 ON
    谓词对候选行最终确认，因此残差谓词与三值逻辑口径与批量完全一致。
    """
    lidx = _col_index(left_schema)
    ridx = _col_index(right_schema)
    terms = []

    def walk(node):
        if not isinstance(node, dict):
            return False
        k = node.get("kind")
        if k == "and":
            return walk(node["left"]) and walk(node["right"])
        if k == "cmp" and node.get("op") == "=":
            a, b = node.get("left"), node.get("right")
            if not (isinstance(a, dict) and isinstance(b, dict)):
                return False
            if a.get("kind") != "col" or b.get("kind") != "col":
                return False
            pa = lidx.get((a.get("table"), a.get("column")))
            pb = ridx.get((b.get("table"), b.get("column")))
            if pa is not None and pb is not None:
                terms.append((pa, pb))
                return True
            qa = ridx.get((a.get("table"), a.get("column")))
            qb = lidx.get((b.get("table"), b.get("column")))
            if qa is not None and qb is not None:
                terms.append((qb, qa))
                return True
        return False

    if not walk(raw_on) or not terms:
        return None
    return tuple(terms)


# ---------------------------------------------------------------- 状态节点

class _Node:
    """增量状态节点：持有输出袋 bag，消费子节点袋净增量并向父节点转发。"""

    def __init__(self, compiled, children, raw):
        self.compiled = compiled
        self.num = compiled.num
        self.label = compiled.label
        self.schema = compiled.schema
        self.children = tuple(children)
        self.raw = raw
        self.bag = Counter()       # 输出行袋（重数恒 >= 0）
        self.touched = set()       # 本批实际触碰的状态键（观测/证明用）

    def clone(self):  # pragma: no cover - 子类覆盖
        raise NotImplementedError

    def _copy_base(self, other):
        other.bag = Counter(self.bag)
        other.touched = set()

    def push(self, child, deltas, ctx):  # pragma: no cover
        raise NotImplementedError

    def export_state(self):  # pragma: no cover
        raise NotImplementedError

    def import_state(self, data):  # pragma: no cover
        raise NotImplementedError

    @staticmethod
    def root_deltas_to_changes(deltas):
        out = [
            (row, d) for row, d in deltas.items() if d
        ]
        out.sort(key=lambda kv: _sort_rows_key(kv[0]))
        return [{"row": list(row), "delta": d} for row, d in out]


class _Scan(_Node):
    def __init__(self, compiled, raw):
        super().__init__(compiled, (), raw)
        self.table = compiled.detail["table"]
        self.columns = compiled.detail["columns"]
        self.rows_by_id = {}   # id -> tuple

    def clone(self):
        c = _Scan(self.compiled, self.raw)
        c.rows_by_id = dict(self.rows_by_id)
        self._copy_base(c)
        return c

    def apply_change(self, op, rid, values):
        prefix = f"[table:{self.table}]"
        if not isinstance(rid, str) or not rid:
            raise ChangeError(f"{prefix}: 行 id 必须是非空字符串，得到 {rid!r}")
        if op == "-" and values is None:
            # 删除允许只给 id（按行身份删除）；行内容由执行器记录。
            if rid not in self.rows_by_id:
                raise ChangeError(f"{prefix}: 删除的行 id {rid!r} 不存在")
            row = self.rows_by_id.pop(rid)
            self.touched.add(f"id:{rid}")
            self.bag[row] -= 1
            if self.bag[row] == 0:
                del self.bag[row]
            return Counter({row: -1})
        if not isinstance(values, (list, tuple)):
            raise ChangeError(f"{prefix} id={rid!r}: row 必须是 list")
        if len(values) != len(self.columns):
            raise ChangeError(
                f"{prefix} id={rid!r}: 行宽 {len(values)} 与列数 {len(self.columns)} 不一致"
            )
        row = tuple(
            validate_scalar(v, f"{prefix} id={rid!r} column {c!r}")
            for c, v in zip(self.columns, values)
        )
        if op == "+":
            if rid in self.rows_by_id:
                raise ChangeError(f"{prefix}: 插入的行 id {rid!r} 已存在")
            self.rows_by_id[rid] = row
        else:
            if rid not in self.rows_by_id:
                raise ChangeError(f"{prefix}: 删除的行 id {rid!r} 不存在")
            old = self.rows_by_id.pop(rid)
            if values and old != row:
                raise ChangeError(
                    f"{prefix} id={rid!r}: 删除内容与当前行不一致 "
                    f"{list(old)} vs {list(row)}"
                )
            row = old
        self.touched.add(f"id:{rid}")
        d = 1 if op == "+" else -1
        self.bag[row] += d
        if self.bag[row] == 0:
            del self.bag[row]
        return Counter({row: d})

    def seed(self, rid, row):
        t = tuple(row)
        self.rows_by_id[rid] = t
        self.bag[t] += 1

    def export_state(self):
        return {"rows": [[rid, list(row)] for rid, row in sorted(self.rows_by_id.items())]}

    def import_state(self, data):
        self.rows_by_id = {}
        self.bag = Counter()
        for rid, row in data["rows"]:
            self.seed(rid, row)


class _Filter(_Node):
    def __init__(self, compiled, child, raw):
        super().__init__(compiled, (child,), raw)
        self.pred = compiled.detail["pred"]

    def clone(self):
        c = _Filter(self.compiled, self.children[0], self.raw)
        self._copy_base(c)
        return c

    def push(self, child, deltas, ctx):
        out = Counter()
        for row, d in deltas.items():
            if self.pred(row) is True:  # 3VL：仅 TRUE 通过
                out[row] += d
                self.touched.add("row:" + _key_repr(row))
        self.bag.update(out)
        _drop_zero(self.bag, out)
        return out

    def export_state(self):
        return {"bag": _bag_json(self.bag)}

    def import_state(self, data):
        self.bag = _bag_from_json(data["bag"])


class _Project(_Node):
    def __init__(self, compiled, child, raw):
        super().__init__(compiled, (child,), raw)
        self.funcs = compiled.detail["proj_funcs"]

    def clone(self):
        c = _Project(self.compiled, self.children[0], self.raw)
        self._copy_base(c)
        return c

    def push(self, child, deltas, ctx):
        out = Counter()
        for row, d in deltas.items():
            out[tuple(f(row) for f in self.funcs)] += d
            self.touched.add("in:" + _key_repr(row))
        self.bag.update(out)
        _drop_zero(self.bag, out)
        return out

    def export_state(self):
        return {"bag": _bag_json(self.bag)}

    def import_state(self, data):
        self.bag = _bag_from_json(data["bag"])


class _Union(_Node):
    def __init__(self, compiled, left, right, raw):
        super().__init__(compiled, (left, right), raw)

    def clone(self):
        c = _Union(self.compiled, self.children[0], self.children[1], self.raw)
        self._copy_base(c)
        return c

    def push(self, child, deltas, ctx):
        self.bag.update(deltas)
        _drop_zero(self.bag, deltas)
        for row in deltas:
            self.touched.add("row:" + _key_repr(row))
        return Counter(deltas)

    def export_state(self):
        return {"bag": _bag_json(self.bag)}

    def import_state(self, data):
        self.bag = _bag_from_json(data["bag"])


class _Distinct(_Node):
    def __init__(self, compiled, child, raw):
        super().__init__(compiled, (child,), raw)
        self.in_bag = Counter()

    def clone(self):
        c = _Distinct(self.compiled, self.children[0], self.raw)
        c.in_bag = Counter(self.in_bag)
        self._copy_base(c)
        return c

    def push(self, child, deltas, ctx):
        out = Counter()
        for row, d in deltas.items():
            before = self.in_bag[row]
            after = before + d
            if after < 0:  # pragma: no cover - 下层生命周期保证
                raise AssertionError("distinct 输入重数变为负")
            if after:
                self.in_bag[row] = after
            else:
                self.in_bag.pop(row, None)
            self.touched.add("row:" + _key_repr(row))
            if before == 0 and after > 0:
                out[row] += 1
            elif before > 0 and after == 0:
                out[row] -= 1
        self.bag.update(out)
        _drop_zero(self.bag, out)
        return out

    def export_state(self):
        return {"bag": _bag_json(self.bag), "in_bag": _bag_json(self.in_bag)}

    def import_state(self, data):
        self.bag = _bag_from_json(data["bag"])
        self.in_bag = _bag_from_json(data["in_bag"])


class _Join(_Node):
    """内/左外连接。状态：左右输入袋 L/R、等值哈希索引、输出袋。

    差量传播规则（M(l) = 左行 l 的*单个副本*在右袋中的 TRUE 匹配重数）：

    * 左行 l 变化 d：对每个匹配右行 r，``O[l,r] += d·R[r]``；
      左外且 M(l)=0 时，``O[l,NULL] += d``（零匹配才补，按左副本数补）。
    * 右行 r 变化 d：先更新 R，对每个匹配左行 l，``O[l,r] += L[l]·d``；
      补 NULL 行 ``O[l,NULL] += L[l]·(1[M'=0] − 1[M=0])``，
      其中 ``M = M' − d``（r 对 M(l) 的贡献恰好变化 d）。
      于是匹配数 0→1 撤回 L[l] 条旧补 NULL 行，1→0 补回 L[l] 条；
      1→1、0→0 不动。全程只枚举与 r 真正匹配的左行，不扫描无关组。

    重数证明例见 README「差量传播与重数证明」。
    """

    def __init__(self, compiled, left, right, raw):
        super().__init__(compiled, (left, right), raw)
        det = compiled.detail
        self.pred = det["pred"]
        self.outer = det["outer"]
        self.nulls = det["nulls"]
        self.L, self.R = Counter(), Counter()
        self.lidx, self.ridx = {}, {}
        pairs = _split_equi_conjuncts(raw["on"], left.schema, right.schema)
        self.equi = pairs
        self.lpos = tuple(p[0] for p in pairs) if pairs else None
        self.rpos = tuple(p[1] for p in pairs) if pairs else None

    def clone(self):
        c = _Join.__new__(_Join)
        _Node.__init__(c, self.compiled, self.children, self.raw)
        c.pred, c.outer, c.nulls = self.pred, self.outer, self.nulls
        c.equi, c.lpos, c.rpos = self.equi, self.lpos, self.rpos
        c.L, c.R = Counter(self.L), Counter(self.R)
        c.lidx = {k: Counter(v) for k, v in self.lidx.items()}
        c.ridx = {k: Counter(v) for k, v in self.ridx.items()}
        self._copy_base(c)
        return c

    @staticmethod
    def _idx_update(idx, key, row, d):
        if key is None:
            return
        slot = idx.get(key)
        if slot is None:
            if d > 0:
                idx[key] = Counter({row: d})
            return
        slot[row] += d
        if slot[row] == 0:
            del slot[row]
        if not slot:
            del idx[key]

    def _matching(self, row, side):
        """枚举对侧袋中与 row 的连接谓词为 TRUE 的行（完整谓词最终确认）。"""
        if self.equi is not None:
            pos = self.lpos if side == "L" else self.rpos
            idx = self.ridx if side == "L" else self.lidx
            key = _join_hash_key(row, pos)
            if key is None:
                return ()
            cand = idx.get(key)
            if not cand:
                return ()
            out = []
            for other in cand:
                joined = row + other if side == "L" else other + row
                if self.pred(joined) is True:
                    out.append(other)
            return out
        opp = self.R if side == "L" else self.L
        return [
            other for other in opp
            if self.pred((row + other) if side == "L" else (other + row)) is True
        ]

    def _match_mult(self, lrow):
        """M(l)：左行单个副本在当前右袋中的 TRUE 匹配重数。"""
        return sum(self.R[r] for r in self._matching(lrow, "L"))

    def _insert_left(self, row, d):
        key = _join_hash_key(row, self.lpos) if self.equi is not None else None
        self.L[row] += d
        if self.L[row] == 0:
            del self.L[row]
        self._idx_update(self.lidx, key, row, d)
        out = Counter()
        for r in self._matching(row, "L"):
            out[row + r] += d * self.R[r]
        if self.outer and self._match_mult(row) == 0:
            out[row + self.nulls] += d
        return out

    def _insert_right(self, row, d):
        key = _join_hash_key(row, self.rpos) if self.equi is not None else None
        self.R[row] += d
        if self.R[row] == 0:
            del self.R[row]
        self._idx_update(self.ridx, key, row, d)
        out = Counter()
        matched_l = self._matching(row, "R")
        for l in matched_l:
            out[l + row] += self.L[l] * d
        if self.outer:
            for l in matched_l:
                m_after = self._match_mult(l)
                m_before = m_after - d  # 变化行 r 对 M(l) 的贡献恰为 d
                pad = (1 if m_after == 0 else 0) - (1 if m_before == 0 else 0)
                if pad:
                    out[l + self.nulls] += self.L[l] * pad
        return out

    def push(self, child, deltas, ctx):
        is_left = child is self.children[0]
        out = Counter()
        for row, d in deltas.items():
            if is_left:
                out.update(self._insert_left(row, d))
                self.touched.add("L:" + _key_repr(
                    _join_hash_key(row, self.lpos) if self.equi is not None else row))
            else:
                out.update(self._insert_right(row, d))
                self.touched.add("R:" + _key_repr(
                    _join_hash_key(row, self.rpos) if self.equi is not None else row))
        self.bag.update(out)
        _drop_zero(self.bag, out)
        return out

    def export_state(self):
        return {
            "bag": _bag_json(self.bag),
            "L": _bag_json(self.L),
            "R": _bag_json(self.R),
        }

    def import_state(self, data):
        self.L, self.R = _bag_from_json(data["L"]), _bag_from_json(data["R"])
        self.bag = _bag_from_json(data["bag"])
        self.lidx, self.ridx = {}, {}
        if self.equi is not None:
            for row, mult in self.L.items():
                self._idx_update(self.lidx, _join_hash_key(row, self.lpos), row, mult)
            for row, mult in self.R.items():
                self._idx_update(self.ridx, _join_hash_key(row, self.rpos), row, mult)


class _GroupBy(_Node):
    """分组聚合。状态：groups[dkey] = (原始键 k, Counter[measure])。"""

    def __init__(self, compiled, child, raw):
        super().__init__(compiled, (child,), raw)
        self.spec = compiled.detail["group_spec"]
        self.groups = {}

    def clone(self):
        c = _GroupBy(self.compiled, self.children[0], self.raw)
        c.groups = {dk: (k, Counter(v)) for dk, (k, v) in self.groups.items()}
        self._copy_base(c)
        return c

    def _result_row(self, k, measures):
        return k + tuple(compute_aggs(measures, self.spec["spec_funcs"]))

    def push(self, child, deltas, ctx):
        out = Counter()
        for row, d in deltas.items():
            k = tuple(f(row) for f in self.spec["key_funcs"])
            m = _measure_row(row, self.spec["measure_funcs"])
            dk = distinct_key(k)  # IS NOT DISTINCT FROM：NULL 同组、bool 独立
            entry = self.groups.get(dk)
            old_row = self._result_row(k, entry[1]) if entry is not None else None
            if entry is None:
                bucket = Counter()
                entry = (k, bucket)
                self.groups[dk] = entry
            else:
                bucket = entry[1]
            bucket[m] += d
            if bucket[m] == 0:
                del bucket[m]
            if bucket:
                new_row = self._result_row(k, bucket)
            else:
                del self.groups[dk]
                new_row = None
            self.touched.add("g:" + _key_repr(k))
            if old_row is not None:
                out[old_row] -= 1
            if new_row is not None:
                out[new_row] += 1
        self.bag.update(out)
        _drop_zero(self.bag, out)
        return out

    def export_state(self):
        return {
            "bag": _bag_json(self.bag),
            "groups": [
                [list(k), [[list(m), mult] for m, mult in sorted(g.items())]]
                for dk, (k, g) in sorted(self.groups.items(),
                                         key=lambda kv: _sort_rows_key(kv[1][0]))
            ],
        }

    def import_state(self, data):
        self.bag = _bag_from_json(data["bag"])
        self.groups = {}
        for k, g in data["groups"]:
            kt = tuple(k)
            self.groups[distinct_key(kt)] = (
                kt, Counter({tuple(m): mult for m, mult in g}))


class _Window(_Node):
    """事件时间滚动窗口聚合。状态：buckets[(start, dkey)] = (k, Counter[measure])。"""

    def __init__(self, compiled, child, raw):
        super().__init__(compiled, (child,), raw)
        det = compiled.detail
        self.spec = det["group_spec"]
        self.time_f = det["time_f"]
        self.width = det["width"]
        self.buckets = {}
        self.closed_starts = set()
        # start -> 该起点下的桶键集合；外加有序起点表，使关闭窗口
        # 只触碰被关闭起点的桶，不扫描其他窗口/无关组。
        self.by_start = {}
        self.sorted_starts = []

    def clone(self):
        c = _Window(self.compiled, self.children[0], self.raw)
        c.buckets = {bk: (k, Counter(v)) for bk, (k, v) in self.buckets.items()}
        c.closed_starts = set(self.closed_starts)
        c.by_start = {s: set(keys) for s, keys in self.by_start.items()}
        c.sorted_starts = list(self.sorted_starts)
        self._copy_base(c)
        return c

    def _result_row(self, start, k, measures):
        vals = tuple(compute_aggs(measures, self.spec["spec_funcs"]))
        return k + (start, start + self.width) + vals

    def _event_time(self, row):
        v = self.time_f(row)
        # 增量入口把事件时间缺失/类型错也视为该条更新不可接受（整批原子拒绝）。
        if v is None or isinstance(v, bool) or not isinstance(v, int):
            raise LateDataError(
                f"[node#{self.num}](window): 事件时间表达式必须求出整数逻辑时钟，"
                f"得到 {type(v).__name__}={v!r}；该更新被拒绝且不改状态",
                reason="missing_timestamp",
            )
        return v

    def push(self, child, deltas, ctx):
        out = Counter()
        wm = ctx["watermark"]
        for row, d in deltas.items():
            t = self._event_time(row)
            if wm is not None and t < wm - ctx["allowed_lateness"]:                raise LateDataError(
                    f"[node#{self.num}](window): 事件时间 {t} 早于保留下限 "
                    f"watermark {wm} - lateness {ctx['allowed_lateness']} "
                    f"= {wm - ctx['allowed_lateness']}；更新被拒绝且不改状态"
                )
            start = (t // self.width) * self.width
            if start in self.closed_starts:
                raise LateDataError(
                    f"[node#{self.num}](window): 窗口 [{start}, {start + self.width}) "
                    f"已关闭；事件时间 {t} 的更新被拒绝且不改状态"
                )
            k = tuple(f(row) for f in self.spec["key_funcs"])
            m = _measure_row(row, self.spec["measure_funcs"])
            bk = (start, distinct_key(k))
            entry = self.buckets.get(bk)
            old_row = self._result_row(start, k, entry[1]) if entry is not None else None
            is_new_bucket = entry is None
            if is_new_bucket:
                bucket = Counter()
                entry = (k, bucket)
                self.buckets[bk] = entry
                keys = self.by_start.setdefault(start, set())
                if not keys:
                    bisect.insort(self.sorted_starts, start)
                keys.add(bk)
            else:
                bucket = entry[1]
            bucket[m] += d
            if bucket[m] == 0:
                del bucket[m]
            if bucket:
                new_row = self._result_row(start, k, bucket)
            else:
                del self.buckets[bk]
                keys = self.by_start.get(start)
                if keys is not None:
                    keys.discard(bk)
                    if not keys:
                        del self.by_start[start]
                        self.sorted_starts.remove(start)
                new_row = None
            self.touched.add("w:" + _key_repr([start, k]))
            if old_row is not None:
                out[old_row] -= 1
            if new_row is not None:
                out[new_row] += 1
        self.bag.update(out)
        _drop_zero(self.bag, out)
        return out

    def advance_watermark(self, wm, lateness):
        """关闭所有 s + width <= wm - lateness 的窗口；返回新关闭的 start 列表。

        用有序起点索引只枚举到期窗口本身，不扫描其他窗口或无关分组。
        """
        cutoff = wm - lateness
        to_close = []
        for s in list(self.sorted_starts):
            if s + self.width <= cutoff:
                to_close.append(s)
            else:
                break  # 起点有序，后面的都不会到期
        for s in to_close:
            for bk in self.by_start.pop(s, ()):
                del self.buckets[bk]
                self.touched.add("close:" + _key_repr([s, bk[1]]))
            self.sorted_starts.remove(s)
            self.closed_starts.add(s)
        return to_close

    def export_state(self):
        return {
            "bag": _bag_json(self.bag),
            "closed_starts": sorted(self.closed_starts),
            "buckets": [
                [start, list(k), [[list(m), mult] for m, mult in sorted(g.items())]]
                for (start, dk), (k, g) in sorted(
                    self.buckets.items(),
                    key=lambda kv: (kv[0][0], _sort_rows_key(kv[1][0])))
            ],
        }

    def import_state(self, data):
        self.bag = _bag_from_json(data["bag"])
        self.closed_starts = set(data["closed_starts"])
        self.buckets = {}
        self.by_start = {}
        self.sorted_starts = []
        for start, k, g in data["buckets"]:
            kt = tuple(k)
            bk = (start, distinct_key(kt))
            self.buckets[bk] = (kt, Counter({tuple(m): mult for m, mult in g}))
            keys = self.by_start.setdefault(start, set())
            if not keys:
                bisect.insort(self.sorted_starts, start)
            keys.add(bk)


# ---------------------------------------------------------------- 执行器

def _preorder_raw(plan):
    """与 engine.compile_plan 完全相同的深度优先先序编号 -> 原始节点 dict。"""
    raw_map = {}

    def walk(node):
        num = len(raw_map)
        raw_map[num] = node
        for key in _CHILD_KEYS[node["op"]]:
            walk(node[key])

    walk(plan)
    return raw_map


class IncrementalExecutor:
    """增删流增量解释器。

    :param plan: 与 :func:`execute` 同构的计划树；支持 scan/filter/project/
        inner_join/left_join/union_all/distinct/group_by/window。
    :param tables: 基表模式与可选初始行，
        ``{"t": {"columns": ["id", "v"], "rows": []}}``；初始行静默装载，
        不发布变更，后续更新走 :meth:`apply_batch`。
    :param allowed_lateness: 非负整数，事件时间允许的迟到阈值（逻辑时钟）。
    """

    def __init__(self, plan, tables, *, allowed_lateness=0):
        if (not isinstance(allowed_lateness, int) or isinstance(allowed_lateness, bool)
                or allowed_lateness < 0):
            raise PlanError("[stream]: allowed_lateness 必须是非负整数")
        norm = validate_tables(tables, limit=STREAM_MAX_ROWS_PER_TABLE)
        root = compile_plan(plan, norm)
        self.plan = plan
        self._raw_map = _preorder_raw(plan)
        self.nodes_by_num = {}
        self.root = self._build(root)
        self.allowed_lateness = allowed_lateness
        self.watermark = None
        self.seq = 0
        self.last_touched = {}
        self.last_closed = {}
        self._load_initial(norm)

    # ----- 构造与初始装载 -----
    def _build(self, compiled):
        op = compiled.op
        raw = self._raw_map[compiled.num]
        children = [self._build(c) for c in compiled.children]
        if op == "scan":
            node = _Scan(compiled, raw)
        elif op == "filter":
            node = _Filter(compiled, children[0], raw)
        elif op == "project":
            node = _Project(compiled, children[0], raw)
        elif op in ("inner_join", "left_join"):
            node = _Join(compiled, children[0], children[1], raw)
        elif op == "union_all":
            node = _Union(compiled, children[0], children[1], raw)
        elif op == "distinct":
            node = _Distinct(compiled, children[0], raw)
        elif op == "group_by":
            node = _GroupBy(compiled, children[0], raw)
        elif op == "window":
            node = _Window(compiled, children[0], raw)
        else:
            raise PlanError(f"[stream]: 暂不支持增量节点 op={op!r}")
        self.nodes_by_num[compiled.num] = node
        return node

    def _load_initial(self, norm):
        """初始行静默装载：scan 赋保留 ID，其余节点自底向上重建内部状态。"""
        for node in self.nodes_by_num.values():
            if isinstance(node, _Scan):
                _, rows = norm[node.table]
                for i, row in enumerate(rows):
                    node.seed(f"__init__:{node.table}:{i}", row)
        # 自底向上（编译先序编号中子节点必大于父节点，逆序即后序）。
        ctx = {"watermark": None, "allowed_lateness": self.allowed_lateness}
        for num in sorted(self.nodes_by_num, reverse=True):
            node = self.nodes_by_num[num]
            if isinstance(node, _Scan):
                continue
            if isinstance(node, _Join):
                # 先左后右：右侧插入时按 0→1 撤回补 NULL 行，输出一次成型。
                for row, mult in node.children[0].bag.items():
                    node.bag.update(node._insert_left(row, mult))
                for row, mult in node.children[1].bag.items():
                    node.bag.update(node._insert_right(row, mult))
            elif isinstance(node, _Union):
                node.push(node.children[0], Counter(node.children[0].bag), ctx)
                node.push(node.children[1], Counter(node.children[1].bag), ctx)
            else:
                # filter/project/distinct/group_by/window：push 内部已维护 bag。
                node.push(node.children[0], Counter(node.children[0].bag), ctx)

    # ----- 批次 -----
    def apply_batch(self, batch):
        """提交一个批次；成功发布一次净变更，失败抛异常且状态不变。

        ``batch``::

            {"changes": [{"table": "orders", "op": "+"|"-", "id": "r1",
                          "row": [1, "A", 10]}, ...],
             "watermark": 7}   # 可选；整数逻辑时钟，单调不减

        :返回: ``{"seq", "watermark", "changes", "touched", "closed"}``。
            ``changes`` 为净变更（撤回为负 delta），整批只发布这一次；
            ``touched`` 记录每个节点本批实际触碰的状态键；
            ``closed`` 记录窗口节点本批新关闭的窗口起点。
        """
        self._validate_batch_shape(batch)
        changes = batch["changes"]
        new_wm = batch.get("watermark")
        if new_wm is not None:
            if isinstance(new_wm, bool) or not isinstance(new_wm, int):
                raise WatermarkError("[stream]: watermark 必须是整数（逻辑时钟）")
            if self.watermark is not None and new_wm < self.watermark:
                raise WatermarkError(
                    f"[stream]: 水位不能倒退：{new_wm} < {self.watermark}"
                )

        # 影子执行：整批在副本上算完，任何异常都丢弃副本 -> 原子发布。
        shadow = {num: node.clone() for num, node in self.nodes_by_num.items()}
        for node in shadow.values():
            node.children = tuple(shadow[c.num] for c in node.children)
        shadow_root = shadow[self.root.num]
        scan_by_table = {
            node.table: node for node in shadow.values() if isinstance(node, _Scan)
        }
        parent_map = {}
        for node in shadow.values():
            for ch in node.children:
                parent_map[ch.num] = (node, ch)

        root_net = Counter()
        # 固定契约：批次的水位与数据**原子生效**——同批 changes 按提交后水位
        # （new_wm）判定迟到，随后再关闭窗口。不给 watermark 字段时沿用当前水位。
        ctx = {"watermark": new_wm if new_wm is not None else self.watermark,
               "allowed_lateness": self.allowed_lateness}

        for rec in changes:
            tname, op, rid = rec["table"], rec["op"], rec["id"]
            scan = scan_by_table.get(tname)
            if scan is None:
                raise ChangeError(f"[stream]: 变更引用的表 {tname!r} 不在计划中")
            if op not in ("+", "-"):
                raise ChangeError(f"[table:{tname}] id={rid!r}: op 必须是 '+' 或 '-'")
            delta = scan.apply_change(op, rid, rec.get("row"))
            cur = scan
            while cur.num in parent_map:
                parent, as_child = parent_map[cur.num]
                delta = parent.push(as_child, delta, ctx)
                cur = parent
            root_net.update(delta)
        _drop_zero(root_net, list(root_net))

        closed = {}
        if new_wm is not None:
            for node in shadow.values():
                if isinstance(node, _Window):
                    starts = node.advance_watermark(new_wm, self.allowed_lateness)
                    if starts:
                        closed[node.num] = starts

        # 提交：影子转正，整批结果只发布这一次。
        self.nodes_by_num = shadow
        for node in shadow.values():  # children 已在 shadow 内重指
            pass
        self.root = shadow_root
        if new_wm is not None:
            self.watermark = new_wm
        self.seq += 1
        published = self.root.root_deltas_to_changes(root_net)
        self.last_touched = {
            num: sorted(node.touched)
            for num, node in shadow.items() if node.touched
        }
        self.last_closed = {num: list(starts) for num, starts in closed.items()}
        return {
            "seq": self.seq,
            "watermark": self.watermark,
            "changes": published,
            "touched": self.last_touched,
            "closed": self.last_closed,
        }

    def _validate_batch_shape(self, batch):
        if not isinstance(batch, dict):
            raise ChangeError("[stream]: batch 必须是 dict")
        changes = batch.get("changes")
        if not isinstance(changes, list):
            raise ChangeError("[stream]: batch['changes'] 必须是 list")
        if len(changes) > STREAM_MAX_BATCH_CHANGES:
            raise ChangeError(
                f"[stream]: 单批变更数 {len(changes)} 超过上限 "
                f"{STREAM_MAX_BATCH_CHANGES}"
            )
        for i, rec in enumerate(changes):
            if not isinstance(rec, dict):
                raise ChangeError(f"[stream] changes#{i}: 必须是 dict")
            for field in ("table", "op", "id"):
                if field not in rec:
                    raise ChangeError(f"[stream] changes#{i}: 缺少字段 {field!r}")
            extra = set(rec) - {"table", "op", "id", "row"}
            if extra:
                raise ChangeError(f"[stream] changes#{i}: 未知字段 {sorted(extra)}")
        extra = set(batch) - {"changes", "watermark"}
        if extra:
            raise ChangeError(f"[stream]: batch 含未知字段 {sorted(extra)}")

    # ----- 结果与快照 -----
    def current_result(self):
        """当前根袋的确定顺序快照（列定义同批量 execute()）。"""
        ordered = []
        for row, mult in sorted(self.root.bag.items(),
                                key=lambda kv: _sort_rows_key(kv[0])):
            ordered.extend([list(row)] * mult)
        return {
            "columns": [{"source": s, "name": c} for s, c in self.root.schema],
            "rows": ordered,
            "watermark": self.watermark,
        }

    def snapshot(self):
        """导出可 JSON 序列化的确定性快照。"""
        return {
            "version": SNAPSHOT_VERSION,
            "watermark": self.watermark,
            "allowed_lateness": self.allowed_lateness,
            "seq": self.seq,
            "nodes": {
                str(num): node.export_state()
                for num, node in sorted(self.nodes_by_num.items())
            },
        }

    def restore(self, snapshot):
        """校验快照并返回同计划、同状态的新执行器；非法快照抛 SnapshotError。"""
        self._check_snapshot(snapshot)
        ex = IncrementalExecutor.__new__(IncrementalExecutor)
        ex.plan = self.plan
        ex._raw_map = self._raw_map
        ex.allowed_lateness = snapshot["allowed_lateness"]
        ex.watermark = snapshot["watermark"]
        ex.seq = snapshot["seq"]
        ex.last_touched = {}
        ex.last_closed = {}
        ex.nodes_by_num = {}
        ex.root = ex._build(self.root.compiled)
        for num_str, data in snapshot["nodes"].items():
            ex.nodes_by_num[int(num_str)].import_state(data)
        return ex

    def _check_snapshot(self, snapshot):
        if not isinstance(snapshot, dict):
            raise SnapshotError("[snapshot]: 必须是 dict")
        if snapshot.get("version") != SNAPSHOT_VERSION:
            raise SnapshotError(
                f"[snapshot]: 不支持的版本 {snapshot.get('version')!r}，"
                f"本构建版本为 {SNAPSHOT_VERSION}"
            )
        for field in ("watermark", "allowed_lateness", "seq", "nodes"):
            if field not in snapshot:
                raise SnapshotError(f"[snapshot]: 缺少字段 {field!r}")
        if set(snapshot["nodes"]) != {str(n) for n in self.nodes_by_num}:
            raise SnapshotError("[snapshot]: 节点集合与当前计划不一致")
