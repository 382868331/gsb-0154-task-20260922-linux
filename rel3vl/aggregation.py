"""分组聚合的共享累加器（批量解释器与增量解释器共用）。

两边走同一份状态更新代码，保证批量对照与增量结果的聚合口径严格一致。

聚合函数（SQL 袋语义）：

* ``count`` —— ``arg`` 为 ``None`` 时计数所有行；否则只计数 arg 非 NULL 的行；
* ``sum``   —— 忽略 NULL；无行（或全 NULL）时为 NULL；只接受 int/float
  （bool 按旧契约是独立类型，**不**当作数值，参与 sum 报错）；
  全 int 得 int，混入 float 得 float；
* ``avg``   —— sum / 非 NULL 行数，按 Python ``/`` 得 float（如 6/2 = 3.0），
  无行时 NULL；
* ``min``/``max`` —— 忽略 NULL；无可比较值时 NULL；比较口径与 3VL 比较
  表达式一致（int/float 可混、str 独立、bool 不做序比较、跨类型拒绝）。

空 ``groups`` 表示全局聚合：即使输入为空袋也恒输出恰好一个组
（count=0，其余为 NULL）。
"""

from __future__ import annotations

from collections import Counter

from .errors import ExpressionError

FUNC_COUNT = "count"
FUNC_SUM = "sum"
FUNC_AVG = "avg"
FUNC_MIN = "min"
FUNC_MAX = "max"
AGG_FUNCS = (FUNC_COUNT, FUNC_SUM, FUNC_AVG, FUNC_MIN, FUNC_MAX)


def _numeric(v, where):
    if isinstance(v, bool):
        raise ExpressionError(
            f"{where}: bool 是独立类型，不参与数值聚合（sum/avg），got {v!r}"
        )
    if isinstance(v, (int, float)):
        return v
    raise ExpressionError(
        f"{where}: sum/avg 只接受 int/float，得到 {type(v).__name__}={v!r}"
    )


def _extreme(values, want_max, where):
    """在一组非 NULL 值上取 min/max，比较口径对齐表达式比较。"""
    it = iter(values)
    try:
        best = next(it)
    except StopIteration:
        return None
    for v in it:
        if isinstance(best, bool) or isinstance(v, bool):
            if not (isinstance(best, bool) and isinstance(v, bool)):
                raise ExpressionError(
                    f"{where}: min/max 不允许 bool 与其他类型混组：{best!r} vs {v!r}"
                )
            pick = best
        elif isinstance(best, (int, float)) and isinstance(v, (int, float)):
            pick = v if (v > best if want_max else v < best) else best
        elif type(best) is type(v) and isinstance(best, str):
            pick = v if (v > best if want_max else v < best) else best
        else:
            raise ExpressionError(
                f"{where}: min/max 跨类型混组被拒绝：{best!r} ({type(best).__name__}) vs "
                f"{v!r} ({type(v).__name__})"
            )
        best = pick
    return best


class AggAccumulator:
    """单个组的全部聚合状态。

    :param specs: ``[(func, has_arg), ...]``，决定 :meth:`result` 的列序。
    """

    __slots__ = ("specs", "where", "count_all", "states")

    def __init__(self, specs, where="[aggregate]"):
        self.specs = specs
        self.where = where
        self.count_all = 0
        # 每个 agg 一份状态：count->int; sum->[total, nonnull];
        # avg->[sum, nonnull]; min/max->Counter。
        self.states = []
        for func, _ in specs:
            if func == FUNC_COUNT:
                self.states.append(0)
            elif func in (FUNC_SUM, FUNC_AVG):
                self.states.append([0, 0])
            else:
                self.states.append(Counter())

    def apply(self, arg_values, mult):
        """加入（mult>0）或撤回（mult<0）重数为 abs(mult) 的同一行参数。"""
        self.count_all += mult
        for idx, (func, has_arg) in enumerate(self.specs):
            v = arg_values[idx]
            st = self.states[idx]
            if func == FUNC_COUNT:
                if (not has_arg) or v is not None:
                    self.states[idx] = st + mult
            elif func in (FUNC_SUM, FUNC_AVG):
                if v is not None:
                    num = _numeric(v, self.where)
                    st[0] += num * mult
                    st[1] += mult
            else:
                if v is not None:
                    # 撤回时 Counter 可能临时为 0/负；批量重放与增量更新都保证
                    # 最终不会为负（只撤回确实存在的行）。
                    st[v] += mult

    def result(self):
        out = []
        for (func, _), st in zip(self.specs, self.states):
            if func == FUNC_COUNT:
                out.append(st)
            elif func == FUNC_SUM:
                out.append(st[0] if st[1] != 0 else None)
            elif func == FUNC_AVG:
                out.append(st[0] / st[1] if st[1] != 0 else None)
            else:
                vals = [v for v, n in st.items() if n > 0]
                out.append(_extreme(iter(vals), func == FUNC_MAX, self.where))
        return tuple(out)


class GroupTable:
    """按分组键维护的全部 :class:`AggAccumulator`（袋重数可为多）。

    内部以可哈希的 ``IS NOT DISTINCT FROM`` 键（``gkey``）索引，
    同时保留原始标量键用于结果输出（bool/int 不混组、1 与 1.0 同组）。
    """

    def __init__(self, specs, global_group=False, where="[aggregate]"):
        self.specs = specs
        self.global_group = global_group
        self.where = where
        # gkey -> [raw_key, AggAccumulator]
        self.groups: dict = {}

    def update(self, gkey, raw_key, arg_values, mult):
        slot = self.groups.get(gkey)
        if slot is None:
            slot = [raw_key, AggAccumulator(self.specs, self.where)]
            self.groups[gkey] = slot
        acc = slot[1]
        acc.apply(arg_values, mult)
        if acc.count_all == 0 and not (self.global_group and gkey == ()):
            # 组内最后一行被撤回：组消失（全局空组保留，输出零行）。
            del self.groups[gkey]

    def result_rows(self, gkey_order):
        """按给定 gkey 首现序输出 (raw_key + agg_values)。"""
        rows = []
        for gkey in gkey_order:
            slot = self.groups.get(gkey)
            if slot is not None:
                rows.append(tuple(slot[0]) + slot[1].result())
        return rows

    def result_row(self, gkey, raw_key):
        slot = self.groups.get(gkey)
        if slot is None:
            return None
        return tuple(raw_key) + slot[1].result()
