"""分组聚合函数（批量解释器与增量解释器共用，保证两者口径完全一致）。

支持的聚合函数（袋语义，重复行按重数计入）：

=================== ==========================================================
func                语义
=================== ==========================================================
``count_star``      组内行数（含全 NULL 行；COUNT(*)）
``count``           对表达式非 NULL 的行计数（COUNT(expr)）
``sum``             表达式非 NULL 值之和；全 NULL / 空组跳过 → 结果 NULL
``avg``             非 NULL 值 ``sum / count`` 的 IEEE-754 双精度浮点除法；
                    无非 NULL 值时为 NULL
``min`` / ``max``   非 NULL 值的最小/最大值；全部为 NULL 时为 NULL
=================== ==========================================================

被聚合表达式在编译期被限定为 ``None | int``（bool 拒绝）：
整数求和在 Python 中任意精度、精确无舍入；``avg`` 是唯一引入一次
双精度除法的地方，误差口径为 IEEE-754 binary64 的单次 ``int / int -> float``
true division，不设容差、不做重复相加（不存在累加顺序问题）。

空组（GROUP BY 中没有任何行的键）按 SQL 语义**不输出**。
"""

from __future__ import annotations

from collections import Counter

from .errors import ExpressionError

AGG_FUNCS = ("count_star", "count", "sum", "avg", "min", "max")
FUNC_NEEDS_EXPR = ("count", "sum", "avg", "min", "max")


def validate_measure(value, where):
    """聚合输入值限定为 None/int（bool 按老规则独立、拒绝混入算术）。"""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ExpressionError(
            f"{where}: 聚合输入只接受 int 或 NULL（bool 不参与算术），"
            f"得到 {type(value).__name__}={value!r}"
        )
    return value


def compute_aggs(measure_counter, spec_funcs):
    """根据组内度量袋计算聚合结果。

    :param measure_counter: ``Counter[tuple]``，组内输入行的投影度量元组袋，
        元组第 i 个分量对应第 i 个需要表达式的聚合；count_star 不占槽位。
    :param spec_funcs: 聚合规格列表，每项为 ``(name, func, expr_index|None)``，
        ``expr_index`` 是度量元组中的下标，count_star 为 ``None``。
    :returns: 与 spec_funcs 等长的结果元组。
    """
    total = sum(measure_counter.values())
    out = []
    for name, func, idx in spec_funcs:
        if func == "count_star":
            out.append(total)
            continue
        # 其余聚合忽略 NULL；measure 已校验为 None/int。
        vals = Counter()
        for tup, mult in measure_counter.items():
            v = tup[idx]
            if v is not None:
                vals[v] += mult
        if func == "count":
            out.append(sum(vals.values()))
        elif func == "sum":
            out.append(sum(v * mult for v, mult in vals.items()) if vals else None)
        elif func == "avg":
            if not vals:
                out.append(None)
            else:
                s = sum(v * mult for v, mult in vals.items())
                n = sum(vals.values())
                out.append(s / n)
        elif func == "min":
            out.append(min(vals) if vals else None)
        elif func == "max":
            out.append(max(vals) if vals else None)
        else:  # pragma: no cover - 编译期已拦截
            raise ExpressionError(f"未知聚合函数 {func!r}（聚合列 {name!r}）")
    return tuple(out)
