"""三值逻辑表达式的编译与求值。

表达式是纯 JSON 风格字典，判别字段为 ``kind``：

=========== =========================================================
kind        形态
=========== =========================================================
``lit``     ``{"kind": "lit", "value": 1}``
``col``     ``{"kind": "col", "table": "s", "column": "x"}``
``cmp``     ``{"kind": "cmp", "op": "=", "left": E, "right": E}``
``and``     ``{"kind": "and", "left": E, "right": E}``
``or``      ``{"kind": "or", "left": E, "right": E}``
``not``     ``{"kind": "not", "expr": E}``
``is_null`` ``{"kind": "is_null", "expr": E}``
``coalesce````{"kind": "coalesce", "args": [E, ...]}``
``param``   ``{"kind": "param", "name": "p"}``（仅相关子计划内部使用）
=========== =========================================================

``param`` 不在用户计划文本中出现：相关 EXISTS/NOT EXISTS 节点的
``correlations`` 绑定由编译器改写为参数表达式，参数值来自外层行，
由执行上下文逐外层行提供。

编译产物是 ``row -> value`` 的闭包；所有结构错误在编译期抛出
（此时还没有任何行被处理），求值期只可能抛出类型语义错误。
"""

from __future__ import annotations

import math

from .errors import ExpressionError, SchemaError

CMP_OPS = ("=", "<>", "<", "<=", ">", ">=")
MAX_EXPR_DEPTH = 200

_ALLOWED_KEYS = {
    "lit": {"kind", "value"},
    "col": {"kind", "table", "column"},
    "cmp": {"kind", "op", "left", "right"},
    "and": {"kind", "left", "right"},
    "or": {"kind", "left", "right"},
    "not": {"kind", "expr"},
    "is_null": {"kind", "expr"},
    "coalesce": {"kind", "args"},
    "param": {"kind", "name"},
}


def _check_keys(raw, allowed, where):
    extra = set(raw) - allowed
    if extra:
        raise ExpressionError(
            f"{where}: 表达式含未知字段 {sorted(extra)}（允许 {sorted(allowed)}）"
        )
    missing = allowed - {"kind"} - set(raw)
    if missing:
        raise ExpressionError(f"{where}: 表达式缺少必填字段 {sorted(missing)}")


def _validate_literal(value, where):
    if value is None or isinstance(value, (bool, str, int)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise ExpressionError(f"{where}: 字面量 float 必须有限，拒绝 NaN/Infinity")
        return value
    raise ExpressionError(
        f"{where}: 不支持的字面量类型 {type(value).__name__}（允许 None/bool/int/float/str）"
    )


def compile_expr(raw, col_index, where, depth=0, params=None):
    """把表达式字典编译为 ``(row, frame) -> value``。

    ``col_index`` 为 ``(source_id, column_name) -> 列下标`` 的映射。
    ``where`` 是错误定位前缀（如 ``"[node#2/expr]"``）。

    ``params`` 描述当前相关作用域可见的外层参数（相关 EXISTS 绑定）：

    * ``params["cols"]``: ``{(inner_alias, inner_name): param_name}``，
      子计划中对这些限定名的 ``col`` 引用改为从执行帧取值；
    * ``params["names"]``: ``{param_name}``，供内部 ``param`` 表达式校验。

    作用域是词法且显式的：不在 ``col_index`` 也不在 ``params`` 中的列
    一律抛 :class:`SchemaError`，绝不静默当作 NULL。
    """
    param_cols = params["cols"] if params else {}
    param_names = params["names"] if params else set()
    if depth > MAX_EXPR_DEPTH:
        raise ExpressionError(f"{where}: 表达式嵌套超过 {MAX_EXPR_DEPTH} 层")
    if not isinstance(raw, dict):
        raise ExpressionError(f"{where}: 表达式必须是 dict，得到 {type(raw).__name__}")
    kind = raw.get("kind")
    if not isinstance(kind, str):
        raise ExpressionError(f"{where}: 表达式缺少字符串字段 'kind'")
    try:
        allowed = _ALLOWED_KEYS[kind]
    except KeyError:
        raise ExpressionError(
            f"{where}: 未知表达式 kind={kind!r}（允许 {sorted(_ALLOWED_KEYS)}）"
        ) from None
    _check_keys(raw, allowed, where)

    if kind == "lit":
        value = _validate_literal(raw["value"], where)
        return lambda row, frame: value

    if kind == "param":
        pname = raw["name"]
        if not isinstance(pname, str) or not pname:
            raise ExpressionError(f"{where}: param.name 必须是非空字符串")
        if pname not in param_names:
            raise SchemaError(
                f"{where}: 内部参数 {pname!r} 在当前相关作用域中不存在；"
                f"可见参数：{sorted(param_names) or '<无>'}"
            )

        def eval_param(row, frame):
            try:
                return frame[pname]
            except KeyError:
                # 理论上编译期已保证名字合法、帧中必有此键；
                # 守卫是为了把任何内部不一致也归为可定位的 QueryError，
                # 而不是裸 KeyError。
                raise SchemaError(
                    f"{where}: 相关参数 {pname!r} 在执行帧中缺失"
                ) from None

        return eval_param

    if kind == "col":
        source, name = raw["table"], raw["column"]
        if not isinstance(source, str):
            raise ExpressionError(f"{where}: col.table 必须是字符串（投影输出列默认 ''）")
        if not isinstance(name, str) or not name:
            raise ExpressionError(f"{where}: col.column 必须是非空字符串")
        qname = (source, name)
        if qname in param_cols:
            if qname in col_index:
                # 不允许相关绑定名遮蔽子计划自身的实际列，避免引用歧义。
                raise SchemaError(
                    f"{where}: 相关绑定 {source}.{name} 与子计划自身的实际列同名；"
                    f"请在 correlations 中换一个 inner_alias/inner_name"
                )
            pname = param_cols[qname]
            return lambda row, frame: frame[pname]
        idx = col_index.get(qname)
        if idx is None:
            available = ", ".join(f"{s}.{c}" for s, c in col_index) or "<无列>"
            declared = ", ".join(f"{s}.{c}" for s, c in param_cols) or "<无相关绑定>"
            raise SchemaError(
                f"{where}: 未知列 {source}.{name}；当前可用列：{available}；"
                f"本作用域已声明的相关绑定：{declared}"
            )
        return lambda row, frame: row[idx]

    if kind in ("and", "or", "cmp"):
        lf = compile_expr(raw["left"], col_index, where, depth + 1, params)
        rf = compile_expr(raw["right"], col_index, where, depth + 1, params)
        if kind == "and":
            return lambda row, frame: _sql_and(lf(row, frame), rf(row, frame), where)
        if kind == "or":
            return lambda row, frame: _sql_or(lf(row, frame), rf(row, frame), where)
        op = raw["op"]
        if op not in CMP_OPS:
            raise ExpressionError(f"{where}: 未知比较符 {op!r}（允许 {CMP_OPS}）")
        return lambda row, frame: _compare(op, lf(row, frame), rf(row, frame), where)

    if kind in ("not", "is_null"):
        inner = compile_expr(raw["expr"], col_index, where, depth + 1, params)
        if kind == "not":
            return lambda row, frame: _sql_not(inner(row, frame), where)
        return lambda row, frame: inner(row, frame) is None

    # coalesce
    args = raw["args"]
    if not isinstance(args, list) or not args:
        raise ExpressionError(f"{where}: coalesce.args 必须是非空 list")
    compiled = [compile_expr(a, col_index, where, depth + 1, params) for a in args]

    def eval_coalesce(row, frame):
        for f in compiled:
            v = f(row, frame)
            if v is not None:
                return v
        return None

    return eval_coalesce


def _as_truth(v, where):
    """把表达式值约束为三值逻辑量：TRUE/FALSE/UNKNOWN(NULL)。"""
    if v is None or isinstance(v, bool):
        return v
    raise ExpressionError(
        f"{where}: 逻辑运算只接受 bool 或 NULL(UNKNOWN)，得到 {type(v).__name__}={v!r}"
    )


def _sql_and(a, b, where):
    # FALSE < UNKNOWN < TRUE；AND 取小。FALSE AND UNKNOWN = FALSE。
    order = {False: 0, None: 1, True: 2}
    a, b = _as_truth(a, where), _as_truth(b, where)
    return min((a, b), key=lambda x: order[x])


def _sql_or(a, b, where):
    # OR 取大；TRUE OR UNKNOWN = TRUE。
    order = {False: 0, None: 1, True: 2}
    a, b = _as_truth(a, where), _as_truth(b, where)
    return max((a, b), key=lambda x: order[x])


def _sql_not(a, where):
    a = _as_truth(a, where)
    if a is None:
        return None
    return not a


def _compare(op, a, b, where):
    """SQL 风格比较：任一操作数为 NULL -> UNKNOWN；不做隐式跨类型转换。"""
    if a is None or b is None:
        return None
    a_bool, b_bool = isinstance(a, bool), isinstance(b, bool)
    if a_bool or b_bool:
        if not (a_bool and b_bool):
            raise ExpressionError(
                f"{where}: bool 与其他类型不可比较（bool 不视作 int）：{a!r} vs {b!r}"
            )
        if op in ("=", "<>"):
            return (a == b) if op == "=" else (a != b)
        raise ExpressionError(f"{where}: bool 只支持 = / <>，得到 {op}")
    a_num, b_num = isinstance(a, (int, float)), isinstance(b, (int, float))
    if a_num != b_num:
        raise ExpressionError(
            f"{where}: 跨类型比较被拒绝（str 与数值不可隐式转换）：{a!r} vs {b!r}"
        )
    if not (a_num or isinstance(a, str)):
        # 数据入口已保证不会走到这里，保留为防御性分支。
        raise ExpressionError(f"{where}: 不可比较的类型 {type(a).__name__}")
    if op == "=":
        return a == b
    if op == "<>":
        return a != b
    return getattr(a, {"<": "__lt__", "<=": "__le__", ">": "__gt__", ">=": "__ge__"}[op])(b)
