"""标量值与三值逻辑 (3VL) 常量。

支持的列值（袋中元组的分量）：

* ``None`` —— SQL 意义上的 NULL（三值逻辑中的 UNKNOWN 来源）；
* ``bool`` —— 注意 ``bool`` 是 ``int`` 的子类，按布尔类型独立处理；
* ``int``  —— 任意精度整数；
* ``float`` —— 必须是有限数，NaN / +Infinity / -Infinity 一律拒绝；
* ``str``  —— Unicode 字符串。

不支持的类型（datetime、bytes、list、dict…）在表数据进入时即被拒绝，
错误可定位到具体表、行、列，而不是等到表达式求值时产生含糊异常。
"""

from __future__ import annotations

import math

from .errors import TableError

NULL = None
TRUE = True
FALSE = False
# 三值逻辑中 UNKNOWN 在谓词层面由 NULL 表示。
UNKNOWN = None

SUPPORTED_SCALAR_TYPES = (bool, int, float, str)
TYPE_NAMES = {bool: "bool", int: "int", float: "float", str: "str"}


def validate_scalar(value, where: str) -> object:
    """校验单个列值；非法时抛 :class:`TableError`。

    ``where`` 是形如 ``"[table:t] row 3 column 'a'"`` 的定位串。
    规则：

    * NaN、+inf、-inf 拒绝（不能用宽容差掩盖，也不允许进入比较）；
    * bool 不会被误当作 int（按 bool 独立接受，消息里也独立标注）；
    * 不支持的类型直接拒绝，不做隐式转换。
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise TableError(f"{where}: float 必须有限，拒绝 NaN/Infinity (got {value!r})")
        return value
    if isinstance(value, str):
        return value
    raise TableError(
        f"{where}: 不支持的标量类型 {type(value).__name__} "
        f"(got {value!r}); 允许 None/bool/int/float(有限)/str"
    )


def _same_typed_value(a, b) -> bool:
    """非 NULL 标量的"不可区分"判定（``IS NOT DISTINCT FROM`` 的非空部分）。"""
    if isinstance(a, bool) or isinstance(b, bool):
        # bool 虽是 int 子类，但按独立类型处理：True 与 1 不可互相替换。
        return type(a) is type(b) and a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        # int/float 按数学值不可区分：1 与 1.0 去重视为相同。
        return a == b
    return type(a) is type(b) and a == b


def not_distinct(a, b) -> bool:
    """``a IS NOT DISTINCT FROM b``：供 DISTINCT 去重使用。

    与比较谓词 ``=`` 的关键区别：两个 NULL 在这里视为相同
    （SQL 标准的去重语义），永不产生 UNKNOWN，只返回 bool。
    """
    if a is None or b is None:
        return a is None and b is None
    return _same_typed_value(a, b)


def rows_not_distinct(row_a, row_b) -> bool:
    """整行的不可区分判定（按列序逐列）。"""
    return len(row_a) == len(row_b) and all(
        not_distinct(a, b) for a, b in zip(row_a, row_b)
    )


_NULL_KEY = object()


def value_key(v):
    """单个标量的可哈希"不可区分"键（:func:`distinct_key` 的单值版本）。

    除供整行去重外，也供相关 EXISTS 的相关键缓存使用：

    * NULL 用模块级唯一哨兵 :data:`_NULL_KEY`，绝不与任何真实标量
      （包括字符串 ``"None"``、数值 ``0`` 等）同键；
    * bool 加 ``"b"`` 类型标签，避免 Python 中 ``1 == True`` 且
      ``hash(1) == hash(True)`` 造成混组；
    * int/float 共用 ``"n"`` 标签，``1`` 与 ``1.0`` 按数学值同键；
    * str 使用独立的 ``"s"`` 标签与数值隔开。
    """
    if v is None:
        return _NULL_KEY
    if isinstance(v, bool):
        return ("b", v)
    if isinstance(v, (int, float)):
        return ("n", v)
    return ("s", v)


def distinct_key(row):
    """整行的可哈希"不可区分"键。

    与 :func:`not_distinct` 同语义，但 O(1) 可哈希，供 DISTINCT 与
    相关 EXISTS 的相关键缓存使用：

    * NULL 共用唯一哨兵（NULL 与 NULL 同组）；
    * bool 加类型标签（防止 Python 中 ``1`` 与 ``True`` 哈希相等而混组）；
    * int/float 不加区分标签，使 ``1`` 与 ``1.0`` 按数学值同组
      （Python 中 ``hash(1) == hash(1.0)`` 且 ``1 == 1.0``）；
    * str 加类型标签与数值隔开。
    """
    return tuple(value_key(v) for v in row)
