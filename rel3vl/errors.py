"""可定位的错误类型。

所有由输入（表数据或计划结构）非法引起的失败都抛出
:class:`QueryError` 的子类。错误消息以稳定前缀开头，便于定位：

* ``[node#<id>]``      —— 计划节点（id 为节点字典在计划中的出现序号，0 起）；
* ``[node#<id>/expr]`` —— 该节点上的表达式；
* ``[table:<name>]``   —— ``tables`` 入参中的某个表；
* ``[plan]``           —— 计划整体结构问题（如节点数超限、存在环）。
"""

from __future__ import annotations


class QueryError(ValueError):
    """所有查询输入错误的基类（同时是 ``ValueError``）。"""


class PlanError(QueryError):
    """计划结构非法：未知操作符、字段缺失/类型错、超限、环等。"""


class TableError(QueryError):
    """``tables`` 入参结构或数据非法。"""


class SchemaError(QueryError):
    """列不存在、来源未知、投影重名等静态模式错误。"""


class ExpressionError(QueryError):
    """表达式结构非法或类型不被支持。"""


class StreamError(QueryError):
    """增量（增删流 / 水位 / 窗口）入口的非法输入基类。

    所有 ``StreamError`` 都在批次提交*之前*抛出：整批被拒绝，
    引擎状态与拒绝前完全一致（原子批次）。
    """


class ChangeError(StreamError):
    """增删流记录非法：未知表/列、重复 ID、删除不存在的行、op 非法等。"""


class WatermarkError(StreamError):
    """水位非法推进（倒退、非整数逻辑时间等）。"""


class LateDataError(StreamError):
    """事件时间早于 ``水位 - 迟到阈值``（超过保留期限）。

    属性 ``reason`` 区分成因：

    * ``"beyond_retention"`` —— 超过保留期限（窗口可能已清除）；
    * ``"missing_timestamp"`` —— 窗口时间表达式求出 NULL/非整数。
    """

    def __init__(self, message, reason="beyond_retention"):
        super().__init__(message)
        self.reason = reason


class SnapshotError(StreamError):
    """快照结构/版本非法或与当前计划不兼容。"""
