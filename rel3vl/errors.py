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
