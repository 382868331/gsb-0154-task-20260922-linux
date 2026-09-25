"""rel3vl —— 三值逻辑 (3VL) 下的关系查询解释器（袋语义，纯标准库）。

公开接口::

    from rel3vl import execute, QueryError, PlanError, SchemaError, TableError

    result = execute(plan, tables)
    # result == {"columns": [{"source": ..., "name": ...}, ...],
    #            "rows": [[...], ...]}
"""

from __future__ import annotations

from .engine import (
    MAX_CORRELATION_DEPTH,
    MAX_NODES,
    MAX_ROWS_PER_TABLE,
    compile_plan,
    execute,
    validate_tables,
)
from .errors import ExpressionError, PlanError, QueryError, SchemaError, TableError

__all__ = [
    "execute",
    "compile_plan",
    "validate_tables",
    "QueryError",
    "PlanError",
    "SchemaError",
    "TableError",
    "ExpressionError",
    "MAX_ROWS_PER_TABLE",
    "MAX_NODES",
    "MAX_CORRELATION_DEPTH",
]
