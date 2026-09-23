"""rel3vl —— 三值逻辑 (3VL) 下的关系查询解释器（袋语义，纯标准库）。

公开接口::

    # 批量解释器（旧契约保持不变）
    from rel3vl import execute, QueryError

    # 增量解释器（带行 ID 的增删流、事件时间窗口、水位）
    from rel3vl import IncrementalProcessor, StreamError

    result = execute(plan, tables)
    proc = IncrementalProcessor(plan, {"t": ["a", "b"]})
    out = proc.apply_batch({"watermark": 10, "changes": {"t": [...]}})
"""

from __future__ import annotations

from .engine import (
    MAX_NODES,
    MAX_ROWS_PER_TABLE,
    MAX_STREAM_BATCHES,
    MAX_STREAM_NODES,
    MAX_STREAM_ROWS,
    compile_plan,
    execute,
    validate_tables,
)
from .errors import (
    ExpressionError,
    PlanError,
    QueryError,
    SchemaError,
    StreamError,
    TableError,
)
from .streaming import IncrementalProcessor

__all__ = [
    "execute",
    "compile_plan",
    "validate_tables",
    "IncrementalProcessor",
    "QueryError",
    "PlanError",
    "SchemaError",
    "TableError",
    "ExpressionError",
    "StreamError",
    "MAX_ROWS_PER_TABLE",
    "MAX_NODES",
    "MAX_STREAM_ROWS",
    "MAX_STREAM_NODES",
    "MAX_STREAM_BATCHES",
]
