"""rel3vl —— 三值逻辑 (3VL) 下的关系查询解释器（袋语义，纯标准库）。

批量接口（原有，保持兼容）::

    from rel3vl import execute, QueryError
    result = execute(plan, tables)

增量接口（v2 新增，增删流 / 行 ID / 窗口 / 水位 / 快照）::

    from rel3vl import IncrementalExecutor, LateDataError
    ex = IncrementalExecutor(plan, tables, allowed_lateness=2)
    out = ex.apply_batch({"changes": [...], "watermark": 7})
"""

from __future__ import annotations

from .engine import (
    MAX_NODES,
    MAX_ROWS_PER_TABLE,
    compile_plan,
    execute,
    validate_tables,
)
from .errors import (
    ChangeError,
    ExpressionError,
    LateDataError,
    PlanError,
    QueryError,
    SchemaError,
    SnapshotError,
    StreamError,
    TableError,
    WatermarkError,
)
from .stream import (
    SNAPSHOT_VERSION,
    STREAM_MAX_BATCH_CHANGES,
    STREAM_MAX_ROWS_PER_TABLE,
    IncrementalExecutor,
)

__all__ = [
    # 批量
    "execute",
    "compile_plan",
    "validate_tables",
    # 增量
    "IncrementalExecutor",
    "SNAPSHOT_VERSION",
    "STREAM_MAX_ROWS_PER_TABLE",
    "STREAM_MAX_BATCH_CHANGES",
    # 错误
    "QueryError",
    "PlanError",
    "SchemaError",
    "TableError",
    "ExpressionError",
    "StreamError",
    "ChangeError",
    "WatermarkError",
    "LateDataError",
    "SnapshotError",
    # 上限
    "MAX_ROWS_PER_TABLE",
    "MAX_NODES",
]
