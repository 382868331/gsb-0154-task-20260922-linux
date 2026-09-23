"""事件时间滚动窗口的分配规则（批量解释器与增量解释器共用）。

窗口是逻辑时间轴上的整数半开区间 ``[start, end)``：

* 翻滚窗口：``slide == size``，每个事件时间 ``t`` 恰属于一个窗口
  ``start = (t // size) * size``；
* 滑动窗口：``slide < size`` 且 ``size % slide == 0``，每个事件属于
  ``size // slide`` 个窗口（同一行复制相应重数，袋语义）；
  起点为 ``floor(t/slide)*slide - k*slide``（k=0..size/slide-1 中满足
  ``start <= t < start+size`` 者）。

固定缺省（缺省选择即契约，不靠运行时配置摇摆）：

* 时间值必须是 **int**（``bool`` 不接受），且非负；单位是纯逻辑计数，
  模块不解读任何物理时区；
* ``[start, end)`` 半开：``t == end`` 属于下一窗口，不属于本窗口；
* ``slide`` 缺省 = ``size``（翻滚）；``allowed_lateness`` 缺省 0；
* 水位 WM 单调不减；事件 ``t < WM - allowed_lateness`` 即超过保留期限，
  整批拒绝、状态不变；窗口在 ``end <= WM - allowed_lateness`` 时密封。
"""

from __future__ import annotations

from .errors import ExpressionError, PlanError

MAX_WINDOW_SIZE = 10**12


def validate_window_spec(size, slide, lateness, where="[window]"):
    if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
        raise PlanError(f"{where}: size 必须是正整数（逻辑时间单位），got {size!r}")
    if size > MAX_WINDOW_SIZE:
        raise PlanError(f"{where}: size 超过上限 {MAX_WINDOW_SIZE}")
    if slide is None:
        slide = size
    if isinstance(slide, bool) or not isinstance(slide, int) or slide <= 0:
        raise PlanError(f"{where}: slide 必须是正整数，got {slide!r}")
    if size % slide != 0:
        raise PlanError(f"{where}: size 必须是 slide 的整数倍（{size} % {slide} != 0）")
    if lateness is None:
        lateness = 0
    if isinstance(lateness, bool) or not isinstance(lateness, int) or lateness < 0:
        raise PlanError(f"{where}: allowed_lateness 必须是非负整数，got {lateness!r}")
    return size, slide, lateness


def validate_event_time(t, where="[window]"):
    if isinstance(t, bool) or not isinstance(t, int):
        raise ExpressionError(
            f"{where}: 事件时间必须是 int（bool 不接受），got {type(t).__name__}={t!r}"
        )
    if t < 0:
        raise ExpressionError(f"{where}: 事件时间必须非负，got {t}")
    return t


def assign_starts(t, size, slide):
    """返回事件 ``t`` 落入的全部窗口起点（升序）。

    事件时间契约为非负整数，故窗口起点同样不允许为负：早于 0 的部分被
    轴边界裁掉（例如 size=100/slide=50 时 t=35 只属于 ``[0,100)``，
    不属于假想的 ``[-50,50)``）。
    """
    validate_event_time(t)
    base = (t // slide) * slide
    k_max = size // slide
    starts = []
    for k in range(k_max):
        s = base - k * slide
        if s >= 0 and s <= t < s + size:
            starts.append(s)
    starts.sort()
    return tuple(starts)


def window_end(start, size):
    return start + size
