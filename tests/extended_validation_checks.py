"""扩展验证（不包含在 `python -m unittest discover -s tests` 的快速回归中）。

运行::

    python -m unittest tests.extended_validation_checks -v

内容：

1. **独立朴素参考交叉校验**：``tests._fixtures.naive_expected``（独立编写、
   不 import 被测核心）覆盖 aggregate/window 的批量语义；
2. **增量 vs 批量**：三类计划（外连接+聚合、窗口+聚合、三算子连接+窗口+聚合）
   在 100 批、随机增删、水位推进下，每批把累计输入整体交给原批量解释器对照；
3. **规模**：≥10000 输入行、100 批；重复重数 100；
4. **触碰键证明**：断言每次更新只触碰相关等值键/分组/窗口，不出现无关组。
"""

from __future__ import annotations

import random
import time
import unittest
from collections import Counter

from rel3vl import (
    MAX_STREAM_NODES,
    MAX_STREAM_ROWS,
    StreamError,
    execute,
)
from rel3vl.streaming import IncrementalProcessor

try:
    from ._fixtures import naive_expected
except ImportError:  # pragma: no cover
    from _fixtures import naive_expected  # type: ignore


def scan(t, a=None):
    n = {"op": "scan", "table": t}
    if a is not None:
        n["as"] = a
    return n


def col(a, c):
    return {"kind": "col", "table": a, "column": c}


def cmp(op, a, b):
    return {"kind": "cmp", "op": op, "left": a, "right": b}


def ins(cid, row):
    return {"id": cid, "op": "insert", "row": row}


def dele(cid):
    return {"id": cid, "op": "delete"}


def bag(proc):
    return Counter(tuple(r) for r in proc.current_result())


def chbag(changes):
    out = Counter()
    for c in changes:
        out[tuple(c["row"])] += c["mult"] if c["op"] == "+" else -c["mult"]
    return out


# ------------------------------------------------------------ 计划族

def plan_lj_agg():
    inputs = {"o": ["oid", "cid", "amt"], "c": ["cid", "city"]}
    plan = {
        "op": "aggregate",
        "input": {"op": "left_join", "left": scan("o", "o"),
                  "right": scan("c", "c"),
                  "on": cmp("=", col("o", "cid"), col("c", "cid"))},
        "groups": [{"name": "city", "expr": col("c", "city")}],
        "aggs": [{"name": "n", "func": "count"},
                 {"name": "s", "func": "sum", "arg": col("o", "amt")},
                 {"name": "a", "func": "avg", "arg": col("o", "amt")},
                 {"name": "hi", "func": "max", "arg": col("o", "amt")}]}
    return inputs, plan


def plan_win_agg(size=8, slide=None, lateness=2):
    inputs = {"e": ["ts", "g", "v"]}
    w = {"op": "window", "input": scan("e", "e"),
         "time_col": col("e", "ts"), "size": size, "allowed_lateness": lateness}
    if slide is not None:
        w["slide"] = slide
    plan = {
        "op": "aggregate", "input": w,
        "groups": [{"name": "g", "expr": col("e", "g")},
                   {"name": "ws", "expr": col("", "window_start")}],
        "aggs": [{"name": "n", "func": "count"},
                 {"name": "s", "func": "sum", "arg": col("e", "v")},
                 {"name": "lo", "func": "min", "arg": col("e", "v")}]}
    return inputs, plan


def plan_three_way():
    inputs = {"a": ["k", "ts"], "b": ["k", "k2"], "c": ["k2", "v"]}
    j1 = {"op": "left_join", "left": scan("a", "a"), "right": scan("b", "b"),
          "on": cmp("=", col("a", "k"), col("b", "k"))}
    j2 = {"op": "inner_join", "left": j1, "right": scan("c", "c"),
          "on": cmp("=", col("b", "k2"), col("c", "k2"))}
    plan = {
        "op": "aggregate",
        "input": {"op": "window", "input": j2, "time_col": col("a", "ts"),
                  "size": 10, "allowed_lateness": 3},
        "groups": [{"name": "ws", "expr": col("", "window_start")}],
        "aggs": [{"name": "n", "func": "count"},
                 {"name": "s", "func": "sum", "arg": col("c", "v")}]}
    return inputs, plan


# ------------------------------------------------------------ 1. 朴素参考交叉校验

class TestNaiveReferenceNewOps(unittest.TestCase):
    """独立朴素参考 vs 批量解释器（aggregate/window 全覆盖，小确定域）。"""

    def _check(self, plan, tables):
        res = execute(plan, tables, max_rows=MAX_STREAM_ROWS,
                      max_nodes=MAX_STREAM_NODES)
        schema, exp = naive_expected(plan, tables)
        self.assertEqual([(s, c) for s, c in schema],
                         [(c["source"], c["name"]) for c in res["columns"]])
        self.assertEqual(Counter(tuple(r) for r in res["rows"]), exp)

    def test_aggregate_all_funcs_small_domain(self):
        # 完整枚举小域：g in {a,b}, v in {None,1,2}
        rows = [(g, v) for g in ("a", "b") for v in (None, 1, 2, 1)]
        tables = {"t": {"columns": ["g", "v"],
                        "rows": [list(r) for r in rows]}}
        plan = {
            "op": "aggregate", "input": scan("t", "t"),
            "groups": [{"name": "g", "expr": col("t", "g")}],
            "aggs": [
                {"name": "n", "func": "count"},
                {"name": "nc", "func": "count", "arg": col("t", "v")},
                {"name": "s", "func": "sum", "arg": col("t", "v")},
                {"name": "a", "func": "avg", "arg": col("t", "v")},
                {"name": "lo", "func": "min", "arg": col("t", "v")},
                {"name": "hi", "func": "max", "arg": col("t", "v")}]}
        self._check(plan, tables)

    def test_global_aggregate_empty_and_nonempty(self):
        plan = {"op": "aggregate", "input": scan("t", "t"),
                "aggs": [{"name": "n", "func": "count"},
                         {"name": "s", "func": "sum", "arg": col("t", "v")}]}
        self._check(plan, {"t": {"columns": ["v"], "rows": []}})
        self._check(plan, {"t": {"columns": ["v"], "rows": [[1], [None], [2]]}})

    def test_window_tumbling_and_sliding(self):
        tables = {"e": {"columns": ["ts", "g", "v"],
                        "rows": [[t, "a", t] for t in range(13) for a in ("a",)]}}
        for kw in ({"size": 5}, {"size": 6, "slide": 2}):
            inputs, plan = plan_win_agg(**kw)
            self._check(plan, tables)

    def test_window_join_aggregate_chain(self):
        inputs, plan = plan_three_way()
        tables = {
            "a": {"columns": inputs["a"], "rows": [[0, 1], [1, 6], [None, 7]]},
            "b": {"columns": inputs["b"], "rows": [[0, 0], [1, 1]]},
            "c": {"columns": inputs["c"], "rows": [[0, 10], [1, 20]]}}
        self._check(plan, tables)


# ------------------------------------------------------------ 2/3. 增量差分 + 规模

class TestDifferentialAtScale(unittest.TestCase):
    def _diff(self, name, make, rand_row, n_batches, seed, *,
              per_batch, deletes=True, expect_rejects=False,
              prime=None, time_based=False):
        inputs, plan = make()
        proc = IncrementalProcessor(plan, inputs)
        rng = random.Random(seed)
        live = {t: {} for t in inputs}
        counters = {"id": {t: 0 for t in inputs}, "inserts": 0}
        applied = Counter()
        for r0 in proc.current_result():
            applied[tuple(r0)] += 1
        wm = 0
        rejects = 0
        if prime is not None:
            batch = prime(rng, live, counters)
            proc.apply_batch(batch)

        def cumulative():
            return {t: {"columns": inputs[t],
                        "rows": [list(r) for r in live[t].values()]}
                for t in inputs}

        for b in range(n_batches):
            changes = {}
            for t in inputs:
                if prime is not None and t in ("b", "c"):
                    continue
                ops = []
                for _ in range(per_batch(t, b)):
                    if deletes and live[t] and rng.random() < 0.35:
                        did = rng.choice(sorted(live[t]))
                        if not any(o["id"] == did for o in ops):
                            ops.append(dele(did))
                            continue
                    counters["id"][t] += 1
                    cid = f"{t[0]}{counters['id'][t]}"
                    row = rand_row(t, rng, wm, b)
                    ops.append(ins(cid, row))
                    counters["inserts"] += 1
                if ops:
                    changes[t] = ops
            new_wm = wm + (rng.randrange(1, 5) if rng.random() < 0.7 else 0)
            try:
                res = proc.apply_batch({"watermark": new_wm, "changes": changes})
            except StreamError:
                rejects += 1
                self.assertTrue(expect_rejects or time_based)
                # 原子回滚：状态仍等于批量重算
                self.assertEqual(bag(proc), Counter(tuple(r) for r in execute(
                    plan, cumulative(), max_rows=MAX_STREAM_ROWS,
                    max_nodes=MAX_STREAM_NODES)["rows"]))
                self.assertEqual(proc.watermark, wm)
                continue
            wm = new_wm
            for t, ops in changes.items():
                for o in ops:
                    if o["op"] == "insert":
                        live[t][o["id"]] = tuple(o["row"])
                    else:
                        live[t].pop(o["id"], None)
            applied.update(chbag(res["changes"]))
            expect = Counter(tuple(r) for r in execute(
                plan, cumulative(), max_rows=MAX_STREAM_ROWS,
                max_nodes=MAX_STREAM_NODES)["rows"])
            self.assertEqual(bag(proc), expect, msg=f"{name} b={b} 袋不符")
            self.assertEqual(Counter({k: v for k, v in applied.items() if v}),
                             expect, msg=f"{name} b={b} changelog 累计不符")
        return proc, counters["inserts"], rejects

    def test_lj_agg_100_batches(self):
        def rand_row(t, rng, wm, b):
            if t == "o":
                return [rng.randrange(20), rng.choice([1, 2, None, 3]),
                        rng.choice([1, 2, None, 10])]
            return [rng.randrange(1, 5), rng.choice(["X", "Y", None])]
        _, n_ins, _ = self._diff(
            "lj_agg", plan_lj_agg, rand_row, 100, seed=11,
            per_batch=lambda t, b: 12, deletes=True)
        self.assertGreater(n_ins, 1000)

    def test_window_agg_100_batches_with_lateness(self):
        def rand_row(t, rng, wm, b):
            # 围绕当前水位：多数在保留期内，偶尔超期触发拒绝
            return [max(0, wm + rng.randrange(-6, 8)),
                    rng.choice(["a", "b"]), rng.choice([1, 2, None])]
        _, n_ins, rej = self._diff(
            "win_agg", lambda: plan_win_agg(8, lateness=2), rand_row, 100,
            seed=12, per_batch=lambda t, b: 10, deletes=True,
            time_based=True, expect_rejects=True)
        self.assertGreater(n_ins, 500)

    def test_three_way_scale_10000_rows_100_batches(self):
        """主规模：维表先就位，事实表 100 批共 ≥10000 行，定期删一份，
        末批超期删除被明确拒绝且不改状态；末态与批量解释器逐组对照。"""
        inputs, plan = plan_three_way()
        proc = IncrementalProcessor(plan, inputs)
        rng = random.Random(42)
        live = {"a": {}, "b": {}, "c": {}}
        b_rows = [(f"b{k}", [k, k % 10]) for k in range(50)]
        c_rows = [(f"c{k}", [k2, k2 + 1])
                  for k2 in range(10) for k in (f"c{k2}_{id(k2)}",)]
        cops = []
        for k2 in range(10):
            for j in range(2):
                cid = f"c{k2}_{j}"
                cops.append(ins(cid, [k2, k2 + 1]))
                live["c"][cid] = (k2, k2 + 1)
        for cid, row in b_rows:
            live["b"][cid] = tuple(row)
        proc.apply_batch({"watermark": 0, "changes": {
            "b": [ins(cid, list(r)) for cid, r in b_rows], "c": cops}})
        total = 70
        nid = 0
        prev = []
        t0 = time.time()
        for b in range(100):
            wm = b * 10
            ops, cur = [], []
            for _ in range(100):
                nid += 1
                cid = f"a{nid}"
                row = [rng.randrange(50), wm + rng.randrange(0, 21)]
                ops.append(ins(cid, row))
                live["a"][cid] = tuple(row)
                cur.append(cid)
                total += 1
            # 删除上一批的一行（事件时间仍在打开窗口/保留期内）
            if prev:
                did = prev[rng.randrange(len(prev))]
                if did in live["a"]:
                    ops.append(dele(did))
                    live["a"].pop(did, None)
            prev = cur
            proc.apply_batch({"watermark": wm, "changes": {"a": ops}})
        self.assertGreaterEqual(total, 10000)
        self.assertLess(time.time() - t0, 60.0)
        # 超过保留期限的删除：明确拒绝，状态不变
        before = bag(proc)
        with self.assertRaises(StreamError):
            proc.apply_batch({"changes": {"a": [dele("a1")]}})
        self.assertEqual(bag(proc), before)
        # 末态与原批量解释器对照
        tables = {t: {"columns": inputs[t],
                      "rows": [list(r) for r in live[t].values()]} for t in inputs}
        expect = Counter(tuple(r) for r in execute(
            plan, tables, max_rows=MAX_STREAM_ROWS,
            max_nodes=MAX_STREAM_NODES)["rows"])
        self.assertEqual(before, expect)

    def test_multiplicity_100(self):
        """重复内容 100 重：删一份只改变一份的乘积。"""
        inputs = {"l": ["k"], "r": ["k"]}
        plan = {"op": "inner_join", "left": scan("l", "l"),
                "right": scan("r", "r"),
                "on": cmp("=", col("l", "k"), col("r", "k"))}
        p = IncrementalProcessor(plan, inputs)
        p.apply_batch({"changes": {
            "l": [ins(f"l{i}", [7]) for i in range(100)],
            "r": [ins("r1", [7])]}})
        self.assertEqual(bag(p), Counter({(7, 7): 100}))
        r = p.apply_batch({"changes": {"l": [dele("l50")]}})
        self.assertEqual(chbag(r["changes"]), Counter({(7, 7): -1}))
        self.assertEqual(bag(p), Counter({(7, 7): 99}))


# ------------------------------------------------------------ 4. 触碰键证明

class TestTouchedKeysProof(unittest.TestCase):
    """证明增量更新只触碰相关状态键，不做无关组全量扫描。"""

    def test_aggregate_touches_only_changed_groups(self):
        inputs = {"t": ["g", "v"]}
        plan = {"op": "aggregate", "input": scan("t", "t"),
                "groups": [{"name": "g", "expr": col("t", "g")}],
                "aggs": [{"name": "c", "func": "count"}]}
        p = IncrementalProcessor(plan, inputs)
        p.apply_batch({"changes": {"t": [ins(c, [c, 1])
                                        for c in ("a", "b", "c", "d")]}})
        r = p.apply_batch({"changes": {"t": [ins("z1", ["b", 1])]}})
        agg_touched = [v for v in r["touched"].values()
                       if v.get("op") == "aggregate"][0]
        # 只有 b 组被触碰；a/c/d 绝不出现在 groups 里
        self.assertEqual(agg_touched["groups"], [["b"]])

    def test_join_touches_only_equi_buckets(self):
        inputs = {"l": ["k", "v"], "r": ["k", "w"]}
        plan = {"op": "left_join", "left": scan("l", "l"),
                "right": scan("r", "r"),
                "on": cmp("=", col("l", "k"), col("r", "k"))}
        p = IncrementalProcessor(plan, inputs)
        p.apply_batch({"changes": {
            "l": [ins(f"l{k}", [k, k]) for k in range(20)],
            "r": [ins(f"r{k}", [k, k]) for k in range(20)]}})
        # 删除键 7 的右行：只触碰键 7（及其左行的 PAD 翻转）
        r = p.apply_batch({"changes": {"r": [dele("r7")]}})
        jt = [v for v in r["touched"].values()
              if v.get("op") == "left_join"][0]
        self.assertEqual(jt["left_keys"], [[7]])
        self.assertEqual(jt["right_keys"], [[7]])
        self.assertEqual(jt["pad_switched"], ["l7"])

    def test_window_touches_only_events_windows(self):
        inputs, plan = plan_win_agg(size=10)
        p = IncrementalProcessor(plan, inputs)
        p.apply_batch({"watermark": 0, "changes": {"e": [
            ins("e1", [1, "a", 1]), ins("e2", [15, "b", 1])]}})
        r = p.apply_batch({"watermark": 5, "changes": {"e": [
            ins("e3", [2, "a", 1])]}})
        wt = [v for v in r["touched"].values()
              if v.get("op") == "window"][0]
        self.assertEqual(wt["window_starts"], [0])  # 不触碰 [10,20)


if __name__ == "__main__":
    unittest.main()
