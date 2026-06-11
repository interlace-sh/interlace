"""
Edge case tests for retry, utilities, schema, and dependency modules.

Tests for specific bugs found during code review of:
- retry/policy.py
- core/dependencies.py
"""

import pytest


@pytest.mark.unit
class TestRetryPolicyGetDelayJitter:
    """RetryPolicy.get_delay: jitter applied after max_delay cap can exceed max_delay."""

    def test_get_delay_with_jitter_never_exceeds_max_delay(self):
        """
        Bug: get_delay caps delay at max_delay, then multiplies by jitter (0.75-1.25).
        This means the final delay can be up to max_delay * 1.25, violating the contract.
        """
        from interlace.core.retry.policy import RetryPolicy

        policy = RetryPolicy(
            max_attempts=10,
            initial_delay=1.0,
            max_delay=10.0,
            exponential_base=2.0,
            jitter=True,
        )

        # At high attempt numbers, base delay far exceeds max_delay,
        # so it's capped. But then jitter can push it above.
        max_seen = 0.0
        for _ in range(1000):
            delay = policy.get_delay(attempt=20)
            if delay > max_seen:
                max_seen = delay
            assert delay <= policy.max_delay, f"Delay {delay:.4f} exceeds max_delay {policy.max_delay}"

    def test_get_delay_without_jitter_respects_max_delay(self):
        """Sanity check: without jitter, max_delay is always respected."""
        from interlace.core.retry.policy import RetryPolicy

        policy = RetryPolicy(
            max_attempts=10,
            initial_delay=1.0,
            max_delay=10.0,
            exponential_base=2.0,
            jitter=False,
        )

        for attempt in range(20):
            delay = policy.get_delay(attempt)
            assert delay <= policy.max_delay


@pytest.mark.unit
class TestDependencyGraphAddModelStaleEdges:
    """DependencyGraph.add_model: calling twice leaves stale reverse edges."""

    def test_add_model_twice_cleans_old_reverse_edges(self):
        """
        Bug: add_model overwrites _graph[model] but does NOT remove the old
        entries from _reverse. This leaves stale dependent-of relationships.
        """
        from interlace.core.dependencies import DependencyGraph

        graph = DependencyGraph()

        # A depends on B and C
        graph.add_model("A", ["B", "C"])
        assert "A" in graph.get_dependents("B")
        assert "A" in graph.get_dependents("C")

        # Now A only depends on B (C removed)
        graph.add_model("A", ["B"])

        # C should no longer list A as a dependent
        assert "A" not in graph.get_dependents("C"), (
            f"Stale reverse edge: get_dependents('C') = {graph.get_dependents('C')}. "
            f"Expected A to be removed after updating A's dependencies."
        )

    def test_add_model_twice_no_duplicate_reverse_edges(self):
        """
        Bug: re-adding a model appends to _reverse again, creating duplicates.
        """
        from interlace.core.dependencies import DependencyGraph

        graph = DependencyGraph()

        graph.add_model("A", ["B"])
        graph.add_model("A", ["B"])  # Same deps, added again

        dependents = graph.get_dependents("B")
        # Should have exactly one entry for A, not two
        assert dependents.count("A") == 1, f"Duplicate reverse edge: get_dependents('B') = {dependents}"


@pytest.mark.unit
class TestDependencyGraphDetectCyclesFalsePositives:
    """DependencyGraph.detect_cycles: stale rec_stack causes false cycle reports."""

    def test_detect_cycles_no_false_positives_from_stale_state(self):
        """
        Bug: When a cycle is found, dfs() returns True without cleaning up
        rec_stack and path. Subsequent DFS traversals of other nodes then
        see stale entries in rec_stack and falsely report cycles.

        Setup:
          A <-> B  (real cycle)
          C -> D -> A  (D depends on A, NOT a cycle)

        Expected: Only the A-B cycle is detected.
        Buggy behavior: Also reports [A, B, C, D, A] as a cycle.
        """
        from interlace.core.dependencies import DependencyGraph

        graph = DependencyGraph()

        # Real cycle: A depends on B, B depends on A
        graph.add_model("A", ["B"])
        graph.add_model("B", ["A"])

        # Non-cyclic: C depends on D, D depends on A
        graph.add_model("C", ["D"])
        graph.add_model("D", ["A"])

        cycles = graph.detect_cycles()

        # Every detected cycle should only involve {A, B}
        for cycle in cycles:
            cycle_nodes = set(cycle)
            assert cycle_nodes.issubset({"A", "B"}), (
                f"False cycle detected: {cycle}. " f"Only A and B form a real cycle; C and D do not."
            )

    def test_detect_cycles_finds_real_cycle(self):
        """Sanity check: a real cycle IS detected."""
        from interlace.core.dependencies import DependencyGraph

        graph = DependencyGraph()
        graph.add_model("X", ["Y"])
        graph.add_model("Y", ["X"])

        cycles = graph.detect_cycles()
        assert len(cycles) >= 1, "Should detect at least one cycle"
