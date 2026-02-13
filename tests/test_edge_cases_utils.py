"""
Edge case tests for retry, utilities, schema, and dependency modules.

Tests for specific bugs found during code review of:
- retry/policy.py, circuit_breaker.py, dlq.py
- core/dependencies.py
"""

import time

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
class TestCircuitBreakerOpenStateFailure:
    """CircuitBreaker.record_failure in OPEN state resets the recovery timeout."""

    def test_record_failure_in_open_state_does_not_reset_timeout(self):
        """
        Bug: record_failure() unconditionally updates _last_failure_time.
        When circuit is OPEN, this resets the timeout for the HALF_OPEN
        transition, potentially preventing recovery indefinitely.
        """
        from interlace.core.retry.circuit_breaker import CircuitBreaker, CircuitState

        breaker = CircuitBreaker(failure_threshold=2, timeout=10.0)

        # Drive to OPEN state
        breaker.record_failure()
        breaker.record_failure()
        assert breaker._state == CircuitState.OPEN

        original_failure_time = breaker._last_failure_time
        assert original_failure_time is not None

        # Small sleep to ensure time.time() changes
        time.sleep(0.02)

        # Call record_failure while OPEN -- should NOT reset the timer
        breaker.record_failure()

        assert breaker._last_failure_time == original_failure_time, (
            f"record_failure() in OPEN state reset _last_failure_time "
            f"from {original_failure_time} to {breaker._last_failure_time}, "
            f"delaying recovery."
        )

    def test_record_failure_in_open_state_does_not_change_failure_count(self):
        """
        Supplementary: failure count should not grow in OPEN state since
        requests are blocked anyway.
        """
        from interlace.core.retry.circuit_breaker import CircuitBreaker, CircuitState

        breaker = CircuitBreaker(failure_threshold=3, timeout=10.0)

        # Drive to OPEN
        breaker.record_failure()
        breaker.record_failure()
        breaker.record_failure()
        assert breaker._state == CircuitState.OPEN

        count_at_open = breaker._failure_count

        # Additional failures while OPEN should be ignored
        breaker.record_failure()
        breaker.record_failure()

        assert breaker._failure_count == count_at_open, (
            f"Failure count grew from {count_at_open} to " f"{breaker._failure_count} while circuit was OPEN"
        )


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


@pytest.mark.unit
class TestDlqSqlValueBoolOrdering:
    """DLQ._persist_entry sql_value: bool isinstance check is dead code."""

    def test_bool_subclass_of_int_causes_wrong_branch(self):
        """
        Bug: In DLQ's local sql_value function, the isinstance check for bool
        comes AFTER (int, float). Since bool is a subclass of int,
        isinstance(True, (int, float)) is True, and the bool branch is
        never reached (dead code).

        We replicate the buggy logic and verify the fix matches state.py's
        _sql_value which has the correct ordering.
        """
        # Demonstrate the root cause
        assert isinstance(True, int), "bool is a subclass of int"
        assert isinstance(True, (int, float)), "True matches (int, float)"

        # Buggy ordering (matches DLQ before fix):
        def sql_value_buggy(val):
            if val is None:
                return "NULL"
            elif isinstance(val, str):
                return f"'{val}'"
            elif isinstance(val, (int, float)):  # bool matches here first!
                return str(val)
            elif isinstance(val, bool):  # DEAD CODE
                return "TRUE" if val else "FALSE"
            else:
                return f"'{str(val)}'"

        # With buggy ordering: True -> str(True) -> "True"
        assert sql_value_buggy(True) == "True", "Buggy: bool falls into int branch"
        assert sql_value_buggy(False) == "False", "Buggy: bool falls into int branch"

        # Verify state.py's _sql_value has correct ordering (bool before int)
        from interlace.core.state import _sql_value

        # _sql_value returns "1"/"0" for bools, which is intentional
        # The key point: it does NOT return str(True)/"True"
        assert _sql_value(True) != "True", "_sql_value should not return str(True) for boolean input"
        assert _sql_value(False) != "False", "_sql_value should not return str(False) for boolean input"

        # Verify integers still work correctly in _sql_value
        assert _sql_value(42) == "42"
        assert _sql_value(3.14) == "3.14"
