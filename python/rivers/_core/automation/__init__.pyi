class AutomationCondition:
    """Composable condition tree for declarative asset automation.

    Conditions determine when assets should be automatically materialized
    by the daemon. Compose them with ``&`` (and), ``|`` (or), ``~`` (not).

    Example::

        # Eager: materialize when deps update or asset is missing
        AutomationCondition.eager()

        # Custom: fire on cron, but skip if any dep is in progress
        AutomationCondition.on_cron("0 * * * *").without("any_deps_in_progress")

        # Check a specific asset's state
        AutomationCondition.newly_updated().on_selected("upstream_feed")
    """

    label: str | None
    """Optional display label for debugging and UI."""
    description: str
    """Human-readable description of the full condition tree."""
    children: list[AutomationCondition]
    """Child conditions (empty for leaf nodes)."""

    # High-level presets

    @staticmethod
    def eager() -> AutomationCondition:
        """Materialize when deps update or asset becomes missing.

        Excludes failed partitions/assets (not auto-retried until re-run).

        Equivalent to::

            (missing().newly_true() | any_deps_updated()).since_last_handled()
            & ~any_deps_missing() & ~any_deps_in_progress() & ~in_progress()
            & ~execution_failed()
        """
        ...
    @staticmethod
    def on_cron(cron_schedule: str, timezone: str | None = None) -> AutomationCondition:
        """Materialize on a cron schedule, after all deps have updated since the tick.

        Equivalent to::

            cron_tick_passed(schedule, tz).since_last_handled()
            & all_deps_updated_since_cron(schedule, tz)

        Args:
            cron_schedule: Cron expression (e.g. ``"0 * * * *"``).
            timezone: Optional timezone name (e.g. ``"US/Eastern"``).

        Raises:
            ValueError: If ``cron_schedule`` is not a valid cron expression.
        """
        ...
    @staticmethod
    def on_missing() -> AutomationCondition:
        """Materialize once when the asset becomes missing, then stop.

        Skips partitions in a failed state, so a ``mark_partition_failed``
        skip isn't re-requested.
        """
        ...

    # Leaf conditions

    @staticmethod
    def missing() -> AutomationCondition:
        """True when the asset has never been materialized."""
        ...
    @staticmethod
    def in_progress() -> AutomationCondition:
        """True when the asset is part of an in-progress run."""
        ...
    @staticmethod
    def execution_failed() -> AutomationCondition:
        """True when the latest execution of this asset failed."""
        ...
    @staticmethod
    def newly_updated() -> AutomationCondition:
        """True when the asset's materialization timestamp changed since the previous tick."""
        ...
    @staticmethod
    def newly_requested() -> AutomationCondition:
        """True when the asset was requested for materialization on the previous tick."""
        ...
    @staticmethod
    def code_version_changed() -> AutomationCondition:
        """True when the asset's code version differs from the last materialized version."""
        ...
    @staticmethod
    def cron_tick_passed(
        cron_schedule: str, timezone: str | None = None
    ) -> AutomationCondition:
        """True when a cron tick has passed since the last evaluation.

        Args:
            cron_schedule: Cron expression.
            timezone: Optional timezone name.

        Raises:
            ValueError: If ``cron_schedule`` is not a valid cron expression.
        """
        ...
    @staticmethod
    def in_latest_time_window(
        lookback_delta: float | None = None,
    ) -> AutomationCondition:
        """True when the partition falls within the latest time window.

        Args:
            lookback_delta: Optional lookback in seconds, measured back from
                the latest window's start — one period reaches exactly one
                window back, regardless of the time of day.

        Raises:
            ValueError: If ``lookback_delta`` is not a positive finite number.
        """
        ...
    @staticmethod
    def initial_evaluation() -> AutomationCondition:
        """True on the first evaluation tick after daemon startup or condition tree change."""
        ...
    @staticmethod
    def data_version_changed() -> AutomationCondition:
        """True when the asset's data version changed since the previous tick."""
        ...
    @staticmethod
    def backfill_in_progress() -> AutomationCondition:
        """True when the asset is part of an active backfill."""
        ...
    @staticmethod
    def in_flight() -> AutomationCondition:
        """True while being materialized by anything — a run (``in_progress``) or
        an active backfill (``backfill_in_progress``).

        Negate it (``~AutomationCondition.in_flight()``) in custom conditions to
        avoid re-dispatching running work; the presets already include it.
        """
        ...
    @staticmethod
    def last_executed_with_tags(
        *,
        tag_keys: list[str] | None = None,
        tag_values: list[tuple[str, str]] | None = None,
    ) -> AutomationCondition:
        """True when the latest run that materialized this asset had matching tags.

        Args:
            tag_keys: Match if any of these keys are present (any value).
            tag_values: Match if these exact key-value pairs are present.
        """
        ...
    @staticmethod
    def last_run_includes_target() -> AutomationCondition:
        """True if the dep's latest run also included the root asset being evaluated.

        Used internally by ``any_deps_updated()`` to suppress re-fires
        when a joint run already covered both the dep and the downstream.
        """
        ...
    @staticmethod
    def will_be_requested() -> AutomationCondition:
        """True if this asset's condition already fired earlier in this tick.

        Enables same-tick cascading: a downstream can fire before its dep
        materializes, as long as the dep's condition fired first (topological order).
        """
        ...
    @staticmethod
    def has_run_with_tags(
        *,
        tag_keys: list[str] | None = None,
        tag_values: list[tuple[str, str]] | None = None,
    ) -> AutomationCondition:
        """True if any new materialization this tick came from a run with matching tags.

        Args:
            tag_keys: Match if any of these keys are present.
            tag_values: Match if these exact key-value pairs are present.
        """
        ...
    @staticmethod
    def all_runs_have_tags(
        *,
        tag_keys: list[str] | None = None,
        tag_values: list[tuple[str, str]] | None = None,
    ) -> AutomationCondition:
        """True if all new materializations this tick came from runs with matching tags.

        Vacuously true if there were no new materializations.

        Args:
            tag_keys: Match if any of these keys are present.
            tag_values: Match if these exact key-value pairs are present.
        """
        ...

    # Dep-aggregate conditions

    @staticmethod
    def any_deps_missing() -> AutomationCondition:
        """True when any upstream dependency is missing and won't be requested this tick."""
        ...
    @staticmethod
    def any_deps_in_progress() -> AutomationCondition:
        """True when any upstream dependency is currently being materialized."""
        ...
    @staticmethod
    def any_deps_updated() -> AutomationCondition:
        """True when any dep was updated (and not in a joint run) or will be requested this tick."""
        ...
    @staticmethod
    def any_deps_match(
        condition: AutomationCondition,
    ) -> AutomationCondition:
        """True when any upstream dependency satisfies the given condition.

        Args:
            condition: The condition to evaluate on each dep.
        """
        ...
    @staticmethod
    def all_deps_match(
        condition: AutomationCondition,
    ) -> AutomationCondition:
        """True when all upstream dependencies satisfy the given condition.

        Args:
            condition: The condition to evaluate on each dep.
        """
        ...
    @staticmethod
    def all_deps_updated_since_cron(
        cron_schedule: str, timezone: str | None = None
    ) -> AutomationCondition:
        """True when all deps have been updated since the last tick of the given cron schedule.

        Equivalent to::

            all_deps_match(newly_updated().since(cron_tick_passed(schedule, tz)) | will_be_requested())

        Args:
            cron_schedule: Cron expression.
            timezone: Optional timezone name.

        Raises:
            ValueError: If ``cron_schedule`` is not a valid cron expression.
        """
        ...
    def on_selected(
        self,
        keys: str | list[str],
    ) -> AutomationCondition:
        """Evaluate this condition on specific named assets (true if any match).

        Args:
            keys: A single asset key or list of keys to evaluate on.

        Example::

            AutomationCondition.newly_updated().on_selected("upstream_feed")
            AutomationCondition.missing().on_selected(["a", "b"])
        """
        ...

    # Composition methods

    def newly_true(self) -> AutomationCondition:
        """Rising-edge detector: true only on the tick where this condition transitions false to true."""
        ...
    def since(self, reset_condition: AutomationCondition) -> AutomationCondition:
        """Latch: stays true once this condition fires, until ``reset_condition`` fires.

        Args:
            reset_condition: The condition that resets the latch.
        """
        ...
    def since_last_handled(self) -> AutomationCondition:
        """Debounce: true while this condition is true and hasn't been handled yet.

        Prevents re-firing on the tick immediately after the daemon requests materialization.
        """
        ...
    def replace(
        self,
        old: str | AutomationCondition,
        new: AutomationCondition,
    ) -> AutomationCondition:
        """Recursively replace sub-conditions matching ``old`` with ``new``.

        Args:
            old: A label string (matches by name) or a condition (matches by structure).
            new: The replacement condition.

        Example::

            AutomationCondition.eager().replace("any_deps_updated", custom_condition)
        """
        ...
    def without(
        self,
        condition: str | AutomationCondition,
    ) -> AutomationCondition:
        """Remove a child from an And condition.

        Matches by effective label — for ``~cond`` children, matches against
        the inner condition's label.

        Args:
            condition: A label string or condition identifying the child to remove.

        Example::

            AutomationCondition.eager().without("any_deps_in_progress")
        """
        ...
    def with_label(self, label: str) -> AutomationCondition:
        """Attach a display label for debugging and UI visualization.

        Args:
            label: The label string.
        """
        ...

    # Boolean operators

    def __and__(self, other: AutomationCondition) -> AutomationCondition: ...
    def __or__(self, other: AutomationCondition) -> AutomationCondition: ...
    def __invert__(self) -> AutomationCondition: ...

__all__ = ["AutomationCondition"]
