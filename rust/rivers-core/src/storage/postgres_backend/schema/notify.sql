-- The tables that the UI server listens to.
DO $rivers$
DECLARE
    watched text;
BEGIN
    -- The daemon sends statements that change no row several times a second
    -- (the sweep of expired slots, the reason a queued run waits): those
    -- must wake no page.
    FOREACH watched IN ARRAY ARRAY[
        'runs', 'assets', 'backfills', 'backfill_runs', 'backfill_run_outcomes',
        'ticks', 'condition_ticks', 'condition_evals', 'concurrency_pools',
        'concurrency_slots', 'pending_steps'
    ] LOOP
        EXECUTE format(
            'CREATE TRIGGER %I AFTER INSERT ON %I REFERENCING NEW TABLE AS changed '
            'FOR EACH STATEMENT EXECUTE FUNCTION rivers_notify_changed()',
            watched || '_notify_insert', watched);
        EXECUTE format(
            'CREATE TRIGGER %I AFTER UPDATE ON %I REFERENCING NEW TABLE AS changed '
            'FOR EACH STATEMENT EXECUTE FUNCTION rivers_notify_changed()',
            watched || '_notify_update', watched);
        EXECUTE format(
            'CREATE TRIGGER %I AFTER DELETE ON %I REFERENCING OLD TABLE AS changed '
            'FOR EACH STATEMENT EXECUTE FUNCTION rivers_notify_changed()',
            watched || '_notify_delete', watched);
    END LOOP;
    -- A run writes these in batches, and a batch has rows. A trigger that
    -- looks at the rows would keep a copy of each batch.
    FOREACH watched IN ARRAY ARRAY['events', 'run_logs', 'asset_partitions'] LOOP
        EXECUTE format(
            'CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %I '
            'FOR EACH STATEMENT EXECUTE FUNCTION rivers_notify()',
            watched || '_notify', watched);
    END LOOP;
END
$rivers$;
