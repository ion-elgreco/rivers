-- One row per materialized partition of an asset: the newest event wins.
CREATE TABLE asset_partitions (
    code_location_id  text NOT NULL DEFAULT 'default',
    asset_key         text NOT NULL,
    partition_ord     bytea NOT NULL,
    partition_key     jsonb NOT NULL,
    last_event_id     text,
    last_run_id       text,
    last_timestamp    bigint,
    PRIMARY KEY (code_location_id, asset_key, partition_ord)
);

-- The newest deletion of a partition. It outlives its Deletion event.
CREATE TABLE asset_partition_deletions (
    code_location_id  text NOT NULL DEFAULT 'default',
    asset_key         text NOT NULL,
    partition_ord     bytea NOT NULL,
    partition_key     jsonb NOT NULL,
    timestamp         bigint NOT NULL,
    PRIMARY KEY (code_location_id, asset_key, partition_ord)
);

CREATE TABLE asset_partition_failures (
    code_location_id  text NOT NULL,
    asset_key         text NOT NULL,
    partition_ord     bytea NOT NULL,
    partition_key     jsonb NOT NULL,
    failed_at         bigint NOT NULL,
    run_id            text NOT NULL,
    built_at          bigint,
    has_row           boolean NOT NULL,
    PRIMARY KEY (code_location_id, asset_key, partition_ord)
);
CREATE INDEX idx_apf_row
    ON asset_partition_failures (code_location_id, asset_key, has_row, partition_ord);

CREATE TABLE asset_partition_run_failures (
    code_location_id  text NOT NULL,
    asset_key         text NOT NULL,
    partition_ord     bytea NOT NULL,
    partition_key     jsonb NOT NULL,
    run_id            text NOT NULL,
    step_failed_at    bigint,
    run_failed_at     bigint,
    PRIMARY KEY (code_location_id, asset_key, partition_ord, run_id)
);
CREATE INDEX idx_aprf_run ON asset_partition_run_failures (run_id);

-- A time that can be unset, compared as SurrealDB compares it: unset sorts
-- before every time.
CREATE FUNCTION rivers_time(p_time bigint) RETURNS bigint
LANGUAGE sql IMMUTABLE AS $rivers$
    SELECT COALESCE(p_time, '-9223372036854775808'::bigint)
$rivers$;

-- Whether a materialization at `p_at` is newer than the one a row holds
-- (`p_held`) and than the deletion of its asset (`p_deleted`).
CREATE FUNCTION rivers_newer(p_held bigint, p_deleted bigint, p_at bigint) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $rivers$
    SELECT rivers_time(p_held) <= p_at AND rivers_time(p_deleted) < p_at
$rivers$;

-- Whether one of the partitions `p_ords` of an asset has a failure row.
CREATE FUNCTION rivers_failures_of(p_cl text, p_asset text, p_ords bytea[]) RETURNS boolean
LANGUAGE sql STABLE AS $rivers$
    SELECT EXISTS (
        SELECT 1 FROM asset_partition_failures f
        WHERE f.code_location_id = p_cl AND f.asset_key = p_asset
            AND f.partition_ord = ANY (p_ords))
$rivers$;

-- Lock the assets `p_cls[i]`/`p_assets[i]`: alone when `p_alone[i]`
-- (deletions, failure rows), else shared (partition rows). A writer takes
-- these locks before any row of the assets. Advisory locks queue fairly: a
-- shared request waits behind a writer that waits to be alone, and they
-- write nothing. The key is a table of this store (the database holds other
-- stores) and the hash of the asset; the length of the code location keeps
-- two assets apart. The locks go in the order of the keys, so two writers
-- never wait for each other in a circle.
CREATE FUNCTION rivers_lock_assets(p_cls text[], p_assets text[], p_alone boolean[])
RETURNS void LANGUAGE plpgsql AS $rivers$
DECLARE
    v_store integer := 'asset_partitions'::regclass::oid::integer;
    v_lock record;
BEGIN
    FOR v_lock IN
        SELECT hashtext(length(u.cl) || ':' || u.cl || u.asset) AS key, bool_or(u.alone) AS alone
        FROM unnest(p_cls, p_assets, p_alone) AS u(cl, asset, alone)
        GROUP BY 1 ORDER BY 1
    LOOP
        IF v_lock.alone THEN
            PERFORM pg_advisory_xact_lock(v_store, v_lock.key);
        ELSE
            PERFORM pg_advisory_xact_lock_shared(v_store, v_lock.key);
        END IF;
    END LOOP;
END
$rivers$;

-- Give each of the partitions `p_ords` of an asset the failure row of its
-- latest failure by run, or none. First the failures by run go that are no
-- newer than the partition's materialization, its deletion and the
-- deletion of the asset: those times only move later, so such a failure
-- fails nothing, now or later. Of two failures at the same time, the run
-- that sorts first wins.
CREATE FUNCTION rivers_settle_failures(p_cl text, p_asset text, p_ords bytea[])
RETURNS void LANGUAGE plpgsql AS $rivers$
DECLARE
    v_deleted_at bigint;
BEGIN
    SELECT a.last_deletion_timestamp INTO v_deleted_at
    FROM assets a WHERE a.code_location_id = p_cl AND a.asset_key = p_asset;

    DELETE FROM asset_partition_run_failures r
    USING (
        SELECT k.ord, GREATEST(p.last_timestamp, d.timestamp, v_deleted_at) AS replaced_at
        FROM unnest(p_ords) AS k(ord)
        LEFT JOIN asset_partitions p ON p.code_location_id = p_cl
            AND p.asset_key = p_asset AND p.partition_ord = k.ord
        LEFT JOIN asset_partition_deletions d ON d.code_location_id = p_cl
            AND d.asset_key = p_asset AND d.partition_ord = k.ord
    ) AS replaced
    WHERE r.code_location_id = p_cl AND r.asset_key = p_asset
        AND r.partition_ord = replaced.ord
        AND rivers_time(GREATEST(r.step_failed_at, r.run_failed_at))
            <= rivers_time(replaced.replaced_at);

    INSERT INTO asset_partition_failures AS f (code_location_id, asset_key, partition_ord,
        partition_key, failed_at, run_id, built_at, has_row)
    SELECT p_cl, p_asset, l.partition_ord, l.partition_key, l.failed_at, l.run_id,
        p.last_timestamp, p.last_timestamp IS NOT NULL
    FROM (
        SELECT DISTINCT ON (r.partition_ord) r.partition_ord, r.partition_key, r.run_id,
            GREATEST(r.step_failed_at, r.run_failed_at) AS failed_at
        FROM asset_partition_run_failures r
        WHERE r.code_location_id = p_cl AND r.asset_key = p_asset
            AND r.partition_ord = ANY (p_ords)
        ORDER BY r.partition_ord, GREATEST(r.step_failed_at, r.run_failed_at) DESC,
            r.run_id COLLATE "C"
    ) AS l
    LEFT JOIN asset_partitions p ON p.code_location_id = p_cl AND p.asset_key = p_asset
        AND p.partition_ord = l.partition_ord
    ON CONFLICT (code_location_id, asset_key, partition_ord) DO UPDATE SET
        failed_at = EXCLUDED.failed_at, run_id = EXCLUDED.run_id,
        built_at = EXCLUDED.built_at, has_row = EXCLUDED.has_row
    WHERE (f.failed_at, f.run_id, f.built_at, f.has_row) IS DISTINCT FROM
        (EXCLUDED.failed_at, EXCLUDED.run_id, EXCLUDED.built_at, EXCLUDED.has_row);

    DELETE FROM asset_partition_failures f
    WHERE f.code_location_id = p_cl AND f.asset_key = p_asset
        AND f.partition_ord = ANY (p_ords)
        AND NOT EXISTS (
            SELECT 1 FROM asset_partition_run_failures r
            WHERE r.code_location_id = p_cl AND r.asset_key = p_asset
                AND r.partition_ord = f.partition_ord);
END
$rivers$;

-- Record the failures (partition `p_ords[i]` at `p_ats[i]` by run
-- `p_runs[i]`, one per partition and run) of an asset that the caller has
-- locked alone. A failure counts when it is newer than the
-- partition's materialization, its deletion and the deletion of the asset.
-- `p_run_end`: the failures are the end of their run in failure, else keyed
-- step failures.
CREATE FUNCTION rivers_record_failures(p_cl text, p_asset text, p_ords bytea[],
    p_keys jsonb[], p_ats bigint[], p_runs text[], p_run_end boolean)
RETURNS void LANGUAGE plpgsql AS $rivers$
DECLARE
    v_deleted_at bigint;
BEGIN
    SELECT a.last_deletion_timestamp INTO v_deleted_at
    FROM assets a WHERE a.code_location_id = p_cl AND a.asset_key = p_asset;

    INSERT INTO asset_partition_run_failures AS r (code_location_id, asset_key,
        partition_ord, partition_key, run_id, step_failed_at, run_failed_at)
    SELECT p_cl, p_asset, f.ord, f.key, f.run,
        CASE WHEN NOT p_run_end THEN f.failed_at END, CASE WHEN p_run_end THEN f.failed_at END
    FROM unnest(p_ords, p_keys, p_ats, p_runs) AS f(ord, key, failed_at, run)
    LEFT JOIN asset_partitions p ON p.code_location_id = p_cl AND p.asset_key = p_asset
        AND p.partition_ord = f.ord
    LEFT JOIN asset_partition_deletions d ON d.code_location_id = p_cl
        AND d.asset_key = p_asset AND d.partition_ord = f.ord
    WHERE rivers_time(GREATEST(p.last_timestamp, d.timestamp, v_deleted_at)) < f.failed_at
    ON CONFLICT (code_location_id, asset_key, partition_ord, run_id) DO UPDATE SET
        step_failed_at = GREATEST(r.step_failed_at, EXCLUDED.step_failed_at),
        run_failed_at = COALESCE(EXCLUDED.run_failed_at, r.run_failed_at);

    PERFORM rivers_settle_failures(p_cl, p_asset, p_ords);
END
$rivers$;

-- Apply the events of one asset of a send (`p_events`, with `p_members`,
-- the members of their partitions) in the order of the SurrealDB backend:
-- materializations, observations, the deletion of the asset, deletions of
-- partitions, step failures. The caller has locked the asset, alone when
-- `p_alone`. `p_actions`: the action runs of the send.
CREATE FUNCTION rivers_apply_events(p_cl text, p_asset text, p_alone boolean,
    p_events rivers_event[], p_members rivers_member[], p_actions text[], p_single boolean)
RETURNS void LANGUAGE plpgsql AS $rivers$
DECLARE
    v_deleted_at bigint;
    v_latest rivers_event;
    v_inputs rivers_event;
    v_provenance_at bigint;
    v_event rivers_event;
    v_keys bytea[];
    v_failures record;
BEGIN
    -- Each partition keeps the row of its newest materialization, unless a
    -- deletion of the asset or of the partition is as new.
    SELECT a.last_deletion_timestamp INTO v_deleted_at
    FROM assets a WHERE a.code_location_id = p_cl AND a.asset_key = p_asset;
    INSERT INTO asset_partitions AS p (code_location_id, asset_key, partition_ord,
        partition_key, last_event_id, last_run_id, last_timestamp)
    SELECT DISTINCT ON (e.ord) p_cl, p_asset, e.ord, e.key, e.ref, e.run, e.ts
    FROM unnest(p_events) AS e
    WHERE e.cl = p_cl AND e.asset = p_asset AND e.type = 'Materialization'
        AND e.ord IS NOT NULL AND rivers_time(v_deleted_at) < e.ts
        AND NOT EXISTS (
            SELECT 1 FROM asset_partition_deletions d
            WHERE d.code_location_id = p_cl AND d.asset_key = p_asset
                AND d.partition_ord = e.ord AND d.timestamp >= e.ts)
    ORDER BY e.ord, e.ts DESC, e.i DESC
    ON CONFLICT (code_location_id, asset_key, partition_ord) DO UPDATE SET
        last_event_id = EXCLUDED.last_event_id, last_run_id = EXCLUDED.last_run_id,
        last_timestamp = EXCLUDED.last_timestamp
    WHERE rivers_time(p.last_timestamp) <= EXCLUDED.last_timestamp;

    -- The failures at or before a materialization go, and a run that
    -- materialized a partition does not fail it by its end. Without the
    -- asset alone, no failure row of these partitions exists.
    IF p_alone THEN
        v_keys := ARRAY(
            SELECT m.ord FROM unnest(p_members) AS m JOIN unnest(p_events) AS e ON e.i = m.event
            WHERE e.cl = p_cl AND e.asset = p_asset AND e.type = 'Materialization');
        IF cardinality(v_keys) > 0 THEN
            UPDATE asset_partition_run_failures r SET run_failed_at = NULL
            FROM unnest(p_members) AS m JOIN unnest(p_events) AS e ON e.i = m.event
            WHERE e.cl = p_cl AND e.asset = p_asset AND e.type = 'Materialization'
                AND r.code_location_id = p_cl AND r.asset_key = p_asset
                AND r.partition_ord = m.ord AND r.run_id = e.run
                AND r.run_failed_at IS NOT NULL;
            PERFORM rivers_settle_failures(p_cl, p_asset, v_keys);
        END IF;
    END IF;

    -- The asset row takes the newest materialization. Its provenance comes
    -- from the newest one that read inputs, when there is one, and moves by
    -- its own time. An action run's `materialized()` writes no provenance,
    -- so a pending code version change survives it.
    SELECT e.* INTO v_latest FROM unnest(p_events) AS e
    WHERE e.cl = p_cl AND e.asset = p_asset AND e.type = 'Materialization'
    ORDER BY e.ts DESC, e.i DESC LIMIT 1;
    IF FOUND THEN
        SELECT e.* INTO v_inputs FROM unnest(p_events) AS e
        WHERE e.cl = p_cl AND e.asset = p_asset AND e.type = 'Materialization'
            AND e.idv <> '[]'::jsonb
        ORDER BY e.ts DESC, e.i DESC LIMIT 1;
        v_provenance_at := COALESCE(v_inputs.ts, v_latest.ts);
        UPDATE assets a SET last_event_id = v_latest.ref, last_run_id = v_latest.run,
            last_timestamp = v_latest.ts, last_data_version = v_latest.dv
        WHERE a.code_location_id = p_cl AND a.asset_key = p_asset
            AND rivers_newer(a.last_timestamp, a.last_deletion_timestamp, v_latest.ts);
        IF NOT (v_latest.run = ANY (p_actions) AND (p_single OR v_inputs.i IS NULL)) THEN
            UPDATE assets a SET last_materialization_code_version = a.code_version,
                last_input_data_versions = COALESCE(v_inputs.idv, a.last_input_data_versions),
                last_provenance_timestamp = v_provenance_at
            WHERE a.code_location_id = p_cl AND a.asset_key = p_asset
                AND rivers_newer(a.last_provenance_timestamp, a.last_deletion_timestamp,
                    v_provenance_at);
        END IF;
    END IF;

    SELECT e.* INTO v_event FROM unnest(p_events) AS e
    WHERE e.cl = p_cl AND e.asset = p_asset AND e.type = 'Observation'
    ORDER BY e.i DESC LIMIT 1;
    IF FOUND THEN
        UPDATE assets a SET last_event_id = v_event.ref, last_timestamp = v_event.ts,
            last_data_version = v_event.dv
        WHERE a.code_location_id = p_cl AND a.asset_key = p_asset;
    END IF;

    -- The deletion of the asset clears its data and its provenance, each by
    -- its own time, and the partition rows and failures at or before it.
    -- The event id still points at the deletion, so timelines resolve.
    SELECT e.* INTO v_event FROM unnest(p_events) AS e
    WHERE e.cl = p_cl AND e.asset = p_asset AND e.type = 'Deletion' AND e.ord IS NULL
    ORDER BY e.ts DESC, e.i DESC LIMIT 1;
    IF FOUND THEN
        UPDATE assets a SET last_event_id = v_event.ref, last_run_id = NULL,
            last_timestamp = NULL, last_data_version = NULL
        WHERE a.code_location_id = p_cl AND a.asset_key = p_asset
            AND rivers_time(a.last_timestamp) <= v_event.ts
            AND rivers_time(a.last_deletion_timestamp) <= v_event.ts;
        UPDATE assets a SET last_materialization_code_version = NULL,
            last_input_data_versions = '[]'::jsonb, last_provenance_timestamp = NULL
        WHERE a.code_location_id = p_cl AND a.asset_key = p_asset
            AND rivers_time(a.last_provenance_timestamp) <= v_event.ts;
        UPDATE assets a
        SET last_deletion_timestamp = GREATEST(a.last_deletion_timestamp, v_event.ts)
        WHERE a.code_location_id = p_cl AND a.asset_key = p_asset;
        DELETE FROM asset_partitions p
        WHERE p.code_location_id = p_cl AND p.asset_key = p_asset
            AND rivers_time(p.last_timestamp) <= v_event.ts;
        DELETE FROM asset_partition_failures f
        WHERE f.code_location_id = p_cl AND f.asset_key = p_asset
            AND f.failed_at <= v_event.ts;
        DELETE FROM asset_partition_run_failures r
        WHERE r.code_location_id = p_cl AND r.asset_key = p_asset
            AND rivers_time(r.step_failed_at) <= v_event.ts
            AND rivers_time(r.run_failed_at) <= v_event.ts;
        UPDATE asset_partition_failures f SET has_row = false, built_at = NULL
        WHERE f.code_location_id = p_cl AND f.asset_key = p_asset AND f.has_row
            AND rivers_time(f.built_at) <= v_event.ts;
    END IF;

    -- A deletion of partitions removes their rows no newer than it, and its
    -- tombstone keeps the newest deletion of each.
    v_keys := ARRAY(
        SELECT DISTINCT e.ord FROM unnest(p_events) AS e
        WHERE e.cl = p_cl AND e.asset = p_asset AND e.type = 'Deletion'
            AND e.ord IS NOT NULL);
    IF cardinality(v_keys) > 0 THEN
        DELETE FROM asset_partitions p
        USING (
            SELECT e.ord, max(e.ts) AS deleted_at FROM unnest(p_events) AS e
            WHERE e.cl = p_cl AND e.asset = p_asset AND e.type = 'Deletion'
                AND e.ord IS NOT NULL
            GROUP BY e.ord
        ) AS d
        WHERE p.code_location_id = p_cl AND p.asset_key = p_asset
            AND p.partition_ord = d.ord AND rivers_time(p.last_timestamp) <= d.deleted_at;
        INSERT INTO asset_partition_deletions AS t (code_location_id, asset_key,
            partition_ord, partition_key, timestamp)
        SELECT DISTINCT ON (e.ord) p_cl, p_asset, e.ord, e.key, e.ts
        FROM unnest(p_events) AS e
        WHERE e.cl = p_cl AND e.asset = p_asset AND e.type = 'Deletion'
            AND e.ord IS NOT NULL
        ORDER BY e.ord, e.ts DESC
        ON CONFLICT (code_location_id, asset_key, partition_ord) DO UPDATE
        SET timestamp = GREATEST(t.timestamp, EXCLUDED.timestamp);
        PERFORM rivers_settle_failures(p_cl, p_asset, v_keys);
    END IF;

    -- Keyed step failures of runs that are not action runs: a failed action
    -- did not fail to materialize anything.
    SELECT array_agg(f.ord) AS ords, array_agg(f.key) AS keys,
        array_agg(f.failed_at) AS ats, array_agg(f.run) AS runs
    INTO v_failures
    FROM (
        SELECT m.ord, (array_agg(m.key))[1] AS key, max(e.ts) AS failed_at, e.run
        FROM unnest(p_members) AS m JOIN unnest(p_events) AS e ON e.i = m.event
        WHERE e.cl = p_cl AND e.asset = p_asset AND e.type = 'StepFailure'
            AND e.run <> ALL (p_actions)
        GROUP BY m.ord, e.run
    ) AS f;
    IF v_failures.ords IS NOT NULL THEN
        PERFORM rivers_record_failures(p_cl, p_asset, v_failures.ords, v_failures.keys,
            v_failures.ats, v_failures.runs, false);
    END IF;
END
$rivers$;

-- Bring the failure rows in line after a change of the run `p_run`: its
-- deletion (`p_deleted`), else its end in failure at `p_end`, or none.
CREATE FUNCTION rivers_repair_failures_of_run(p_run text, p_deleted boolean, p_end bigint)
RETURNS void LANGUAGE plpgsql AS $rivers$
DECLARE
    v_cls text[];
    v_assets text[];
    v_keys bytea[];
BEGIN
    SELECT COALESCE(array_agg(t.cl ORDER BY t.cl COLLATE "C", t.asset COLLATE "C"), '{}'),
        COALESCE(array_agg(t.asset ORDER BY t.cl COLLATE "C", t.asset COLLATE "C"), '{}')
    INTO v_cls, v_assets
    FROM (
        SELECT DISTINCT r.code_location_id AS cl, r.asset_key AS asset
        FROM asset_partition_run_failures r
        WHERE r.run_id = p_run AND (p_deleted OR (r.run_failed_at IS NOT NULL
            AND r.run_failed_at IS DISTINCT FROM p_end))
    ) AS t;

    PERFORM rivers_lock_assets(v_cls, v_assets, array_fill(true, ARRAY[cardinality(v_assets)]));
    FOR i IN 1 .. cardinality(v_assets) LOOP
        IF p_deleted THEN
            WITH gone AS (
                DELETE FROM asset_partition_run_failures r
                WHERE r.run_id = p_run AND r.code_location_id = v_cls[i]
                    AND r.asset_key = v_assets[i]
                RETURNING r.partition_ord
            )
            SELECT array_agg(gone.partition_ord) INTO v_keys FROM gone;
        ELSE
            WITH moved AS (
                UPDATE asset_partition_run_failures r SET run_failed_at = p_end
                WHERE r.run_id = p_run AND r.code_location_id = v_cls[i]
                    AND r.asset_key = v_assets[i] AND r.run_failed_at IS NOT NULL
                    AND r.run_failed_at IS DISTINCT FROM p_end
                RETURNING r.partition_ord
            )
            SELECT array_agg(moved.partition_ord) INTO v_keys FROM moved;
        END IF;
        IF v_keys IS NOT NULL THEN
            PERFORM rivers_settle_failures(v_cls[i], v_assets[i], v_keys);
        END IF;
    END LOOP;
END
$rivers$;
