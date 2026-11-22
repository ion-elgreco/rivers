CREATE TABLE events (
    id                   text PRIMARY KEY,
    code_location_id     text NOT NULL DEFAULT 'default',
    event_type           text NOT NULL,
    asset_key            text,
    run_id               text NOT NULL,
    partition_key        jsonb,
    partition_ord        bytea,
    timestamp            bigint NOT NULL,
    sort_order           bigint NOT NULL DEFAULT 0,
    metadata             jsonb NOT NULL DEFAULT '[]'::jsonb,
    data_version         text,
    code_version         text,
    input_data_versions  jsonb NOT NULL DEFAULT '[]'::jsonb
);
CREATE INDEX idx_events_run_type ON events (run_id, event_type);
CREATE INDEX idx_events_run_ts ON events (run_id, timestamp, sort_order);
-- The events of one partition of an asset. The key of a run or of a failure
-- can name thousands of partitions, too long for a btree row: the index
-- holds its md5, and readers compare `partition_ord` too.
CREATE INDEX idx_events_part ON events (code_location_id, asset_key, (md5(partition_ord)::uuid))
    WHERE partition_ord IS NOT NULL;
CREATE INDEX idx_events_loc_asset_type
    ON events (code_location_id, asset_key, event_type, timestamp, sort_order);
CREATE INDEX idx_events_loc_asset_ts ON events (code_location_id, asset_key, timestamp, sort_order);

-- An event of an asset in a send, as `rivers_store_events` applies it: `i`
-- is its place in the send, `ref` its id in the form the asset rows hold.
CREATE TYPE rivers_event AS (
    i      integer,
    cl     text,
    asset  text,
    type   text,
    run    text,
    ref    text,
    key    jsonb,
    ord    bytea,
    ts     bigint,
    dv     text,
    idv    jsonb
);

-- A member of the partition of the event at place `event` of a send.
CREATE TYPE rivers_member AS (
    event  integer,
    ord    bytea,
    key    jsonb
);

-- One send of events: store them and apply what they change, in one
-- transaction. The arrays hold one entry per event; `p_member_*` one per
-- member of the partition of a materialization or of a step failure, with
-- the place of its event. `p_single`: the send of one event by
-- `store_event`, which stamps the code version and the inputs on a
-- materialization, and lets an action run write no provenance even with
-- inputs. `p_alone`: lock alone each asset that gets partition rows, after
-- a send that met new failure rows (`RV001`).
--
-- The locks of all assets go first (`rivers_lock_assets`), then the rows of
-- each asset in the order of the assets.
CREATE FUNCTION rivers_store_events(
    p_id text[], p_ref text[], p_cl text[], p_type text[], p_asset text[], p_run text[],
    p_key jsonb[], p_ord bytea[], p_ts bigint[], p_sort bigint[], p_meta jsonb[],
    p_dv text[], p_idv jsonb[], p_member_of integer[], p_member_ord bytea[],
    p_member_key jsonb[], p_single boolean, p_alone boolean
) RETURNS void LANGUAGE plpgsql AS $rivers$
DECLARE
    v_events rivers_event[];
    v_members rivers_member[];
    v_actions text[];
    v_target record;
    v_alone boolean;
    v_locked_cls text[] := '{}';
    v_locked_assets text[] := '{}';
    v_locked_alone boolean[] := '{}';
    v_cls text[] := '{}';
    v_assets text[] := '{}';
    v_alones boolean[] := '{}';
BEGIN
    INSERT INTO events (id, code_location_id, event_type, asset_key, run_id, partition_key,
        partition_ord, timestamp, sort_order, metadata, data_version, code_version,
        input_data_versions)
    SELECT e.id, e.cl, e.type, e.asset, e.run, e.key, e.ord, e.ts, e.sort, e.meta, e.dv,
        CASE WHEN e.stamped THEN a.code_version END,
        CASE WHEN e.stamped THEN e.idv ELSE '[]'::jsonb END
    FROM (
        SELECT u.*, p_single AND u.type = 'Materialization' AND u.asset IS NOT NULL AS stamped
        FROM unnest(p_id, p_cl, p_type, p_asset, p_run, p_key, p_ord, p_ts, p_sort, p_meta,
            p_dv, p_idv) AS u(id, cl, type, asset, run, key, ord, ts, sort, meta, dv, idv)
    ) AS e
    LEFT JOIN assets a
        ON e.stamped AND a.code_location_id = e.cl AND a.asset_key = e.asset
    ON CONFLICT (id) DO NOTHING;

    v_events := ARRAY(
        SELECT ROW(u.i, u.cl, u.asset, u.type, u.run, u.ref, u.key, u.ord, u.ts, u.dv,
            u.idv)::rivers_event
        FROM unnest(p_cl, p_asset, p_type, p_run, p_ref, p_key, p_ord, p_ts, p_dv, p_idv)
            WITH ORDINALITY AS u(cl, asset, type, run, ref, key, ord, ts, dv, idv, i)
        WHERE u.asset IS NOT NULL);
    v_members := ARRAY(
        SELECT ROW(m.event, m.ord, m.key)::rivers_member
        FROM unnest(p_member_of, p_member_ord, p_member_key) AS m(event, ord, key));
    v_actions := ARRAY(
        SELECT r.run_id FROM runs r WHERE r.run_id = ANY (p_run) AND r.action IS NOT NULL);

    -- An asset with only step events, or failures of action runs, has
    -- nothing to apply.
    FOR v_target IN
        SELECT e.cl, e.asset,
            bool_or(e.type = 'Deletion' OR (e.type = 'StepFailure' AND e.ord IS NOT NULL
                AND e.run <> ALL (v_actions))) AS writes_alone,
            bool_or(e.type = 'Materialization' AND e.ord IS NOT NULL) AS adds_rows,
            bool_or(e.type IN ('Materialization', 'Observation', 'Deletion')
                OR (e.type = 'StepFailure' AND e.ord IS NOT NULL
                    AND e.run <> ALL (v_actions))) AS applies,
            ARRAY(
                SELECT m.ord FROM unnest(v_members) AS m
                JOIN unnest(v_events) AS b ON b.i = m.event
                WHERE b.cl = e.cl AND b.asset = e.asset AND b.type = 'Materialization'
            ) AS built
        FROM unnest(v_events) AS e
        GROUP BY e.cl, e.asset
        ORDER BY e.cl COLLATE "C", e.asset COLLATE "C"
    LOOP
        CONTINUE WHEN NOT v_target.applies;
        v_alone := v_target.writes_alone OR (v_target.adds_rows
            AND (p_alone OR rivers_failures_of(v_target.cl, v_target.asset, v_target.built)));
        IF v_alone OR v_target.adds_rows THEN
            v_locked_cls := v_locked_cls || v_target.cl;
            v_locked_assets := v_locked_assets || v_target.asset;
            v_locked_alone := v_locked_alone || v_alone;
        END IF;
        v_cls := v_cls || v_target.cl;
        v_assets := v_assets || v_target.asset;
        v_alones := v_alones || v_alone;
    END LOOP;

    PERFORM rivers_lock_assets(v_locked_cls, v_locked_assets, v_locked_alone);
    -- Failure rows come only from a writer that has the asset alone.
    IF EXISTS (
        SELECT 1 FROM unnest(v_locked_cls, v_locked_assets, v_locked_alone)
            AS l(cl, asset, alone)
        JOIN unnest(v_events) AS e ON e.cl = l.cl AND e.asset = l.asset
            AND e.type = 'Materialization'
        JOIN unnest(v_members) AS m ON m.event = e.i
        JOIN asset_partition_failures f ON f.code_location_id = l.cl
            AND f.asset_key = l.asset AND f.partition_ord = m.ord
        WHERE NOT l.alone
    ) THEN
        RAISE EXCEPTION 'failure rows of the partitions came in' USING ERRCODE = 'RV001';
    END IF;

    FOR v_n IN 1 .. cardinality(v_assets) LOOP
        PERFORM rivers_apply_events(v_cls[v_n], v_assets[v_n], v_alones[v_n],
            ARRAY(SELECT e FROM unnest(v_events) AS e
                WHERE e.cl = v_cls[v_n] AND e.asset = v_assets[v_n]),
            v_members, v_actions, p_single);
    END LOOP;
END
$rivers$;
