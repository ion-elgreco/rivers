-- One row per asset that declares a version of its stored data: the version
-- of the data, and the state of a migration to the version that the code
-- declares. `revision` goes up by one with each write: a writer that read the
-- row writes it back only if the revision did not change in between.
CREATE TABLE asset_versions (
    code_location_id  text NOT NULL,
    asset_key         text NOT NULL,
    applied_version   bigint NOT NULL,
    partitions        text,
    state             text NOT NULL,
    target_version    bigint,
    reason            text,
    started_at        bigint,
    run_id            text,
    backfill_id       text,
    owner             text,
    lease_expires_at  bigint,
    revision          bigint NOT NULL,
    updated_at        bigint NOT NULL,
    PRIMARY KEY (code_location_id, asset_key)
);

-- One row per version that the stored data of an asset reached.
CREATE TABLE asset_version_history (
    code_location_id  text NOT NULL,
    asset_key         text NOT NULL,
    version           bigint NOT NULL,
    kind              text NOT NULL,
    breaking          boolean NOT NULL,
    run_id            text,
    backfill_id       text,
    filled            bigint,
    failed            bigint,
    finished_at       bigint NOT NULL,
    PRIMARY KEY (code_location_id, asset_key, version)
);

-- The UI server listens to the version rows, as to the tables of
-- `schema/notify.sql`.
CREATE TRIGGER asset_versions_notify_insert AFTER INSERT ON asset_versions
    REFERENCING NEW TABLE AS changed
    FOR EACH STATEMENT EXECUTE FUNCTION rivers_notify_changed();
CREATE TRIGGER asset_versions_notify_update AFTER UPDATE ON asset_versions
    REFERENCING NEW TABLE AS changed
    FOR EACH STATEMENT EXECUTE FUNCTION rivers_notify_changed();
CREATE TRIGGER asset_versions_notify_delete AFTER DELETE ON asset_versions
    REFERENCING OLD TABLE AS changed
    FOR EACH STATEMENT EXECUTE FUNCTION rivers_notify_changed();

-- Additive: no older build reads or writes the tables. The V17 floors carry
-- forward.
INSERT INTO migration_meta (version, class, min_reader, min_writer)
VALUES (18, 'additive', 14, 17);
