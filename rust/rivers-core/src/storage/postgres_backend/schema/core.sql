-- rivers on PostgreSQL: the schema at version 17.
--
-- Times are nanoseconds in bigint. Flexible objects and lists are jsonb.
-- A row with a partition key has `partition_key jsonb` (the key) and
-- `partition_ord bytea` (bytes that sort and compare as the key does).

-- One row per applied migration: its compatibility class and the floors it set.
CREATE TABLE migration_meta (
    version     bigint PRIMARY KEY,
    class       text NOT NULL,
    min_reader  bigint NOT NULL,
    min_writer  bigint NOT NULL
);
INSERT INTO migration_meta (version, class, min_reader, min_writer)
VALUES (17, 'breaking', 14, 17);

-- Change notifications for `subscribe_table`: one per statement, not per
-- row. The payload is the schema, so two rivers schemas of one database do
-- not wake the listeners of each other.
CREATE FUNCTION rivers_notify() RETURNS trigger AS $rivers$
BEGIN
    PERFORM pg_notify('rivers_' || TG_TABLE_NAME, TG_TABLE_SCHEMA);
    RETURN NULL;
END;
$rivers$ LANGUAGE plpgsql;

-- The same, but silent for a statement that changed no row. Its trigger
-- names the rows of the statement `changed`.
CREATE FUNCTION rivers_notify_changed() RETURNS trigger AS $rivers$
BEGIN
    IF EXISTS (SELECT FROM changed) THEN
        PERFORM pg_notify('rivers_' || TG_TABLE_NAME, TG_TABLE_SCHEMA);
    END IF;
    RETURN NULL;
END;
$rivers$ LANGUAGE plpgsql;
