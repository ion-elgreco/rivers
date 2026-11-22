-- Key-value entries: the code location directory and the state that has no
-- table of its own. Byte order for the keys, so the entries under a prefix
-- are a range of the primary key.
CREATE TABLE kv (
    key    text COLLATE "C" PRIMARY KEY,
    value  bytea NOT NULL
);

-- The processes without a directory entry that open the store: the
-- operator and the UI.
CREATE TABLE components (
    name            text COLLATE "C" PRIMARY KEY,
    schema_version  bigint NOT NULL,
    rivers_version  text NOT NULL,
    writes          boolean NOT NULL,
    started_at      bigint NOT NULL
);
