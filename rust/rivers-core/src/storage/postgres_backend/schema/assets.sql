CREATE TABLE assets (
    code_location_id                   text NOT NULL DEFAULT 'default',
    asset_key                          text NOT NULL,
    tags                               text[] NOT NULL DEFAULT '{}',
    kinds                              text[] NOT NULL DEFAULT '{}',
    asset_group                        text,
    code_version                       text,
    last_event_id                      text,
    last_run_id                        text,
    last_timestamp                     bigint,
    last_data_version                  text,
    last_materialization_code_version  text,
    last_input_data_versions           jsonb NOT NULL DEFAULT '[]'::jsonb,
    last_provenance_timestamp          bigint,
    last_deletion_timestamp            bigint,
    pool                               jsonb NOT NULL DEFAULT '[]'::jsonb,
    PRIMARY KEY (code_location_id, asset_key)
);
CREATE INDEX idx_assets_loc_group ON assets (code_location_id, asset_group);

-- Byte order for the keys, so the primary key gives the order of a listing.
CREATE TABLE dynamic_partitions (
    code_location_id     text NOT NULL DEFAULT 'default',
    partitions_def_name  text NOT NULL,
    partition_key        text COLLATE "C" NOT NULL,
    create_timestamp     bigint NOT NULL,
    PRIMARY KEY (code_location_id, partitions_def_name, partition_key)
);

-- The asset graph of each code location, as its last resolve wrote it.
CREATE TABLE graph_topologies (
    code_location_id  text PRIMARY KEY,
    topology          jsonb NOT NULL
);

-- One row per distinct static key list that the topology of a code location
-- names, by the digest of the list. Written with the topology.
CREATE TABLE partition_key_lists (
    code_location_id  text NOT NULL,
    digest            text NOT NULL,
    keys              text[] NOT NULL,
    PRIMARY KEY (code_location_id, digest)
);
