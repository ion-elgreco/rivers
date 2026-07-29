-- An asset's implicit pool stops excluding by slot count and starts excluding
-- by the partitions a step actually touches. A held slot now records that set,
-- so a `delete` on one partition no longer blocks a materialize of another.
--
-- `partitions` is a set (deduplicated) of rendered partition keys; NONE means
-- the whole asset, which conflicts with everything. `exclusive` marks an action
-- step — two non-exclusive holders never conflict, whatever they touch.
DEFINE FIELD IF NOT EXISTS partitions ON concurrency_slots TYPE option<set<string>>;
DEFINE FIELD IF NOT EXISTS exclusive ON concurrency_slots TYPE bool DEFAULT false;

-- DEFAULT only fills the field at create time, and `concurrency_slots` is
-- SCHEMAFULL — so live pre-V5 rows must be backfilled or the next UPDATE (lease
-- renewal) trips field coercion, and the conflict clause reads their missing
-- `exclusive` as NONE, which is falsy and would let a materialize run straight
-- past an in-flight exclusive action. Under the old scheme an exclusive holder
-- is exactly the one that consumed the whole pool, so its weight identifies it.
-- `partitions` stays NONE on those rows, i.e. whole-asset scope.
UPDATE concurrency_slots SET exclusive = (slots_consumed > 1) WHERE exclusive IS NONE;

-- NOT additive on the write side: a V5 writer records an exclusive action as
-- one slot plus `exclusive = true`, and a pre-V5 reader — which only counts
-- slots — would see a free pool and admit a materialize alongside a running
-- `delete`. Old readers stay safe (they still see the fields they know), so
-- min_reader holds at 2 while min_writer moves to 5.
UPSERT migration_meta:5 CONTENT { version: 5, class: 'breaking', min_reader: 2, min_writer: 5 };
