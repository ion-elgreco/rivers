-- A run records when it began to execute. `start_time` is when the run was
-- created: the wait of a queued run showed as a part of its duration.
DEFINE FIELD IF NOT EXISTS execution_start_time ON runs TYPE option<int>;

-- An ended run from before this version gets the best time that the store has.
-- It began if it succeeded or if one of its steps started. It began when it
-- left the queue, or when it was created if it never was in the queue.
-- A run in progress is left alone: its executor writes that row.
UPDATE runs SET execution_start_time =
    IF status = 'Success' OR (SELECT VALUE id FROM events
        WHERE run_id = $parent.run_id AND event_type = 'StepStart' LIMIT 1)[0] != NONE
    {
        (SELECT VALUE timestamp FROM events
            WHERE run_id = $parent.run_id AND event_type = 'RunDequeued'
            ORDER BY timestamp DESC LIMIT 1)[0]
        ?? IF (SELECT VALUE id FROM events
            WHERE run_id = $parent.run_id AND event_type = 'RunQueued' LIMIT 1)[0] = NONE
            { start_time } ELSE { NONE }
    }
    ELSE { NONE }
    WHERE status IN ['Success', 'Failure', 'Canceled'];

-- Additive: an older reader ignores the field, and an older writer leaves it
-- out (its runs show no duration). The V14 floors carry forward.
UPSERT migration_meta:15 CONTENT { version: 15, class: 'additive', min_reader: 14, min_writer: 14 };
