-- Indexes that only reads need. Partial where a read takes one kind of
-- event, so that the other events do not pay for it on each write.
CREATE INDEX idx_events_observations ON events (code_location_id, timestamp)
    WHERE event_type = 'Observation';
