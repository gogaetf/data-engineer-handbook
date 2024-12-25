-- Create processed_events table
CREATE TABLE IF NOT EXISTS processed_events (
    ip VARCHAR,
    event_timestamp TIMESTAMP(3),
    referrer VARCHAR,
    host VARCHAR,
    url VARCHAR,
    geodata VARCHAR
);

-- Create processed_events_aggregated table
CREATE TABLE IF NOT EXISTS processed_events_aggregated (
    event_hour TIMESTAMP(3),
    host VARCHAR,
    num_hits BIGINT
);

-- Create processed_events_aggregated_source table
CREATE TABLE IF NOT EXISTS processed_events_aggregated_source (
    event_hour TIMESTAMP(3),
    host VARCHAR,
    referrer VARCHAR,
    num_hits BIGINT
);

-- Create processed_events_session_aggregated table
CREATE TABLE IF NOT EXISTS processed_events_session_aggregated (
    sw_start TIMESTAMP,
    sw_end TIMESTAMP,
    ip VARCHAR,
    host VARCHAR,
    num_hits BIGINT
);


