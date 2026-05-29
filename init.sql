CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE sensor_events (
    time TIMESTAMPTZ NOT NULL,
    device_id TEXT,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION
);

SELECT create_hypertable('sensor_events', 'time');
