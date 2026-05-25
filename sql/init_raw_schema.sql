-- run once to setup the db before ingestion

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.consent_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    purpose VARCHAR(30) NOT NULL,
    channel VARCHAR(20) NOT NULL,
    policy_version_id VARCHAR(10) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL, 
    metadata JSONB,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.dsr_requests(
    request_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    request_type VARCHAR(20) NOT NULL,
    submitted_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL,
);
