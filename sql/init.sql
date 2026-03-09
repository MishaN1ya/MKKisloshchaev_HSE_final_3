CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS dm;

CREATE TABLE IF NOT EXISTS raw.user_sessions (
    session_id      TEXT        NOT NULL,
    user_id         TEXT        NOT NULL,
    start_time      TIMESTAMPTZ NOT NULL,
    end_time        TIMESTAMPTZ,
    pages_visited   TEXT[],
    device_type     TEXT,
    device_os       TEXT,
    actions         TEXT[],
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (session_id, start_time)
) PARTITION BY RANGE (start_time);

CREATE TABLE IF NOT EXISTS raw.user_sessions_2026_01 PARTITION OF raw.user_sessions FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE IF NOT EXISTS raw.user_sessions_2026_02 PARTITION OF raw.user_sessions FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE IF NOT EXISTS raw.user_sessions_2026_03 PARTITION OF raw.user_sessions FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS raw.user_sessions_default  PARTITION OF raw.user_sessions DEFAULT;

CREATE TABLE IF NOT EXISTS raw.event_logs (
    event_id    TEXT        NOT NULL,
    ts          TIMESTAMPTZ NOT NULL,
    event_type  TEXT,
    page        TEXT,
    product_id  TEXT,
    user_id     TEXT,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, ts)
) PARTITION BY RANGE (ts);

CREATE TABLE IF NOT EXISTS raw.event_logs_2026_01 PARTITION OF raw.event_logs FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE IF NOT EXISTS raw.event_logs_2026_02 PARTITION OF raw.event_logs FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE IF NOT EXISTS raw.event_logs_2026_03 PARTITION OF raw.event_logs FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS raw.event_logs_default  PARTITION OF raw.event_logs DEFAULT;

CREATE TABLE IF NOT EXISTS raw.support_tickets (
    ticket_id     TEXT        NOT NULL PRIMARY KEY,
    user_id       TEXT,
    status        TEXT,
    issue_type    TEXT,
    created_at    TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ,
    message_count INT,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.user_recommendations (
    user_id              TEXT        NOT NULL PRIMARY KEY,
    recommended_products TEXT[],
    last_updated         TIMESTAMPTZ,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.moderation_queue (
    review_id         TEXT     NOT NULL PRIMARY KEY,
    user_id           TEXT,
    product_id        TEXT,
    review_text       TEXT,
    rating            SMALLINT,
    moderation_status TEXT,
    flags             TEXT[],
    submitted_at      TIMESTAMPTZ,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dm.user_activity (
    report_date        DATE    NOT NULL,
    user_id            TEXT    NOT NULL,
    total_sessions     INT,
    total_duration_min NUMERIC(10,2),
    avg_duration_min   NUMERIC(10,2),
    unique_pages       INT,
    total_actions      INT,
    most_used_device   TEXT,
    PRIMARY KEY (report_date, user_id)
);

CREATE TABLE IF NOT EXISTS dm.support_efficiency (
    report_date        DATE    NOT NULL,
    issue_type         TEXT    NOT NULL,
    status             TEXT    NOT NULL,
    ticket_count       INT,
    avg_resolution_hrs NUMERIC(10,2),
    max_resolution_hrs NUMERIC(10,2),
    PRIMARY KEY (report_date, issue_type, status)
);
