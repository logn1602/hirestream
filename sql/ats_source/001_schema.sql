-- ATS source schema (SPEC §7.4): a vendor schema the pipeline adapts to, never the reverse.
-- Timestamps are `timestamp without time zone` holding UTC, a documented quirk of this vendor.
-- The generator bulk-loads the final state here with COPY (ADR-0008); T2.4 extracts it
-- incrementally by `updated_at` and `change_id`. Tables are created in foreign-key order.

CREATE TABLE candidates (
    candidate_id      text PRIMARY KEY,
    first_name        text NOT NULL,
    last_name         text NOT NULL,
    email             text NOT NULL,
    phone             text,
    location_city     text NOT NULL,
    location_country  text NOT NULL,
    is_internal       boolean NOT NULL,
    employee_id       text,
    created_at        timestamp NOT NULL,
    updated_at        timestamp NOT NULL,
    CONSTRAINT candidates_internal_has_employee CHECK (is_internal = (employee_id IS NOT NULL))
);

CREATE TABLE requisitions (
    req_id            text PRIMARY KEY,
    title             text NOT NULL,
    role_family       text NOT NULL,
    job_level         text NOT NULL,
    org               text NOT NULL,
    team              text NOT NULL,
    location_city     text NOT NULL,
    headcount         integer NOT NULL CHECK (headcount > 0),
    hiring_manager_id text NOT NULL,
    recruiter_id      text,
    status            text NOT NULL CHECK (status IN ('open', 'on_hold', 'filled', 'cancelled')),
    is_evergreen      boolean NOT NULL,
    is_internal_only  boolean NOT NULL,
    opened_at         timestamp NOT NULL,
    closed_at         timestamp,
    created_at        timestamp NOT NULL,
    updated_at        timestamp NOT NULL,
    CONSTRAINT requisitions_closed_when_final
        CHECK ((status IN ('filled', 'cancelled')) = (closed_at IS NOT NULL)),
    CONSTRAINT requisitions_times_ordered
        CHECK (opened_at <= updated_at AND (closed_at IS NULL OR closed_at <= updated_at))
);

CREATE TABLE applications (
    application_id    text PRIMARY KEY,
    candidate_id      text NOT NULL REFERENCES candidates,
    req_id            text NOT NULL REFERENCES requisitions,
    source_channel    text NOT NULL
        CHECK (source_channel IN ('career_site', 'referral', 'sourced', 'agency', 'internal')),
    applied_at        timestamp NOT NULL,
    current_stage     text NOT NULL
        CHECK (current_stage IN ('applied', 'recruiter_screen', 'phone_screen', 'onsite', 'offer')),
    status            text NOT NULL CHECK (status IN (
        'active', 'rejected', 'withdrawn', 'hired', 'offer_declined', 'no_start')),
    status_reason     text,
    updated_at        timestamp NOT NULL
);

CREATE TABLE offers (
    offer_id          text PRIMARY KEY,
    application_id    text NOT NULL UNIQUE REFERENCES applications,
    extended_at       timestamp NOT NULL,
    status            text NOT NULL
        CHECK (status IN ('extended', 'accepted', 'declined', 'rescinded')),
    decided_at        timestamp,
    start_date        date,
    updated_at        timestamp NOT NULL
);

-- Append-only history. change_id is loaded with the generator's ids; the loader then moves the
-- sequence past the maximum so later live-tail inserts (T3.4) continue it.
CREATE TABLE application_stage_changes (
    change_id         bigserial PRIMARY KEY,
    application_id    text NOT NULL REFERENCES applications,
    from_stage        text
        CHECK (from_stage IN ('applied', 'recruiter_screen', 'phone_screen', 'onsite', 'offer')),
    to_stage          text NOT NULL
        CHECK (to_stage IN ('applied', 'recruiter_screen', 'phone_screen', 'onsite', 'offer')),
    from_status       text,
    to_status         text NOT NULL,
    reason            text NOT NULL,
    changed_at        timestamp NOT NULL,
    changed_by        text NOT NULL
);

-- SPEC §7.4: index every updated_at (change_id is indexed by its primary key).
CREATE INDEX candidates_updated_at ON candidates (updated_at);
CREATE INDEX requisitions_updated_at ON requisitions (updated_at);
CREATE INDEX applications_updated_at ON applications (updated_at);
CREATE INDEX offers_updated_at ON offers (updated_at);
