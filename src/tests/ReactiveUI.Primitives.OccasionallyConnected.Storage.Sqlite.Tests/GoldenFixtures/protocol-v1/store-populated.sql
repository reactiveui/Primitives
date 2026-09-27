-- Store written by the frozen release. Replay it into an empty database file.
-- table oc_inbox
CREATE TABLE oc_inbox (
    store_identity TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    server_cursor TEXT NOT NULL,
    committed_at_utc TEXT NOT NULL,
    PRIMARY KEY (store_identity, stream_id, event_id),
    FOREIGN KEY (store_identity, stream_id)
        REFERENCES oc_streams (store_identity, stream_id)
        ON DELETE CASCADE);
-- table oc_metadata
CREATE TABLE oc_metadata (key TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL);
-- table oc_outbox
CREATE TABLE oc_outbox (
    store_identity TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    client_sequence INTEGER NOT NULL,
    timestamp_utc TEXT NOT NULL,
    base_version TEXT NULL,
    operation_type INTEGER NOT NULL,
    payload_contract_id TEXT NOT NULL,
    payload_schema_version INTEGER NOT NULL,
    payload_content_type TEXT NOT NULL,
    payload BLOB NOT NULL,
    payload_hash TEXT NOT NULL,
    policy_delivery_guarantee INTEGER NOT NULL,
    policy_durability INTEGER NOT NULL,
    policy_priority INTEGER NOT NULL,
    policy_conflict INTEGER NOT NULL,
    snapshot_revision INTEGER NOT NULL,
    committed_at_utc TEXT NOT NULL,
    commit_fingerprint BLOB NOT NULL,
    PRIMARY KEY (store_identity, operation_id),
    UNIQUE (store_identity, stream_id, client_sequence),
    FOREIGN KEY (store_identity, stream_id)
        REFERENCES oc_streams (store_identity, stream_id)
        ON DELETE CASCADE);
-- table oc_outbox_authoritative_mutations
CREATE TABLE oc_outbox_authoritative_mutations (
    store_identity TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    payload_contract_id TEXT NOT NULL,
    payload_schema_version INTEGER NOT NULL,
    payload_content_type TEXT NOT NULL,
    payload BLOB NOT NULL,
    payload_hash TEXT NOT NULL,
    PRIMARY KEY (store_identity, operation_id),
    FOREIGN KEY (store_identity, operation_id)
        REFERENCES oc_outbox (store_identity, operation_id)
        ON DELETE CASCADE);
-- table oc_outbox_leases
CREATE TABLE oc_outbox_leases (
    store_identity TEXT NOT NULL,
    lease_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    client_sequence INTEGER NOT NULL,
    lease_expires_at_utc TEXT NOT NULL,
    lease_member_count INTEGER NOT NULL,
    PRIMARY KEY (store_identity, lease_id, operation_id),
    UNIQUE (store_identity, operation_id),
    FOREIGN KEY (store_identity, operation_id)
        REFERENCES oc_outbox (store_identity, operation_id)
        ON DELETE CASCADE,
    FOREIGN KEY (store_identity, stream_id)
        REFERENCES oc_streams (store_identity, stream_id)
        ON DELETE CASCADE);
-- table oc_outbox_metadata
CREATE TABLE oc_outbox_metadata (
    store_identity TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (store_identity, operation_id, key),
    FOREIGN KEY (store_identity, operation_id)
        REFERENCES oc_outbox (store_identity, operation_id)
        ON DELETE CASCADE);
-- table oc_outbox_operation_states
CREATE TABLE oc_outbox_operation_states (
    store_identity TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    operation_state INTEGER NOT NULL,
    attempt_count INTEGER NOT NULL,
    changed_at_utc TEXT NOT NULL,
    reason_code TEXT NULL,
    retry_started_utc TEXT NULL,
    retry_due_utc TEXT NULL,
    retry_previous_delay_ticks INTEGER NULL,
    retry_transient_attempt_count INTEGER NULL,
    retry_authentication_state INTEGER NULL,
    retry_credentials_version TEXT NULL,
    PRIMARY KEY (store_identity, operation_id),
    FOREIGN KEY (store_identity, operation_id)
        REFERENCES oc_outbox (store_identity, operation_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE);
-- table oc_outbox_receive_inclusions
CREATE TABLE oc_outbox_receive_inclusions (
    store_identity TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    PRIMARY KEY (store_identity, operation_id),
    FOREIGN KEY (store_identity, operation_id)
        REFERENCES oc_outbox (store_identity, operation_id)
        ON DELETE CASCADE);
-- table oc_payload_quarantine
CREATE TABLE oc_payload_quarantine (
    store_identity TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    quarantine_id TEXT NOT NULL,
    subscription_id TEXT NULL,
    operation_id TEXT NULL,
    event_id TEXT NULL,
    source INTEGER NOT NULL,
    reason INTEGER NOT NULL,
    reason_code TEXT NULL,
    cursor TEXT NULL,
    evidence_contract_id TEXT NULL,
    evidence_schema_version INTEGER NULL,
    evidence_content_type TEXT NULL,
    evidence_payload_length INTEGER NOT NULL,
    evidence_payload_hash TEXT NULL,
    evidence_payload_prefix BLOB NOT NULL,
    observed_at_utc TEXT NOT NULL,
    PRIMARY KEY (store_identity, stream_id),
    FOREIGN KEY (store_identity, stream_id)
        REFERENCES oc_streams (store_identity, stream_id)
        ON DELETE CASCADE);
-- table oc_snapshot_authoritative_states
CREATE TABLE oc_snapshot_authoritative_states (
    store_identity TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    payload_contract_id TEXT NOT NULL,
    payload_schema_version INTEGER NOT NULL,
    payload_content_type TEXT NOT NULL,
    payload BLOB NOT NULL,
    payload_hash TEXT NOT NULL,
    PRIMARY KEY (store_identity, stream_id),
    FOREIGN KEY (store_identity, stream_id)
        REFERENCES oc_snapshots (store_identity, stream_id)
        ON DELETE CASCADE);
-- table oc_snapshots
CREATE TABLE oc_snapshots (
    store_identity TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    format_version INTEGER NOT NULL,
    server_cursor TEXT NULL,
    payload_contract_id TEXT NOT NULL,
    payload_schema_version INTEGER NOT NULL,
    payload_content_type TEXT NOT NULL,
    payload BLOB NOT NULL,
    payload_hash TEXT NOT NULL,
    revision INTEGER NOT NULL,
    saved_at_utc TEXT NOT NULL,
    PRIMARY KEY (store_identity, stream_id),
    FOREIGN KEY (store_identity, stream_id)
        REFERENCES oc_streams (store_identity, stream_id)
        ON DELETE CASCADE);
-- table oc_streams
CREATE TABLE oc_streams (
    store_identity TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    subscription_id TEXT NOT NULL,
    next_client_sequence INTEGER NOT NULL,
    server_cursor TEXT NULL,
    PRIMARY KEY (store_identity, stream_id),
    FOREIGN KEY (store_identity, stream_id)
        REFERENCES oc_subscription_identities (store_identity, stream_id)
        ON DELETE CASCADE);
-- table oc_subscription_identities
CREATE TABLE oc_subscription_identities (
    store_identity TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    subscription_id TEXT NOT NULL,
    PRIMARY KEY (store_identity, stream_id));
INSERT INTO oc_inbox VALUES ('golden-client', 'sensor/temperature', '00000000-0000-0000-0000-000000000201', 'cursor-1', '2026-09-13T00:00:10.0000000+00:00');
INSERT INTO oc_metadata VALUES ('schema_version', '8');
INSERT INTO oc_outbox VALUES ('golden-client', '00000000-0000-0000-0000-000000000001', 'sensor/temperature', 1, '2026-09-13T00:00:10.0000000+00:00', 'cursor-1', 0, 'temperature-reading', 1, 'application/json', X'7B2276616C7565223A32322E312C22756E6974223A2243227D', 'sha256-AHrmRLT/fVIWmAB/ynZthFtmjIjyxB262Fj0y6tmYUY=', 1, 0, 0, 1, 2, '2026-09-13T00:00:10.0000000+00:00', X'9B467AC2A82967B5CCBC9EFAFE50D91EE752CFFE79A62982F9668CDE26BE5151');
INSERT INTO oc_outbox VALUES ('golden-client', '00000000-0000-0000-0000-000000000002', 'sensor/temperature', 2, '2026-09-13T00:00:10.0000000+00:00', 'cursor-1', 0, 'temperature-reading', 1, 'application/json', X'7B2276616C7565223A32322E312C22756E6974223A2243227D', 'sha256-AHrmRLT/fVIWmAB/ynZthFtmjIjyxB262Fj0y6tmYUY=', 1, 0, 0, 1, 3, '2026-09-13T00:00:10.0000000+00:00', X'D4036A1308F54A7D872DA8E1F02C6880EABC95A70A70196E6867A73C88AD07A2');
INSERT INTO oc_outbox_metadata VALUES ('golden-client', '00000000-0000-0000-0000-000000000001', 'origin', 'unit-test');
INSERT INTO oc_outbox_metadata VALUES ('golden-client', '00000000-0000-0000-0000-000000000002', 'origin', 'unit-test');
INSERT INTO oc_outbox_operation_states VALUES ('golden-client', '00000000-0000-0000-0000-000000000001', 1, 0, '2026-09-13T00:00:10.0000000+00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO oc_outbox_operation_states VALUES ('golden-client', '00000000-0000-0000-0000-000000000002', 1, 0, '2026-09-13T00:00:10.0000000+00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO oc_snapshots VALUES ('golden-client', 'sensor/temperature', 1, 'cursor-1', 'temperature-state', 1, 'application/json', X'7B2272656164696E6773223A332C226C617374223A32322E317D', 'sha256-P5xyzqNgFYRDlrUn+Ckvesp71hydfu3VHQUAmFfkaco=', 3, '2026-09-13T00:00:10.0000000+00:00');
INSERT INTO oc_streams VALUES ('golden-client', 'sensor/temperature', '00000000-0000-0000-0000-000000000301', 3, 'cursor-1');
INSERT INTO oc_subscription_identities VALUES ('golden-client', 'sensor/temperature', '00000000-0000-0000-0000-000000000301');
PRAGMA user_version = 8;
