# OccasionallyConnected compatibility fixtures

This matrix maps the current compatibility rules to executable fixtures. Protocol versions, payload schema versions and database schema versions are separate values. A package version does not select any of them.

## HTTP and payloads

| Surface | Supported behavior | Fixture |
| --- | --- | --- |
| HTTP envelope | The endpoint accepts protocol major 1. Versioned content uses `v=1`. | `HttpServerEndpointTests.Validation` and `HttpProtocolCodecTests` test accepted and rejected versions. |
| Connect version range | The codec preserves the offered range, including 1.0 through 1.1. The peer must select a compatible version and capabilities. | `HttpProtocolCodecTests.Server.DeserializeConnectRequestReadsClientWireClaim` checks both ends of the range. |
| JSON messages | Fixed JSON fixtures cover connect, push, subscribe, ACK, operation results, payloads and event batches. Unknown fields can be ignored. Duplicate members and invalid required values fail validation. | `HttpProtocolJsonContextTests` contains the golden JSON and invalid-input fixtures. |
| Payload schema | The application registers each contract and target type. Older payloads need a complete sequence of registered upcasters. | `JsonPayloadSerializerTests` tests canonical hashes, contiguous and multi-step upcasts, unknown contracts and corrupt payloads. `SchemaRegistryTests` rejects missing or ambiguous chains. |
| Snapshot metadata | Snapshot format and payload schema are validated separately. Cursor and stream identity must agree with stored state. | `SnapshotRecoveryValidatorTests`, `InMemoryLocalStoreAdapterTests` and `SqliteLocalStoreAdapterTests` test malformed metadata and recovery bounds. |

The connect range fixture proves how the codec carries a range. It does not promise every feature at every minor version. Use the negotiated capabilities when selecting a delivery guarantee.

## Local SQLite migrations

The current local commit schema is version 8. Initialization validates the stored schema before migrating it. Invalid metadata, unsupported newer schemas and schema drift fail without silently repairing the database.

| Starting schema | State preserved or introduced | Fixture |
| --- | --- | --- |
| 1 | Subscription identity survives creation of the local commit tables. | `SqliteLocalCommitStoreTests.WhenIdentitySchemaMigratesToLocalCommitSchema_ThenExistingIdentityIsPreserved` |
| 2 | Committed outbox and snapshot rows survive migration. | `SqliteLocalCommitStoreTests.Remote.WhenLegacyLocalCommitSchemaMigratesToCurrent_ThenCommittedRowsArePreserved` |
| 3 | Existing pending rows become leaseable. | `SqliteLocalCommitStoreTests.Leases.WhenRemoteApplySchemaMigratesToCurrent_ThenPendingRowsCanBeLeased` |
| 4 | Existing operations receive operation-state records. | `SqliteLocalCommitStoreTests.OperationState.WhenSchemaFourMigratesToCurrent_ThenOperationStateIsBackfilled` |
| 5 | Optimistic state and pending operations survive. Missing authoritative state remains unknown. | `SqliteLocalCommitStoreTests.AuthoritativeState.WhenSchemaFiveMigrates_ThenOptimisticAndPendingRecoverWithUnknownAuthoritativeState` |
| 6 | Accepted operations remain available for replay when receive inclusion is unknown. | `SqliteLocalCommitStoreTests.AuthoritativeState.WhenSchemaSixMigrates_ThenAcceptedOperationsRecoverAsReplayVisibleUnknownInclusion` |
| 7 | The migrated store can persist quarantine markers. | `SqliteLocalCommitStoreTests.Leases.WhenPreQuarantineSchemaMigratesToCurrent_ThenQuarantineMarkersCanBeWritten` |
| 8 | Reopening preserves current durable state. | `ILocalStoreAdapterTests.SqliteDurableCapabilitiesReopenOperationInboxCursorSnapshotAndStatusState` |

Historical fixtures are kept in the SQLite test project. They include frozen SQL independent of current schema construction. The migration rollback tests also verify that failed migration leaves the old database usable.

These fixtures cover the formats implemented in this repository. They do not claim compatibility with unpublished historical package releases. Final release validation must run the current tests against the exact packages being shipped.
