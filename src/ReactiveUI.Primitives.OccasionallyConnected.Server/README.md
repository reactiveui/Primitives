# ReactiveUI.Primitives.OccasionallyConnected.Server

Server-side stream and conflict-resolution primitives for authenticated synchronization. The package includes an in-memory or SQLite-backed stream hub, conflict resolvers, server domain handlers, and durable journal support.

## Offline retention

The default journal retains terminal operation results for 30 days.
Configure `ServerCommitJournalLimits.OperationRetention` for your expected offline interval.
Finite row and byte limits still apply. The server rejects new work at capacity instead of dropping unexpired proofs.
Set `ReceiveHistoryRetention` to a shorter interval when subscribers should recover from a snapshot instead.
This expires subscriber history without deleting operation replay proofs.
Event data inside a retained operation response remains until operation expiry, so replay results stay identical.
Receive history cannot outlive operation retention.

Exactly-once means exactly-once effect within the negotiated deduplication window.
Deduplication means recognizing an operation that the server has already applied.
The engine persists the first-attempt time before sending.
After that window expires, the default policy stops the operation as `GuaranteeExpired` before another send.
A restart does not reset that time. An operation never sent before can still start its first attempt after a long offline interval.
`FallbackToAtLeastOnce` is an explicit opt-in that can permit duplicate effects after expiry.
Applications must reconcile expired operations instead of assuming they were delivered.
At-least-once delivery requires application idempotency once the server has discarded its replay proof.

## Install

```bash
dotnet add package ReactiveUI.Primitives.OccasionallyConnected.Server
```

Source builds target `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48`, and `net481`.
It depends on the core contracts, `SQLitePCLRaw.core`, and `SQLite3MC.PCLRaw.bundle`.
The bundle supplies the SQLite3 Multiple Ciphers native library. Do not add another SQLite native bundle.

## Release assets

A package asset is a library built for one target framework.
Stable package versions omit .NET 11 preview assets and their dependency groups.
A .NET 11 app that installs a stable version uses the compatible .NET 10 asset.
Prerelease versions include .NET 11 preview assets from the source targets you build.

## Use

Create a `ServerStreamHub` with `ServerStreamHub.CreateInMemory(options)` for process-local use or `ServerStreamHub.CreateSqlite(databasePath, options)` for a durable server journal. Register stream handlers and authorization policies for the data your service exposes.

The [collaboration server example](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.Collaboration.Server/README.md) shows an HTTP service using a durable hub. [ResilienceLab](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.ResilienceLab/README.md) demonstrates retries, duplicate delivery, and conflict behavior.

This package provides server primitives; host endpoints and application authentication are configured by your service. This is the first V1 release of the feature, with no earlier version to migrate from.

## Native journal storage

The journal calls SQLite directly through internal native handle owners shared with the SQLite store.
It binds values as SQL parameters. It does not pool connections.
Writer transactions take the database lock before changing journal state.
WAL journaling and FULL synchronous writes preserve atomic commits after a process crash.

The hub runs synchronous SQLite commands on bounded dedicated workers, not request threads.
Effect calls and acknowledgements have separate worker capacity.
Cancellation removes commands that have not started. Disposal joins a command that has already started.
An active native command may still wait for SQLite's bounded busy timeout.
Domain handlers and authorization policies stay asynchronous.
The processor prepares at most eight operations against sequential projected state.
It does this before opening a write transaction. Each preparation must be side-effect-free.
The processor checks each plan against the journal limits before adding it to the group.
One physical transaction then checks each operation's own revision, capacity and replay proof.
It advances state and revision once per accepted operation.
This shares one durable disk sync across up to eight operations.
Receipts become visible only after that transaction commits.
If one plan fails admission, the journal stops applying dependent plans.
The processor re-prepares stale plans within the existing retry bound.
A later preparation failure or cancellation still flushes the valid earlier prefix.
It does not admit the failing or canceled preparation.

Unchanged registration and empty receive polls use read transactions.
They do not refresh durable timestamps or reserve the writer lock.
New bindings, resolved anchors, offered pages and acknowledgements persist their changes.
Subscription retention starts at the last durable change, not the latest empty poll.
An active subscriber reports a retention gap if compaction replaces its binding.
It does not silently restart a `Latest` anchor and skip a new event.
One publish broadcasts to every waiting subscriber after it releases its active-call slot.
Poll timers still detect changes made by another process.

Replay reads select only the requested operations.
Receive reads select a bounded range of complete groups and their cursor anchor.
The journal batches child events, metadata and conflicts instead of querying each row.
Indexes support those reads and expiry cleanup.
Transactional counters enforce shared storage bounds without scanning the store for each request.
Opening an existing journal seeds these counters once without changing its retained data.

Native failures use `SqliteDatabaseException` from the core contracts.
Its `SqliteErrorCode` and `SqliteExtendedErrorCode` properties preserve SQLite's result codes.
The public hub does not expose native handles or accept a database passphrase.
