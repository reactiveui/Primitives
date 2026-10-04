# ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem

This package provides a durable file-backed local store. It persists subscription
identity, snapshots, pending operations, leases, retry state, remote inbox entries,
operation results, and dead letters in a checksummed journal.

The store flushes each complete journal record to disk before it acknowledges a
commit. Each mutation appends one transaction with changed records. It does not
append the full retained store history. A transaction stores a replacement snapshot
only when that snapshot changes.

On startup, the store replays complete records and discards only a final record
whose header, payload, or checksum was cut short. It rejects complete records with
invalid headers, checksums, or contents. It never resets corrupt data.
Recovery rejects a transaction that moves the next client sequence backward.
Every retained operation must have a lower sequence than the next client sequence.
Each changed stream includes a snapshot field. A metadata-only transaction sets
that field to null and keeps the current checkpoint. The store rejects a replacement
that clears an existing checkpoint.
Compaction writes one full state snapshot into a new journal file and atomically
replaces the old file.

The store reads existing version-one full-state journals. New mutations append
version-two transactions after those records without rewriting them. Opening an
unchanged store does not append another copy of its state. Compaction produces a
version-one snapshot followed by version-two transactions. Older package versions
cannot read version-two transactions. Do not downgrade a store after it writes one.

The adapter supports one open process per store directory. It rejects authenticated
encryption-at-rest requirements. Use a local filesystem that supports exclusive file
sharing and atomic file replacement.

## Release assets

A package asset is a library built for one target framework.
Stable package versions omit .NET 11 preview assets and their dependency groups.
A .NET 11 app that installs a stable version uses the compatible .NET 10 asset.
Prerelease versions include .NET 11 preview assets from the source targets you build.
