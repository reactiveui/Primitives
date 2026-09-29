# ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem

This package provides a durable file-backed local store. It persists subscription
identity, snapshots, pending operations, leases, retry state, remote inbox entries,
operation results, and dead letters in a checksummed journal.

The store flushes each complete journal record to disk before it acknowledges a
commit. On startup, it restores the last complete record and discards an incomplete
tail. Compaction rewrites the current state into a new journal file and atomically
replaces the old file.

The adapter supports one open process per store directory. It rejects authenticated
encryption-at-rest requirements. Use a local filesystem that supports exclusive file
sharing and atomic file replacement.
