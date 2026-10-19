// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem.Tests;

/// <summary>Tests sparse state comparisons and fail-closed transaction validation.</summary>
public sealed class FileSystemJournalDeltaTests
{
    /// <summary>The stream used by valid state fixtures.</summary>
    private const string StreamName = "delta-validation";

    /// <summary>A different stream used to corrupt durable identities.</summary>
    private const string OtherStreamName = "other-stream";

    /// <summary>The identity of the valid fixture store.</summary>
    private const string StoreIdentity = "delta-store";

    /// <summary>The client identity whose publication is fenced by replay validation.</summary>
    private const string BoundClientIdentity = "bound-client";

    /// <summary>The cursor used to detect a partially published failed transaction.</summary>
    private const string UncommittedCursor = "uncommitted-cursor";

    /// <summary>Verifies every required snapshot collection is checked before replay.</summary>
    /// <param name="field">The missing snapshot field.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(nameof(FileSystemLocalStoreAdapter.StoreState.StoreIdentity))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.StoreState.Streams))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.StoreState.Leases))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.StoreState.Inbox))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.StoreState.IncludedOperations))]
    public async Task SnapshotRejectsMissingCollections(string field)
    {
        var state = field switch
        {
            nameof(FileSystemLocalStoreAdapter.StoreState.StoreIdentity) => new FileSystemLocalStoreAdapter.StoreState(),
            nameof(FileSystemLocalStoreAdapter.StoreState.Streams) => new() { StoreIdentity = StoreIdentity, Streams = null!, },
            nameof(FileSystemLocalStoreAdapter.StoreState.Leases) => new() { StoreIdentity = StoreIdentity, Leases = null!, },
            nameof(FileSystemLocalStoreAdapter.StoreState.Inbox) => new() { StoreIdentity = StoreIdentity, Inbox = null!, },
            _ => new() { StoreIdentity = StoreIdentity, IncludedOperations = null!, },
        };
        await Assert.That(() => FileSystemJournalDelta.Validate(new(state), false)).Throws<InvalidDataException>();
    }

    /// <summary>Verifies missing stream containers fail closed in snapshots.</summary>
    /// <param name="missingStream">Whether the stream itself is missing.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SnapshotRejectsMissingStreamData(bool missingStream)
    {
        var state = new FileSystemLocalStoreAdapter.StoreState { StoreIdentity = StoreIdentity };
        state.Streams[StreamName] = missingStream ? null! : new() { DeadLetters = null! };
        await Assert.That(() => FileSystemJournalDelta.Validate(new(state), false)).Throws<InvalidDataException>();
    }

    /// <summary>Verifies every required incremental collection is checked before replay.</summary>
    /// <param name="field">The missing transaction field.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(nameof(FileSystemLocalStoreAdapter.JournalDelta.Streams))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.JournalDelta.Leases))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.JournalDelta.RemovedLeases))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.JournalDelta.Inbox))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.JournalDelta.IncludedOperations))]
    public async Task TransactionRejectsMissingCollections(string field)
    {
        var delta = field switch
        {
            nameof(FileSystemLocalStoreAdapter.JournalDelta.Streams) => new FileSystemLocalStoreAdapter.JournalDelta { Streams = null! },
            nameof(FileSystemLocalStoreAdapter.JournalDelta.Leases) => new() { Leases = null! },
            nameof(FileSystemLocalStoreAdapter.JournalDelta.RemovedLeases) => new() { RemovedLeases = null! },
            nameof(FileSystemLocalStoreAdapter.JournalDelta.Inbox) => new() { Inbox = null! },
            _ => new() { IncludedOperations = null! },
        };
        await Assert.That(() => ValidateDelta(delta)).Throws<InvalidDataException>();
    }

    /// <summary>Verifies stream transactions cannot hide missing data or an unmarked snapshot.</summary>
    /// <param name="field">The invalid stream field.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(nameof(FileSystemLocalStoreAdapter.JournalDelta.Streams))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.StreamDelta.DeadLetters))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.StreamDelta.Snapshot))]
    public async Task TransactionRejectsInvalidStreamShape(string field)
    {
        var delta = new FileSystemLocalStoreAdapter.JournalDelta();
        delta.Streams[StreamName] = field switch
        {
            nameof(FileSystemLocalStoreAdapter.JournalDelta.Streams) => null!,
            nameof(FileSystemLocalStoreAdapter.StreamDelta.DeadLetters) => new() { DeadLetters = null! },
            _ => new() { Snapshot = CreateSnapshot(StreamName) },
        };
        await Assert.That(() => ValidateDelta(delta)).Throws<InvalidDataException>();
    }

    /// <summary>Verifies stream identity and sequence guards reject malformed durable metadata.</summary>
    /// <param name="field">The invalid stream field.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(nameof(StreamId))]
    [Arguments(nameof(SubscriptionId))]
    [Arguments(nameof(Guid.Empty))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.StreamState.NextSequence))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.StreamState.Operations))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.StreamState.Snapshot))]
    public async Task StreamRejectsInvalidMetadata(string field)
    {
        var state = CreateState();
        var stream = state.Streams[StreamName];
        switch (field)
        {
            case nameof(StreamId):
            {
                _ = state.Streams.Remove(StreamName);
                state.Streams[string.Empty] = stream;
                break;
            }

            case nameof(SubscriptionId):
            {
                stream.SubscriptionId = null;
                break;
            }

            case nameof(Guid.Empty):
            {
                stream.SubscriptionId = new(Guid.Empty);
                break;
            }

            case nameof(FileSystemLocalStoreAdapter.StreamState.NextSequence):
            {
                stream.NextSequence = -1;
                break;
            }

            case nameof(FileSystemLocalStoreAdapter.StreamState.Operations):
            {
                state.Streams[StreamName] = new() { SubscriptionId = stream.SubscriptionId, Operations = null!, };
                break;
            }

            default:
            {
                stream.Snapshot = CreateSnapshot(OtherStreamName);
                break;
            }
        }

        await Assert.That(() => FileSystemJournalDelta.Validate(new(state), false)).Throws<InvalidDataException>();
    }

    /// <summary>Verifies operation records contain the data needed by recovery and upload.</summary>
    /// <param name="field">The missing operation field.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(nameof(FileSystemLocalStoreAdapter.StreamState.Operations))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.OperationState.Operation))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.OperationState.Status))]
    [Arguments(nameof(SyncOperation.Payload))]
    [Arguments(nameof(SyncOperation.Policy))]
    public async Task OperationRejectsMissingData(string field)
    {
        var state = CreateState();
        var stream = state.Streams[StreamName];
        var operation = stream.Operations.Values.Single();
        switch (field)
        {
            case nameof(FileSystemLocalStoreAdapter.StreamState.Operations):
            {
                stream.Operations[operation.Operation.OperationId.Value] = null!;
                break;
            }

            case nameof(FileSystemLocalStoreAdapter.OperationState.Operation):
            {
                operation.Operation = null!;
                break;
            }

            case nameof(FileSystemLocalStoreAdapter.OperationState.Status):
            {
                operation.Status = null!;
                break;
            }

            case nameof(SyncOperation.Payload):
            {
                operation.Operation = operation.Operation with { Payload = null! };
                break;
            }

            default:
            {
                operation.Operation = operation.Operation with { Policy = null! };
                break;
            }
        }

        await Assert.That(() => FileSystemJournalDelta.Validate(new(state), false)).Throws<InvalidDataException>();
    }

    /// <summary>Verifies operation and status identities agree with their containing durable keys.</summary>
    /// <param name="changeStatus">Whether the status identity changes.</param>
    /// <param name="changeStream">Whether the stream, rather than operation, identity changes.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task OperationRejectsMismatchedIdentity(bool changeStatus, bool changeStream)
    {
        var state = CreateState();
        var operation = state.Streams[StreamName].Operations.Values.Single();
        if (changeStatus)
        {
            operation.Status = changeStream
                ? operation.Status with { StreamId = new(OtherStreamName) }
                : operation.Status with { OperationId = OperationId.New() };
        }
        else
        {
            operation.Operation = changeStream
                ? operation.Operation with { StreamId = new(OtherStreamName) }
                : operation.Operation with { OperationId = OperationId.New() };
        }

        await Assert.That(() => FileSystemJournalDelta.Validate(new(state), false)).Throws<InvalidDataException>();
    }

    /// <summary>Verifies leases cannot replay missing records, mismatched keys, or missing membership.</summary>
    /// <param name="field">The invalid lease field.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(nameof(FileSystemLocalStoreAdapter.StoreState.Leases))]
    [Arguments(nameof(Guid.Empty))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.LeaseState.LeaseId))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.LeaseState.OperationIds))]
    public async Task LeaseRejectsInvalidShape(string field)
    {
        var state = CreateState();
        var leaseId = field == nameof(Guid.Empty) ? Guid.Empty : Guid.NewGuid();
        var lease = field switch
        {
            nameof(FileSystemLocalStoreAdapter.StoreState.Leases) => null,
            nameof(FileSystemLocalStoreAdapter.LeaseState.LeaseId) => new FileSystemLocalStoreAdapter.LeaseState { LeaseId = Guid.NewGuid() },
            nameof(FileSystemLocalStoreAdapter.LeaseState.OperationIds) => new() { LeaseId = leaseId, OperationIds = null!, },
            _ => new() { LeaseId = leaseId },
        };
        state.Leases[leaseId] = lease!;
        await Assert.That(() => FileSystemJournalDelta.Validate(new(state), false)).Throws<InvalidDataException>();
    }

    /// <summary>Verifies terminal records are validated before exposing recovered dead letters.</summary>
    /// <param name="field">The missing or invalid terminal-record field.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(nameof(FileSystemLocalStoreAdapter.StreamState.DeadLetters))]
    [Arguments(nameof(DeadLetterRecord.Operation))]
    [Arguments(nameof(StreamId))]
    [Arguments(nameof(SyncOperation.Payload))]
    [Arguments(nameof(SyncOperation.Policy))]
    public async Task DeadLetterRejectsInvalidShape(string field)
    {
        var state = CreateState();
        var operation = state.Streams[StreamName].Operations.Values.Single().Operation;
        var invalid = field switch
        {
            nameof(DeadLetterRecord.Operation) => null,
            nameof(StreamId) => operation with { StreamId = new(OtherStreamName) },
            nameof(SyncOperation.Payload) => operation with { Payload = null! },
            nameof(SyncOperation.Policy) => operation with { Policy = null! },
            _ => operation,
        };
        state.Streams[StreamName].DeadLetters.Add(field == nameof(FileSystemLocalStoreAdapter.StreamState.DeadLetters)
            ? null!
            : new(invalid!, "terminal", 0, DateTimeOffset.UnixEpoch));
        await Assert.That(() => FileSystemJournalDelta.Validate(new(state), false)).Throws<InvalidDataException>();
    }

    /// <summary>Verifies deduplication containers reject invalid durable identities.</summary>
    /// <param name="invalidEvent">Whether the event, rather than operation, identity is invalid.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DeduplicationRejectsInvalidIdentity(bool invalidEvent)
    {
        var state = CreateState();
        if (invalidEvent)
        {
            _ = state.Inbox.Add(" ");
        }
        else
        {
            _ = state.IncludedOperations.Add(Guid.Empty);
        }

        await Assert.That(() => FileSystemJournalDelta.Validate(new(state), false)).Throws<InvalidDataException>();
    }

    /// <summary>Verifies sparse comparisons notice each independent stream metadata change.</summary>
    /// <param name="field">The changed stream field.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(nameof(FileSystemLocalStoreAdapter.StreamState.SubscriptionId))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.StreamState.NextSequence))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.StreamState.Cursor))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.StreamState.DeadLetters))]
    public async Task IndependentStreamChangesOmitUnchangedSnapshotAndOperations(string field)
    {
        var previous = CreateState();
        var next = new FileSystemLocalStoreAdapter.StoreState(previous);
        var stream = next.Streams[StreamName];
        switch (field)
        {
            case nameof(FileSystemLocalStoreAdapter.StreamState.SubscriptionId):
            {
                stream.SubscriptionId = SubscriptionId.New();
                break;
            }

            case nameof(FileSystemLocalStoreAdapter.StreamState.NextSequence):
            {
                stream.NextSequence++;
                break;
            }

            case nameof(FileSystemLocalStoreAdapter.StreamState.Cursor):
            {
                stream.Cursor = "next-cursor";
                break;
            }

            default:
            {
                stream.DeadLetters.Add(new(stream.Operations.Values.Single().Operation, "terminal", 0, DateTimeOffset.UnixEpoch));
                break;
            }
        }

        var delta = FileSystemJournalDelta.Create(previous, next);
        await Assert.That(delta.Streams).HasSingleItem();
        await Assert.That(delta.Streams[StreamName].ReplaceSnapshot).IsFalse();
        await Assert.That(delta.Streams[StreamName].Operations).IsEmpty();
        ValidateDelta(delta);
        FileSystemJournalDelta.Apply(previous, delta);
        await Assert.That(previous.Streams[StreamName].Cursor).IsEqualTo(stream.Cursor);
        await Assert.That(previous.Streams[StreamName].DeadLetters.Count).IsEqualTo(stream.DeadLetters.Count);
    }

    /// <summary>Verifies changing only the terminal bit or operation reference persists that record.</summary>
    /// <param name="changeTerminal">Whether the terminal bit changes.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task IndependentOperationChangeIsIncluded(bool changeTerminal)
    {
        var previous = CreateState();
        var next = new FileSystemLocalStoreAdapter.StoreState(previous);
        var operation = next.Streams[StreamName].Operations.Values.Single();
        if (changeTerminal)
        {
            operation.Terminal = true;
        }
        else
        {
            operation.Operation = operation.Operation with { BaseVersion = "changed-base" };
        }

        var delta = FileSystemJournalDelta.Create(previous, next);
        await Assert.That(delta.Streams[StreamName].Operations).HasSingleItem();
    }

    /// <summary>Verifies every independent lease metadata change produces a transaction record.</summary>
    /// <param name="field">The changed lease field.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(nameof(FileSystemLocalStoreAdapter.LeaseState.ExpiresAtUtc))]
    [Arguments(nameof(List<>.Count))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.LeaseState.OperationIds))]
    public async Task ChangedLeaseMetadataIsIncluded(string field)
    {
        var previous = CreateState();
        var leaseId = Guid.NewGuid();
        previous.Leases[leaseId] = new() { LeaseId = leaseId, OperationIds = [OperationId.New().Value], };
        var next = new FileSystemLocalStoreAdapter.StoreState(previous);
        switch (field)
        {
            case nameof(FileSystemLocalStoreAdapter.LeaseState.ExpiresAtUtc):
            {
                next.Leases[leaseId].ExpiresAtUtc = DateTimeOffset.UnixEpoch;
                break;
            }

            case nameof(List<>.Count):
            {
                next.Leases[leaseId].OperationIds.Add(OperationId.New().Value);
                break;
            }

            default:
            {
                next.Leases[leaseId].OperationIds[0] = OperationId.New().Value;
                break;
            }
        }
        var delta = FileSystemJournalDelta.Create(previous, next);
        await Assert.That(delta.Leases).HasSingleItem();
        await Assert.That(delta.Streams).IsEmpty();
    }

    /// <summary>Verifies snapshots and transactions cannot be combined or assigned the wrong version.</summary>
    /// <param name="version">The record version.</param>
    /// <param name="includeSnapshot">Whether the invalid record includes a snapshot.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(1, true)]
    [Arguments(FileSystemLocalStoreAdapter.IncrementalRecordVersion, true)]
    [Arguments(FileSystemLocalStoreAdapter.IncrementalRecordVersion, false)]
    public async Task InvalidRecordCombinationFailsClosed(int version, bool includeSnapshot)
    {
        var record = new FileSystemLocalStoreAdapter.JournalRecord(
            includeSnapshot ? CreateState() : null,
            includeSnapshot ? new() : null,
            version);
        await Assert.That(() => FileSystemJournalDelta.Validate(record, true)).Throws<InvalidDataException>();
    }

    /// <summary>Verifies replay checks sequence regression before changing any recovered metadata.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RegressedSequenceCannotPublishMetadata()
    {
        var state = CreateState();
        var delta = new FileSystemLocalStoreAdapter.JournalDelta { ClientId = BoundClientIdentity };
        var change = CreateMetadataDelta(state.Streams[StreamName]);
        change.NextSequence = 0;
        change.Cursor = UncommittedCursor;
        delta.Streams[StreamName] = change;
        ValidateDelta(delta);
        await Assert.That(() => FileSystemJournalDelta.Apply(state, delta)).Throws<InvalidDataException>();
        await Assert.That(state.ClientId).IsNull();
        await Assert.That(state.Streams[StreamName].Cursor).IsNull();
        await Assert.That(state.Streams[StreamName].NextSequence).IsEqualTo(1);
    }

    /// <summary>Verifies retained operations are checked even when a transaction has no operation upserts.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RetainedOperationAtNextSequenceCannotPublishMetadata()
    {
        var state = CreateState();
        var stream = state.Streams[StreamName];
        var operation = stream.Operations.Values.Single();
        operation.Operation = operation.Operation with { ClientSequence = stream.NextSequence };
        var delta = new FileSystemLocalStoreAdapter.JournalDelta { ClientId = BoundClientIdentity };
        delta.Streams[StreamName] = CreateMetadataDelta(stream);
        ValidateDelta(delta);
        await Assert.That(() => FileSystemJournalDelta.Apply(state, delta)).Throws<InvalidDataException>();
        await Assert.That(state.ClientId).IsNull();
        await Assert.That(stream.Operations).HasSingleItem();
    }

    /// <summary>Verifies complete validation precedes every stream mutation in a multi-stream transaction.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvalidSecondStreamLeavesFirstStreamUnchanged()
    {
        var state = CreateState();
        state.Streams[OtherStreamName] = new() { SubscriptionId = SubscriptionId.New(), NextSequence = 1, };
        var delta = new FileSystemLocalStoreAdapter.JournalDelta { ClientId = BoundClientIdentity };
        delta.Streams[StreamName] = CreateMetadataDelta(state.Streams[StreamName]);
        delta.Streams[StreamName].Cursor = UncommittedCursor;
        delta.Streams[OtherStreamName] = CreateMetadataDelta(state.Streams[OtherStreamName]);
        delta.Streams[OtherStreamName].NextSequence = 0;
        ValidateDelta(delta);
        await Assert.That(() => FileSystemJournalDelta.Apply(state, delta)).Throws<InvalidDataException>();
        await Assert.That(state.Streams[StreamName].Cursor).IsNull();
        await Assert.That(state.ClientId).IsNull();
    }

    /// <summary>Verifies checkpoint replacement cannot be confused with metadata-only null fields.</summary>
    /// <param name="clearCheckpoint">Whether the transaction tries to erase the existing checkpoint.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ExistingCheckpointRequiresNonNullReplacement(bool clearCheckpoint)
    {
        var state = CreateState();
        var stream = state.Streams[StreamName];
        stream.Snapshot = CreateSnapshot(StreamName);
        var original = stream.Snapshot;
        var delta = new FileSystemLocalStoreAdapter.JournalDelta();
        var change = CreateMetadataDelta(stream);
        change.ReplaceSnapshot = true;
        change.Snapshot = clearCheckpoint ? null : CreateSnapshot(StreamName);
        delta.Streams[StreamName] = change;
        ValidateDelta(delta);
        if (clearCheckpoint)
        {
            await Assert.That(() => FileSystemJournalDelta.Apply(state, delta)).Throws<InvalidDataException>();
            await Assert.That(ReferenceEquals(stream.Snapshot, original)).IsTrue();
        }
        else
        {
            FileSystemJournalDelta.Apply(state, delta);
            await Assert.That(ReferenceEquals(stream.Snapshot, change.Snapshot)).IsTrue();
        }
    }

    /// <summary>Verifies a metadata-only null field keeps the existing checkpoint.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task MetadataNullSnapshotPreservesExistingCheckpoint()
    {
        var state = CreateState();
        var stream = state.Streams[StreamName];
        stream.Snapshot = CreateSnapshot(StreamName);
        var original = stream.Snapshot;
        var delta = new FileSystemLocalStoreAdapter.JournalDelta();
        delta.Streams[StreamName] = CreateMetadataDelta(stream);
        ValidateDelta(delta);
        FileSystemJournalDelta.Apply(state, delta);
        await Assert.That(ReferenceEquals(stream.Snapshot, original)).IsTrue();
    }

    /// <summary>Verifies explicit null checkpoint replacement is valid for a new empty stream.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task NewEmptyStreamAllowsExplicitNullCheckpoint()
    {
        var state = new FileSystemLocalStoreAdapter.StoreState { StoreIdentity = StoreIdentity };
        var delta = new FileSystemLocalStoreAdapter.JournalDelta();
        delta.Streams[StreamName] = new() { SubscriptionId = SubscriptionId.New(), ReplaceSnapshot = true, };
        ValidateDelta(delta);
        FileSystemJournalDelta.Apply(state, delta);
        await Assert.That(state.Streams[StreamName].Snapshot).IsNull();
        await Assert.That(state.Streams[StreamName].NextSequence).IsEqualTo(0);
    }

    /// <summary>Verifies negative and unreserved operation sequences fail transaction validation.</summary>
    /// <param name="negativeSequence">Whether the operation sequence is negative.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task TransactionRejectsUnreservedOperationSequence(bool negativeSequence)
    {
        var state = CreateState();
        var stream = state.Streams[StreamName];
        var operation = stream.Operations.Values.Single();
        operation.Operation = operation.Operation with { ClientSequence = negativeSequence ? -1 : stream.NextSequence };
        var delta = new FileSystemLocalStoreAdapter.JournalDelta();
        var change = CreateMetadataDelta(stream);
        change.Operations[operation.Operation.OperationId.Value] = operation;
        delta.Streams[StreamName] = change;
        await Assert.That(() => ValidateDelta(delta)).Throws<InvalidDataException>();
    }

    /// <summary>Verifies an unchanged lease and its operation do not produce redundant records.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UnchangedLeaseProducesEmptyTransaction()
    {
        var state = CreateState();
        var operation = state.Streams[StreamName].Operations.Values.Single();
        var leaseId = Guid.NewGuid();
        operation.LeaseId = leaseId;
        operation.LeaseExpiry = DateTimeOffset.UnixEpoch;
        state.Leases[leaseId] = new() { LeaseId = leaseId, ExpiresAtUtc = operation.LeaseExpiry.Value, OperationIds = [operation.Operation.OperationId.Value], };
        var delta = FileSystemJournalDelta.Create(state, new(state));
        await Assert.That(delta.Streams).IsEmpty();
        await Assert.That(delta.Leases).IsEmpty();
        await Assert.That(delta.RemovedLeases).IsEmpty();
        ValidateDelta(delta);
        FileSystemJournalDelta.Apply(state, delta);
    }

    /// <summary>Verifies sparse replay handles stream creation, lease replacement, and deduplication together.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateAndApplyPreserveCombinedTransaction()
    {
        var previous = CreateState();
        var first = previous.Streams[StreamName].Operations.Values.Single();
        var oldLease = Guid.NewGuid();
        previous.Leases[oldLease] = new() { LeaseId = oldLease, ExpiresAtUtc = DateTimeOffset.UnixEpoch, OperationIds = [first.Operation.OperationId.Value], };
        first.LeaseId = oldLease;
        first.LeaseExpiry = DateTimeOffset.UnixEpoch;
        first.Status = first.Status with { State = SyncOperationState.Uploading };
        var next = new FileSystemLocalStoreAdapter.StoreState(previous);
        _ = next.Leases.Remove(oldLease);
        var completed = next.Streams[StreamName].Operations.Values.Single();
        completed.LeaseId = null;
        completed.LeaseExpiry = null;
        completed.Terminal = true;
        completed.Status = completed.Status with { State = SyncOperationState.Synchronized };
        _ = next.IncludedOperations.Add(completed.Operation.OperationId.Value);
        var eventIdentity = $"{StreamName}|{Guid.NewGuid()}";
        _ = next.Inbox.Add(eventIdentity);
        next.Streams[OtherStreamName] = new() { SubscriptionId = SubscriptionId.New() };
        var newLease = Guid.NewGuid();
        var uploaded = new FileSystemLocalStoreAdapter.OperationState(completed)
        {
            Operation = completed.Operation with { OperationId = OperationId.New(), ClientSequence = 1, },
            LeaseId = newLease,
            LeaseExpiry = DateTimeOffset.UnixEpoch,
            Terminal = false,
        };
        uploaded.Status = uploaded.Status with { OperationId = uploaded.Operation.OperationId, State = SyncOperationState.Uploading, };
        next.Streams[StreamName].NextSequence++;
        next.Streams[StreamName].Operations[uploaded.Operation.OperationId.Value] = uploaded;
        next.Leases[newLease] = new() { LeaseId = newLease, ExpiresAtUtc = DateTimeOffset.UnixEpoch, OperationIds = [uploaded.Operation.OperationId.Value], };
        var delta = FileSystemJournalDelta.Create(previous, next);
        ValidateDelta(delta);
        FileSystemJournalDelta.Apply(previous, delta);
        await Assert.That(previous.Leases.ContainsKey(oldLease)).IsFalse();
        await Assert.That(previous.Leases.ContainsKey(newLease)).IsTrue();
        await Assert.That(previous.Inbox.Contains(eventIdentity)).IsTrue();
        await Assert.That(previous.IncludedOperations.Contains(completed.Operation.OperationId.Value)).IsTrue();
        await Assert.That(previous.Streams[OtherStreamName].Snapshot).IsNull();
        await Assert.That(previous.Streams[StreamName].NextSequence).IsEqualTo(next.Streams[StreamName].NextSequence);
    }

    /// <summary>Verifies legacy snapshots cannot drop streams or roll back client sequences.</summary>
    /// <param name="dropStream">Whether the snapshot omits a committed stream.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task LegacySnapshotTransitionRejectsRegression(bool dropStream)
    {
        var state = CreateState();
        var snapshot = new FileSystemLocalStoreAdapter.StoreState(state);
        if (dropStream)
        {
            snapshot.Streams.Clear();
        }
        else
        {
            snapshot.Streams[StreamName].NextSequence = 0;
        }

        await Assert.That(() => FileSystemJournalDelta.ValidateSnapshotReplay(state, snapshot)).Throws<InvalidDataException>();
        await Assert.That(state.Streams[StreamName].NextSequence).IsEqualTo(1);
    }

    /// <summary>Verifies valid legacy snapshot transitions retain all prefix fences.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task LegacySnapshotTransitionAllowsUnchangedCommittedState()
    {
        var state = CreateState();
        state.Streams[StreamName].Snapshot = CreateSnapshot(StreamName);
        FileSystemJournalDelta.ValidateSnapshotReplay(state, new(state));
        await Assert.That(state.Streams[StreamName].NextSequence).IsEqualTo(1);
    }

    /// <summary>Creates metadata-only changes without copying retained operations or snapshots.</summary>
    /// <param name="stream">The committed stream.</param>
    /// <returns>The metadata-only changed stream.</returns>
    private static FileSystemLocalStoreAdapter.StreamDelta CreateMetadataDelta(FileSystemLocalStoreAdapter.StreamState stream) =>
        new() { SubscriptionId = stream.SubscriptionId, NextSequence = stream.NextSequence, Cursor = stream.Cursor, };

    /// <summary>Creates one valid durable operation using production state types.</summary>
    /// <returns>The initialized state fixture.</returns>
    private static FileSystemLocalStoreAdapter.StoreState CreateState()
    {
        var operation = new SyncOperation
        {
            OperationId = OperationId.New(),
            StreamId = new(StreamName),
            ClientSequence = 0,
            TimestampUtc = DateTimeOffset.UnixEpoch,
            Type = SyncOperationType.Update,
            Payload = CreatePayload(),
        };
        var stream = new FileSystemLocalStoreAdapter.StreamState { SubscriptionId = SubscriptionId.New(), NextSequence = 1, };
        stream.Operations[operation.OperationId.Value] = new()
        {
            Operation = operation,
            Status = new(operation.OperationId, operation.StreamId, SyncOperationState.SavedLocally, 0, DateTimeOffset.UnixEpoch, null),
        };
        var state = new FileSystemLocalStoreAdapter.StoreState { StoreIdentity = StoreIdentity };
        state.Streams[StreamName] = stream;
        return state;
    }

    /// <summary>Creates a valid snapshot for a requested stream.</summary>
    /// <param name="streamName">The snapshot stream identity.</param>
    /// <returns>The snapshot fixture.</returns>
    private static LocalSnapshot CreateSnapshot(string streamName) =>
        new(new(streamName), 1, null, CreatePayload(), 1, DateTimeOffset.UnixEpoch);

    /// <summary>Creates a hashed payload using the existing journal checksum helper.</summary>
    /// <returns>The payload fixture.</returns>
    private static PayloadEnvelope CreatePayload()
    {
        var bytes = "payload"u8.ToArray();
        return new("test", 1, "text/plain", bytes, Convert.ToHexString(FileSystemJournalHelpers.ComputeHash(bytes)));
    }

    /// <summary>Validates an incremental record with an existing committed prefix.</summary>
    /// <param name="delta">The proposed transaction.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateDelta(FileSystemLocalStoreAdapter.JournalDelta delta) =>
        FileSystemJournalDelta.Validate(new(null, delta, FileSystemLocalStoreAdapter.IncrementalRecordVersion), true);
}
