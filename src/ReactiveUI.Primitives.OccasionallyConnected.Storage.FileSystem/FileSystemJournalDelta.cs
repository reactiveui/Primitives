// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.IO;
#if NET5_0_OR_GREATER
using System.Runtime.InteropServices;
#endif

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem;

/// <summary>Builds and replays incremental transactions using the existing durable state types.</summary>
internal static class FileSystemJournalDelta
{
    /// <summary>Creates a transaction containing only changed records and newly added identities.</summary>
    /// <param name="previous">The committed state.</param>
    /// <param name="next">The proposed state.</param>
    /// <returns>The atomic journal transaction.</returns>
    internal static FileSystemLocalStoreAdapter.JournalDelta Create(
        FileSystemLocalStoreAdapter.StoreState previous,
        FileSystemLocalStoreAdapter.StoreState next)
    {
        var delta = new FileSystemLocalStoreAdapter.JournalDelta
        {
            ClientId = next.ClientId,
            Outbox = next.Outbox,
            Inbox = Added(next.Inbox, previous.Inbox),
            IncludedOperations = Added(next.IncludedOperations, previous.IncludedOperations),
            RemovedLeases = RemovedLeases(previous.Leases, next.Leases),
        };
        foreach (var pair in next.Streams)
        {
            _ = previous.Streams.TryGetValue(pair.Key, out var old);
            var changed = CreateStreamDelta(old, pair.Value);
            if (StreamChanged(old, changed))
            {
                delta.Streams.Add(pair.Key, changed);
            }
        }

        foreach (var pair in next.Leases)
        {
            if (!previous.Leases.TryGetValue(pair.Key, out var prior) || !SameLease(prior, pair.Value))
            {
                delta.Leases.Add(pair.Key, pair.Value);
            }
        }

        return delta;
    }

    /// <summary>Replays a validated transaction onto the recovered state.</summary>
    /// <param name="state">The recovered committed prefix.</param>
    /// <param name="delta">The transaction to replay.</param>
    /// <exception cref="InvalidDataException">The transaction regresses or invalidates committed state.</exception>
    internal static void Apply(
        FileSystemLocalStoreAdapter.StoreState state,
        FileSystemLocalStoreAdapter.JournalDelta delta)
    {
        ValidateReplay(state, delta);
        state.ClientId = delta.ClientId;
        state.Outbox = delta.Outbox;
        foreach (var pair in delta.Streams)
        {
            var stream = GetStreamForReplay(state, pair.Key);
            var change = pair.Value;
            stream.SubscriptionId = change.SubscriptionId;
            stream.NextSequence = change.NextSequence;
            stream.Cursor = change.Cursor;
            if (change.ReplaceSnapshot)
            {
                stream.Snapshot = change.Snapshot;
            }

            foreach (var operation in change.Operations)
            {
                stream.Operations[operation.Key] = operation.Value;
            }

            stream.DeadLetters.AddRange(change.DeadLetters);
        }

        foreach (var id in delta.RemovedLeases)
        {
            _ = state.Leases.Remove(id);
        }

        foreach (var pair in delta.Leases)
        {
            state.Leases[pair.Key] = pair.Value;
        }

        state.Inbox.UnionWith(delta.Inbox);
        state.IncludedOperations.UnionWith(delta.IncludedOperations);
    }

    /// <summary>Rejects incomplete or inconsistent records even when their checksum is valid.</summary>
    /// <param name="record">The decoded journal record.</param>
    /// <param name="hasSnapshot">Whether the journal has a committed initial snapshot.</param>
    /// <exception cref="InvalidDataException">The record has an invalid version or shape.</exception>
    internal static void Validate(FileSystemLocalStoreAdapter.JournalRecord record, bool hasSnapshot)
    {
        if (record.Version == 1 && record.State is { } state && record.Delta is null)
        {
            ValidateSnapshot(state);
            return;
        }

        if (record.Version != FileSystemLocalStoreAdapter.IncrementalRecordVersion
            || record.State is not null || record.Delta is not { } delta || !hasSnapshot)
        {
            throw new InvalidDataException("The filesystem journal transaction is invalid.");
        }

        ValidateDelta(delta);
    }

    /// <summary>Checks legacy full-state records against the recovered committed prefix.</summary>
    /// <param name="state">The committed prefix.</param>
    /// <param name="snapshot">The decoded legacy snapshot.</param>
    /// <exception cref="InvalidDataException">The snapshot loses a stream or regresses committed metadata.</exception>
    internal static void ValidateSnapshotReplay(
        FileSystemLocalStoreAdapter.StoreState state,
        FileSystemLocalStoreAdapter.StoreState snapshot)
    {
        foreach (var pair in state.Streams)
        {
            if (!snapshot.Streams.TryGetValue(pair.Key, out var next))
            {
                throw new InvalidDataException("The filesystem journal snapshot discards a committed stream.");
            }

            ValidatePrefix(pair.Value, next.NextSequence, true, next.Snapshot);
        }
    }

    /// <summary>Checks all prefix fences before changing any recovered state.</summary>
    /// <param name="state">The recovered committed prefix.</param>
    /// <param name="delta">The proposed transaction.</param>
    /// <exception cref="InvalidDataException">The transaction regresses or invalidates committed state.</exception>
    private static void ValidateReplay(
        FileSystemLocalStoreAdapter.StoreState state,
        FileSystemLocalStoreAdapter.JournalDelta delta)
    {
        foreach (var pair in delta.Streams)
        {
            if (!state.Streams.TryGetValue(pair.Key, out var previous))
            {
                continue;
            }

            ValidatePrefix(previous, pair.Value.NextSequence, pair.Value.ReplaceSnapshot, pair.Value.Snapshot);
            foreach (var operation in previous.Operations)
            {
                if (!pair.Value.Operations.ContainsKey(operation.Key))
                {
                    ValidateSequence(operation.Value.Operation.ClientSequence, pair.Value.NextSequence);
                }
            }
        }
    }

    /// <summary>Checks monotonic stream metadata and checkpoint preservation.</summary>
    /// <param name="previous">The committed stream.</param>
    /// <param name="nextSequence">The next client sequence after the transaction.</param>
    /// <param name="replaceSnapshot">Whether the transaction replaces the checkpoint.</param>
    /// <param name="snapshot">The replacement checkpoint.</param>
    /// <exception cref="InvalidDataException">The transaction reuses a sequence or deletes a checkpoint.</exception>
    private static void ValidatePrefix(
        FileSystemLocalStoreAdapter.StreamState previous,
        long nextSequence,
        bool replaceSnapshot,
        LocalSnapshot? snapshot)
    {
        if (nextSequence < previous.NextSequence)
        {
            throw new InvalidDataException("The filesystem journal regresses the committed client sequence.");
        }

        if (previous.Snapshot is not null && replaceSnapshot && snapshot is null)
        {
            throw new InvalidDataException("The filesystem journal deletes a committed checkpoint without a deletion transition.");
        }
    }

    /// <summary>Collects identities not present in the committed prefix.</summary>
    /// <typeparam name="T">The identity type.</typeparam>
    /// <param name="next">The proposed identities.</param>
    /// <param name="previous">The committed identities.</param>
    /// <returns>The newly added identities.</returns>
    private static T[] Added<T>(HashSet<T> next, HashSet<T> previous)
    {
        var added = new List<T>();
        foreach (var value in next)
        {
            if (!previous.Contains(value))
            {
                added.Add(value);
            }
        }

        return added.ToArray();
    }

    /// <summary>Collects leases removed by the proposed transaction.</summary>
    /// <param name="previous">The committed leases.</param>
    /// <param name="next">The proposed leases.</param>
    /// <returns>The removed lease identifiers.</returns>
    private static Guid[] RemovedLeases(
        Dictionary<Guid, FileSystemLocalStoreAdapter.LeaseState> previous,
        Dictionary<Guid, FileSystemLocalStoreAdapter.LeaseState> next)
    {
        var removed = new List<Guid>();
        foreach (var id in previous.Keys)
        {
            if (!next.ContainsKey(id))
            {
                removed.Add(id);
            }
        }

        return removed.ToArray();
    }

    /// <summary>Builds changed records for one stream.</summary>
    /// <param name="previous">The committed stream, if one exists.</param>
    /// <param name="current">The proposed stream.</param>
    /// <returns>The changed stream records.</returns>
    private static FileSystemLocalStoreAdapter.StreamDelta CreateStreamDelta(
        FileSystemLocalStoreAdapter.StreamState? previous,
        FileSystemLocalStoreAdapter.StreamState current)
    {
        var changed = new FileSystemLocalStoreAdapter.StreamDelta
        {
            SubscriptionId = current.SubscriptionId,
            NextSequence = current.NextSequence,
            Cursor = current.Cursor,
            ReplaceSnapshot = !ReferenceEquals(previous?.Snapshot, current.Snapshot),
            Snapshot = ReferenceEquals(previous?.Snapshot, current.Snapshot) ? null : current.Snapshot,
            DeadLetters = AddedDeadLetters(previous, current),
        };
        foreach (var operation in current.Operations)
        {
            if (previous is null
                || !previous.Operations.TryGetValue(operation.Key, out var prior)
                || !SameOperation(prior, operation.Value))
            {
                changed.Operations.Add(operation.Key, operation.Value);
            }
        }

        return changed;
    }

    /// <summary>Copies only newly appended dead letters.</summary>
    /// <param name="previous">The committed stream.</param>
    /// <param name="current">The proposed stream.</param>
    /// <returns>The newly appended terminal records.</returns>
    private static DeadLetterRecord[] AddedDeadLetters(
        FileSystemLocalStoreAdapter.StreamState? previous,
        FileSystemLocalStoreAdapter.StreamState current)
    {
        var start = previous?.DeadLetters.Count ?? 0;
        var added = new DeadLetterRecord[current.DeadLetters.Count - start];
        for (var index = 0; index < added.Length; index++)
        {
            added[index] = current.DeadLetters[start + index];
        }

        return added;
    }

    /// <summary>Determines whether the stream has any changed records or metadata.</summary>
    /// <param name="previous">The committed stream.</param>
    /// <param name="changed">The proposed changed records.</param>
    /// <returns>Whether the stream changed.</returns>
    private static bool StreamChanged(
        FileSystemLocalStoreAdapter.StreamState? previous,
        FileSystemLocalStoreAdapter.StreamDelta changed) =>
        previous is null
        || changed.ReplaceSnapshot
        || changed.SubscriptionId != previous.SubscriptionId
        || changed.NextSequence != previous.NextSequence
        || changed.Cursor != previous.Cursor
        || changed.Operations.Count != 0
        || changed.DeadLetters.Length != 0;

    /// <summary>Finds or creates a stream during replay.</summary>
    /// <param name="state">The recovered state.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The mutable recovered stream.</returns>
    private static FileSystemLocalStoreAdapter.StreamState GetStreamForReplay(
        FileSystemLocalStoreAdapter.StoreState state,
        string streamId)
    {
#if NET5_0_OR_GREATER
        ref var stream = ref CollectionsMarshal.GetValueRefOrAddDefault(state.Streams, streamId, out _);
        return stream ??= new();
#else
        if (!state.Streams.TryGetValue(streamId, out var stream))
        {
            stream = new();
            state.Streams.Add(streamId, stream);
        }

        return stream;
#endif
    }

    /// <summary>Checks a full snapshot before using it for recovery.</summary>
    /// <param name="state">The decoded snapshot.</param>
    /// <exception cref="InvalidDataException">The snapshot has an invalid shape.</exception>
    private static void ValidateSnapshot(FileSystemLocalStoreAdapter.StoreState state)
    {
        if (string.IsNullOrWhiteSpace(state.StoreIdentity)
            || state.Streams is null || state.Leases is null || state.Inbox is null || state.IncludedOperations is null)
        {
            throw new InvalidDataException("The filesystem journal snapshot is invalid.");
        }

        foreach (var pair in state.Streams)
        {
            var stream = pair.Value;
            if (stream is null || stream.DeadLetters is null)
            {
                throw new InvalidDataException("The filesystem journal stream is invalid.");
            }

            ValidateStream(pair.Key, stream.SubscriptionId, stream.NextSequence, stream.Snapshot, stream.Operations);
            ValidateDeadLetters(pair.Key, stream.DeadLetters);
        }

        ValidateLeases(state.Leases);
        ValidateIdentities(state.Inbox, state.IncludedOperations);
    }

    /// <summary>Checks incremental record contents before replay.</summary>
    /// <param name="delta">The decoded transaction.</param>
    /// <exception cref="InvalidDataException">The transaction has an invalid shape.</exception>
    private static void ValidateDelta(FileSystemLocalStoreAdapter.JournalDelta delta)
    {
        if (delta.Streams is null || delta.Leases is null || delta.RemovedLeases is null
            || delta.Inbox is null || delta.IncludedOperations is null)
        {
            throw new InvalidDataException("The filesystem journal transaction is invalid.");
        }

        foreach (var pair in delta.Streams)
        {
            ValidateStreamDelta(pair.Key, pair.Value);
        }

        ValidateLeases(delta.Leases);
        ValidateIdentities(delta.Inbox, delta.IncludedOperations);
    }

    /// <summary>Checks the changed records of one stream.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="stream">The decoded changed records.</param>
    /// <exception cref="InvalidDataException">The stream transaction has an invalid shape.</exception>
    private static void ValidateStreamDelta(string streamId, FileSystemLocalStoreAdapter.StreamDelta stream)
    {
        if (stream is null || stream.DeadLetters is null || (!stream.ReplaceSnapshot && stream.Snapshot is not null))
        {
            throw new InvalidDataException("The filesystem journal stream transaction is invalid.");
        }

        ValidateStream(streamId, stream.SubscriptionId, stream.NextSequence, stream.Snapshot, stream.Operations);
        ValidateDeadLetters(streamId, stream.DeadLetters);
    }

    /// <summary>Checks durable stream and operation identities.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="subscriptionId">The durable subscription identifier.</param>
    /// <param name="sequence">The next client sequence.</param>
    /// <param name="snapshot">The replacement snapshot, if any.</param>
    /// <param name="operations">The retained or changed operations.</param>
    /// <exception cref="InvalidDataException">The stream has invalid metadata.</exception>
    private static void ValidateStream(
        string streamId,
        SubscriptionId? subscriptionId,
        long sequence,
        LocalSnapshot? snapshot,
        Dictionary<Guid, FileSystemLocalStoreAdapter.OperationState> operations)
    {
        if (string.IsNullOrWhiteSpace(streamId) || subscriptionId is null || subscriptionId.Value.Value == Guid.Empty
            || sequence < 0 || operations is null || (snapshot is not null && snapshot.StreamId.Value != streamId))
        {
            throw new InvalidDataException("The filesystem journal stream identity is invalid.");
        }

        foreach (var pair in operations)
        {
            ValidateOperation(streamId, pair.Key, pair.Value);
            ValidateSequence(pair.Value.Operation.ClientSequence, sequence);
        }
    }

    /// <summary>Checks that a retained operation cannot reuse the next client sequence.</summary>
    /// <param name="sequence">The retained operation sequence.</param>
    /// <param name="nextSequence">The next available client sequence.</param>
    /// <exception cref="InvalidDataException">The operation falls outside the committed sequence range.</exception>
    private static void ValidateSequence(long sequence, long nextSequence)
    {
        if (sequence < 0 || sequence >= nextSequence)
        {
            throw new InvalidDataException("The filesystem journal operation sequence is outside the committed range.");
        }
    }

    /// <summary>Checks the identity and required data of one operation record.</summary>
    /// <param name="streamId">The containing stream.</param>
    /// <param name="operationId">The durable operation identifier.</param>
    /// <param name="operation">The decoded operation.</param>
    /// <exception cref="InvalidDataException">The operation has inconsistent or missing data.</exception>
    private static void ValidateOperation(string streamId, Guid operationId, FileSystemLocalStoreAdapter.OperationState operation)
    {
        if (operation?.Operation is null || operation.Status is null
            || operation.Operation.Payload is null || operation.Operation.Policy is null
            || operation.Operation.OperationId.Value != operationId
            || operation.Operation.StreamId.Value != streamId
            || operation.Status.OperationId.Value != operationId
            || operation.Status.StreamId.Value != streamId)
        {
            throw new InvalidDataException("The filesystem journal operation is invalid.");
        }
    }

    /// <summary>Checks durable lease identities and membership shape.</summary>
    /// <param name="leases">The decoded leases.</param>
    /// <exception cref="InvalidDataException">A lease has inconsistent or missing data.</exception>
    private static void ValidateLeases(Dictionary<Guid, FileSystemLocalStoreAdapter.LeaseState> leases)
    {
        foreach (var pair in leases)
        {
            if (pair.Value is null || pair.Key == Guid.Empty || pair.Value.LeaseId != pair.Key
                || pair.Value.OperationIds is null)
            {
                throw new InvalidDataException("The filesystem journal lease is invalid.");
            }
        }
    }

    /// <summary>Checks retained terminal records before publishing recovered state.</summary>
    /// <param name="streamId">The containing stream.</param>
    /// <param name="deadLetters">The decoded terminal records.</param>
    /// <exception cref="InvalidDataException">A terminal record has inconsistent or missing data.</exception>
    private static void ValidateDeadLetters(string streamId, IEnumerable<DeadLetterRecord> deadLetters)
    {
        foreach (var deadLetter in deadLetters)
        {
            if (deadLetter?.Operation is null || deadLetter.Operation.StreamId.Value != streamId
                || deadLetter.Operation.Payload is null || deadLetter.Operation.Policy is null)
            {
                throw new InvalidDataException("The filesystem journal dead letter is invalid.");
            }
        }
    }

    /// <summary>Checks the identities used to deduplicate recovered events and operation inclusions.</summary>
    /// <param name="inbox">The decoded event identities.</param>
    /// <param name="includedOperations">The decoded operation inclusions.</param>
    /// <exception cref="InvalidDataException">A deduplication identity is invalid.</exception>
    private static void ValidateIdentities(IEnumerable<string> inbox, IEnumerable<Guid> includedOperations)
    {
        foreach (var identity in inbox)
        {
            if (string.IsNullOrWhiteSpace(identity))
            {
                throw new InvalidDataException("The filesystem journal event identity is invalid.");
            }
        }

        foreach (var identity in includedOperations)
        {
            if (identity == Guid.Empty)
            {
                throw new InvalidDataException("The filesystem journal operation inclusion is invalid.");
            }
        }
    }

    /// <summary>Compares lease metadata without serializing membership.</summary>
    /// <param name="left">The committed lease.</param>
    /// <param name="right">The proposed lease.</param>
    /// <returns>Whether both leases have the same contents.</returns>
    private static bool SameLease(FileSystemLocalStoreAdapter.LeaseState left, FileSystemLocalStoreAdapter.LeaseState right)
    {
        if (left.ExpiresAtUtc != right.ExpiresAtUtc || left.OperationIds.Count != right.OperationIds.Count)
        {
            return false;
        }

        for (var index = 0; index < left.OperationIds.Count; index++)
        {
            if (left.OperationIds[index] != right.OperationIds[index])
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Compares mutable operation metadata without serializing retained payloads.</summary>
    /// <param name="left">The committed operation.</param>
    /// <param name="right">The proposed operation.</param>
    /// <returns>Whether both operations have the same contents.</returns>
    private static bool SameOperation(
        FileSystemLocalStoreAdapter.OperationState left,
        FileSystemLocalStoreAdapter.OperationState right) =>
        ReferenceEquals(left.Operation, right.Operation)
        && ReferenceEquals(left.Status, right.Status)
        && ReferenceEquals(left.RetryState, right.RetryState)
        && left.LeaseId == right.LeaseId
        && left.LeaseExpiry == right.LeaseExpiry
        && left.Terminal == right.Terminal;
}
