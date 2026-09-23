// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Coordinates atomic local stream recovery and optimistic operation commits.</summary>
/// <content>Applies bounded snapshot recovery after a remote retained-history gap.</content>
internal sealed partial class LocalStreamCommitter<TState, TInput>
{
    /// <summary>The logical byte count for an Int32 field.</summary>
    private const long SnapshotRecoveryInt32LogicalBytes = 4;

    /// <summary>The logical byte count for an Int64 field.</summary>
    private const long SnapshotRecoveryInt64LogicalBytes = 8;

    /// <summary>The logical byte count for a Guid field.</summary>
    private const long SnapshotRecoveryGuidLogicalBytes = 16;

    /// <summary>The logical byte count for a DateTimeOffset field.</summary>
    private const long SnapshotRecoveryDateTimeOffsetLogicalBytes = 16;

    /// <summary>Captures a bounded snapshot recovery request without mutating local state.</summary>
    /// <param name="expiredCursor">The expired cursor, if known.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The local capture and remote request.</returns>
    internal async ValueTask<SnapshotRecoveryCaptureResult> CaptureSnapshotRecoveryAsync(
        string? expiredCursor,
        SnapshotRecoveryLimits limits,
        CancellationToken cancellationToken)
    {
        EnterExclusive();
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            ThrowIfNotRecovered();
            var captureStore = RequireSnapshotRecoveryCaptureStore();
            var capture = await CaptureSnapshotRecoveryStoreAsync(captureStore, limits, cancellationToken).ConfigureAwait(false);
            var normalized = NormalizeSnapshotRecoveryCapture(capture, limits);
            ValidateSnapshotRecoveryCapture(normalized.LocalCapture, Current);
            var request = CreateSnapshotRecoveryRequest(normalized.RemoteCapture, expiredCursor, limits);
            SnapshotRecoveryValidator.Validate(request, limits);
            return new(normalized.LocalCapture, request);
        }
        finally
        {
            ExitExclusive();
        }
    }

    /// <summary>Applies a bounded remote snapshot recovery response as one durable local transition.</summary>
    /// <param name="capture">The capture used to build the remote request.</param>
    /// <param name="result">The remote recovery result.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <param name="cancellationToken">The precommit cancellation token.</param>
    /// <returns>The committed recovery result.</returns>
    internal async ValueTask<SnapshotRecoveryStreamCommitResult<TState>> ApplySnapshotRecoveryAsync(
        SnapshotRecoveryCaptureResult capture,
        RemoteSnapshotRecoveryResult result,
        SnapshotRecoveryLimits limits,
        CancellationToken cancellationToken)
    {
        SnapshotRecoveryValidator.Validate(capture.Request, result, limits);
        ThrowIfRetryableConcurrentChange(result);
        var checkpoint = RequireRecoveredCheckpoint(result);
        EnterExclusive();
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            ThrowIfNotRecovered();
            var fresh = await CaptureFreshSnapshotRecoveryAsync(capture, limits, cancellationToken).ConfigureAwait(false);
            return await CommitSnapshotRecoveryAsync(fresh, checkpoint, result, limits, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ExitExclusive();
        }
    }

    /// <summary>Gets a recovered checkpoint or converts a retryable status into a typed retry exception.</summary>
    /// <param name="result">The remote recovery result.</param>
    /// <returns>The recovered checkpoint.</returns>
    /// <exception cref="InvalidOperationException">The recovery result does not contain a recovered checkpoint.</exception>
    private static RemoteSnapshotCheckpoint RequireRecoveredCheckpoint(RemoteSnapshotRecoveryResult result)
    {
        if (result.Status == RemoteSnapshotRecoveryStatus.Recovered && result.Checkpoint is { } checkpoint)
        {
            return checkpoint;
        }

        throw new InvalidOperationException($"Snapshot recovery failed with status {result.Status}.");
    }

    /// <summary>Throws a typed retry exception for concurrent remote changes.</summary>
    /// <param name="result">The remote recovery result.</param>
    /// <exception cref="SnapshotRecoveryRetryableConcurrentChangeException">The remote status requires a fresh capture.</exception>
    private static void ThrowIfRetryableConcurrentChange(RemoteSnapshotRecoveryResult result)
    {
        if (result.Status == RemoteSnapshotRecoveryStatus.RetryableConcurrentChange)
        {
            throw new SnapshotRecoveryRetryableConcurrentChangeException();
        }
    }

    /// <summary>Validates per-role counts before allocating request-owned copies.</summary>
    /// <param name="capture">The capture to validate.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <exception cref="SnapshotRecoveryCapacityExceededException">A role count exceeds the limit.</exception>
    private static void ValidateSnapshotRecoveryRoleCounts(
        LocalSnapshotRecoveryCapture capture,
        SnapshotRecoveryLimits limits)
    {
        ThrowIfSnapshotRecoveryCapacityExceeded(
            capture.PendingOperations.Count,
            limits.MaximumPendingOperations,
            nameof(SnapshotRecoveryLimits.MaximumPendingOperations));
        ThrowIfSnapshotRecoveryCapacityExceeded(
            capture.ReplayOperations.Count,
            limits.MaximumPendingOperations,
            nameof(SnapshotRecoveryLimits.MaximumPendingOperations));
    }

    /// <summary>Adds bounded logical bytes for the optional local snapshot.</summary>
    /// <param name="snapshot">The optional local snapshot.</param>
    /// <param name="logicalBytes">The logical bytes observed so far.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <returns>The updated logical byte count.</returns>
    private static long AddSnapshotRecoveryCaptureSnapshotBytes(
        LocalSnapshot? snapshot,
        long logicalBytes,
        SnapshotRecoveryLimits limits)
    {
        if (snapshot is null)
        {
            return logicalBytes;
        }

        var cursorBytes = GetOptionalSnapshotRecoveryUtf8Bytes(snapshot.ServerCursor);
        ThrowIfSnapshotRecoveryCapacityExceeded(
            cursorBytes,
            limits.MaximumCursorUtf8Bytes,
            nameof(SnapshotRecoveryLimits.MaximumCursorUtf8Bytes));
        var updated = checked(logicalBytes
            + GetRequiredSnapshotRecoveryUtf8Bytes(snapshot.StreamId.Value)
            + SnapshotRecoveryInt32LogicalBytes
            + cursorBytes
            + GetSnapshotRecoveryPayloadLogicalBytes(snapshot.State, limits)
            + SnapshotRecoveryInt64LogicalBytes
            + SnapshotRecoveryDateTimeOffsetLogicalBytes);
        if (snapshot.AuthoritativeState is { } authoritative)
        {
            updated = checked(updated + GetSnapshotRecoveryPayloadLogicalBytes(authoritative, limits));
        }

        ThrowIfSnapshotRecoveryCapacityExceeded(
            updated,
            limits.MaximumLogicalBytes,
            nameof(SnapshotRecoveryLimits.MaximumLogicalBytes));
        return updated;
    }

    /// <summary>Validates one operation payload and metadata before equality comparisons.</summary>
    /// <param name="operation">The operation to validate.</param>
    /// <param name="limits">The finite recovery limits.</param>
    private static void ValidateSnapshotRecoveryOperationBounds(
        SyncOperation operation,
        SnapshotRecoveryLimits limits) =>
        _ = AddSnapshotRecoveryOperationBytes(0, operation, limits);

    /// <summary>Validates that pending and replay roles describe the same immutable operation.</summary>
    /// <param name="pending">The pending operation.</param>
    /// <param name="replay">The overlapping replay operation.</param>
    /// <exception cref="ArgumentException">The overlapping operation payload or metadata differs.</exception>
    private static void ValidateSnapshotRecoveryOverlap(SyncOperation pending, SyncOperation replay)
    {
        if (SnapshotRecoveryOperationsEqual(pending, replay))
        {
            return;
        }

        throw new ArgumentException(
            "Snapshot recovery pending/replay overlap must describe the same operation.",
            nameof(replay));
    }

    /// <summary>Determines whether two operations contain the same immutable intent.</summary>
    /// <param name="left">The first operation.</param>
    /// <param name="right">The second operation.</param>
    /// <returns>Whether the operations match.</returns>
    private static bool SnapshotRecoveryOperationsEqual(SyncOperation left, SyncOperation right) =>
        left.OperationId == right.OperationId
        && left.StreamId == right.StreamId
        && left.ClientSequence == right.ClientSequence
        && left.TimestampUtc == right.TimestampUtc
        && string.Equals(left.BaseVersion, right.BaseVersion, StringComparison.Ordinal)
        && left.Type == right.Type
        && left.Policy == right.Policy
        && SnapshotRecoveryMetadataEquals(left.Metadata, right.Metadata)
        && PayloadEnvelopeComparison.ContentEquals(left.Payload, right.Payload);

    /// <summary>Determines whether two metadata dictionaries match ordinally.</summary>
    /// <param name="left">The first dictionary.</param>
    /// <param name="right">The second dictionary.</param>
    /// <returns>Whether both dictionaries match.</returns>
    private static bool SnapshotRecoveryMetadataEquals(
        IReadOnlyDictionary<string, string> left,
        IReadOnlyDictionary<string, string> right)
    {
        if (left.Count != right.Count)
        {
            return false;
        }

        foreach (var pair in left)
        {
            if (!right.TryGetValue(pair.Key, out var value)
                || !string.Equals(pair.Value, value, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Adds one operation's logical bytes to the bounded role total.</summary>
    /// <param name="logicalBytes">The logical bytes observed so far.</param>
    /// <param name="operation">The operation to count.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <returns>The updated logical byte count.</returns>
    private static long AddSnapshotRecoveryOperationBytes(
        long logicalBytes,
        SyncOperation operation,
        SnapshotRecoveryLimits limits)
    {
        var updated = checked(logicalBytes + GetSnapshotRecoveryOperationLogicalBytes(operation, limits));
        ThrowIfSnapshotRecoveryCapacityExceeded(
            updated,
            limits.MaximumLogicalBytes,
            nameof(SnapshotRecoveryLimits.MaximumLogicalBytes));
        return updated;
    }

    /// <summary>Counts logical bytes for one recovery operation.</summary>
    /// <param name="operation">The operation to count.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetSnapshotRecoveryOperationLogicalBytes(
        SyncOperation operation,
        SnapshotRecoveryLimits limits) =>
        SnapshotRecoveryGuidLogicalBytes
        + GetRequiredSnapshotRecoveryUtf8Bytes(operation.StreamId.Value)
        + SnapshotRecoveryInt64LogicalBytes
        + SnapshotRecoveryDateTimeOffsetLogicalBytes
        + SnapshotRecoveryInt32LogicalBytes
        + GetSnapshotRecoveryPayloadLogicalBytes(operation.Payload, limits)
        + GetOptionalSnapshotRecoveryUtf8Bytes(operation.BaseVersion)
        + GetSnapshotRecoveryMetadataLogicalBytes(operation.Metadata, limits)
        + SnapshotRecoveryInt32LogicalBytes
        + SnapshotRecoveryInt32LogicalBytes
        + SnapshotRecoveryInt32LogicalBytes
        + SnapshotRecoveryInt32LogicalBytes;

    /// <summary>Counts logical bytes for one payload envelope.</summary>
    /// <param name="payload">The payload to count.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetSnapshotRecoveryPayloadLogicalBytes(PayloadEnvelope payload, SnapshotRecoveryLimits limits)
    {
        ThrowIfSnapshotRecoveryCapacityExceeded(
            payload.PayloadLength,
            limits.MaximumPayloadBytes,
            nameof(SnapshotRecoveryLimits.MaximumPayloadBytes));
        return SnapshotRecoveryInt32LogicalBytes
            + SnapshotRecoveryInt64LogicalBytes
            + GetRequiredSnapshotRecoveryUtf8Bytes(payload.ContractId)
            + GetRequiredSnapshotRecoveryUtf8Bytes(payload.ContentType)
            + GetRequiredSnapshotRecoveryUtf8Bytes(payload.PayloadHash)
            + payload.PayloadLength;
    }

    /// <summary>Counts logical bytes for operation metadata.</summary>
    /// <param name="metadata">The metadata to count.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetSnapshotRecoveryMetadataLogicalBytes(
        IReadOnlyDictionary<string, string> metadata,
        SnapshotRecoveryLimits limits)
    {
        ThrowIfSnapshotRecoveryCapacityExceeded(
            metadata.Count,
            limits.MaximumMetadataEntries,
            nameof(SnapshotRecoveryLimits.MaximumMetadataEntries));
        var bytes = SnapshotRecoveryInt32LogicalBytes;
        foreach (var pair in metadata)
        {
            bytes = checked(bytes + GetRequiredSnapshotRecoveryUtf8Bytes(pair.Key));
            bytes = checked(bytes + GetRequiredSnapshotRecoveryUtf8Bytes(pair.Value));
        }

        ThrowIfSnapshotRecoveryCapacityExceeded(
            bytes,
            limits.MaximumMetadataBytes,
            nameof(SnapshotRecoveryLimits.MaximumMetadataBytes));
        return bytes;
    }

    /// <summary>Gets UTF-8 byte count for an optional string.</summary>
    /// <param name="value">The string.</param>
    /// <returns>The UTF-8 byte count.</returns>
    private static int GetOptionalSnapshotRecoveryUtf8Bytes(string? value) =>
        value is null ? 0 : GetRequiredSnapshotRecoveryUtf8Bytes(value);

    /// <summary>Gets UTF-8 byte count for a required string.</summary>
    /// <param name="value">The string.</param>
    /// <returns>The UTF-8 byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetRequiredSnapshotRecoveryUtf8Bytes(string value) => Encoding.UTF8.GetByteCount(value);

    /// <summary>Throws when snapshot recovery capacity is exceeded.</summary>
    /// <param name="observed">The observed value.</param>
    /// <param name="maximum">The configured maximum.</param>
    /// <param name="limitName">The exceeded limit name.</param>
    /// <exception cref="SnapshotRecoveryCapacityExceededException">The observed value exceeds the maximum.</exception>
    private static void ThrowIfSnapshotRecoveryCapacityExceeded(long observed, long maximum, string limitName)
    {
        if (observed <= maximum)
        {
            return;
        }

        throw new SnapshotRecoveryCapacityExceededException(limitName, maximum, observed);
    }

    /// <summary>Selects operations that must not be replayed after the recovered checkpoint.</summary>
    /// <param name="dispositions">The remote operation dispositions.</param>
    /// <returns>The excluded operation identifiers.</returns>
    private static HashSet<OperationId> CreateSnapshotRecoveryExcludedOperations(
        IReadOnlyList<SnapshotOperationDisposition> dispositions)
    {
        HashSet<OperationId> excluded = [];
        for (var index = 0; index < dispositions.Count; index++)
        {
            var disposition = dispositions[index];
            if (disposition.Kind != SnapshotOperationDispositionKind.Unknown)
            {
                _ = excluded.Add(disposition.OperationId);
            }
        }

        return excluded;
    }

    /// <summary>Creates a bounded local copy of validated dispositions in captured operation order.</summary>
    /// <param name="operations">The captured operations.</param>
    /// <param name="dispositions">The validated remote dispositions.</param>
    /// <returns>The dispositions ordered by captured operation order.</returns>
    /// <exception cref="ArgumentException">A disposition identity is duplicated.</exception>
    private static SnapshotOperationDisposition[] CreateSnapshotRecoveryDispositionsByOperationOrder(
        IReadOnlyList<SyncOperation> operations,
        IReadOnlyList<SnapshotOperationDisposition> dispositions)
    {
        Dictionary<OperationId, SnapshotOperationDisposition> indexed = [with(capacity: dispositions.Count)];
        for (var index = 0; index < dispositions.Count; index++)
        {
            var disposition = dispositions[index];
            indexed.Add(disposition.OperationId, disposition);
        }

        var ordered = new SnapshotOperationDisposition[operations.Count];
        for (var index = 0; index < operations.Count; index++)
        {
            var operationId = operations[index].OperationId;

            // SnapshotRecoveryValidator.Validate(request, result, limits) proves the remote response
            // matched the original bounded request, while the fresh capture fence/store contract keeps
            // pending identities stable under that durable fence before this helper orders them.
            var disposition = indexed[operationId];
            _ = indexed.Remove(operationId);
            ordered[index] = disposition;
        }

        return ordered;
    }

    /// <summary>Creates the queue diagnostic aggregate after durable recovery.</summary>
    /// <param name="capture">The fresh local capture.</param>
    /// <param name="commit">The durable recovery result.</param>
    /// <param name="dispositions">The recovery operation dispositions.</param>
    /// <returns>The queue diagnostic snapshot.</returns>
    private static QueueDiagnosticSnapshot CreateSnapshotRecoveryQueueSnapshot(
        LocalSnapshotRecoveryCapture capture,
        LocalSnapshotRecoveryResult commit,
        IReadOnlyList<SnapshotOperationDisposition> dispositions)
    {
        var pendingBytes = GetPreservedPendingBytes(capture, dispositions);
        return new(commit.PreservedPendingOperationCount, pendingBytes, commit.Snapshot.Revision);
    }

    /// <summary>Counts dispositions that preserve pending local operation ownership.</summary>
    /// <param name="dispositions">The recovery operation dispositions.</param>
    /// <returns>The preserved pending operation count.</returns>
    private static int CountPreservedPendingDispositions(IReadOnlyList<SnapshotOperationDisposition> dispositions)
    {
        var count = 0;
        for (var index = 0; index < dispositions.Count; index++)
        {
            if (PreservesSnapshotRecoveryPendingOperation(dispositions[index]))
            {
                count++;
            }
        }

        return count;
    }

    /// <summary>Gets whether a disposition preserves pending local operation ownership.</summary>
    /// <param name="disposition">The recovery operation disposition.</param>
    /// <returns>Whether pending ownership is preserved.</returns>
    private static bool PreservesSnapshotRecoveryPendingOperation(SnapshotOperationDisposition disposition)
    {
        // Validated dispositions are Unknown iff Result is null; an accepted conflict is the only
        // proven server result that preserves pending ownership after recovery.
        var result = disposition.Result;
        return result is null || result.Kind == OperationResultKind.Conflict;
    }

    /// <summary>Gets the retained bytes for operations preserved by nonterminal dispositions.</summary>
    /// <param name="capture">The fresh local capture.</param>
    /// <param name="dispositions">The recovery operation dispositions ordered to the pending operations.</param>
    /// <returns>The retained byte count.</returns>
    private static long GetPreservedPendingBytes(
        LocalSnapshotRecoveryCapture capture,
        IReadOnlyList<SnapshotOperationDisposition> dispositions)
    {
        var pendingBytes = 0L;
        for (var index = 0; index < dispositions.Count; index++)
        {
            var disposition = dispositions[index];
            if (!PreservesSnapshotRecoveryPendingOperation(disposition))
            {
                continue;
            }

            var operation = capture.PendingOperations[index];
            pendingBytes = checked(pendingBytes + SyncEngine.GetOperationRetainedBytes(operation));
        }

        return pendingBytes;
    }

    /// <summary>Checks whether two local captures describe the same durable revision fence.</summary>
    /// <param name="left">The original capture.</param>
    /// <param name="right">The fresh capture.</param>
    /// <returns>Whether both captures have the same durable fence.</returns>
    private static bool SnapshotRecoveryCaptureMatches(
        LocalSnapshotRecoveryCapture left,
        LocalSnapshotRecoveryCapture right) =>
        left.SubscriptionId == right.SubscriptionId
        && left.NextClientSequence == right.NextClientSequence
        && (left.Snapshot?.Revision ?? 0) == (right.Snapshot?.Revision ?? 0)
        && string.Equals(left.ServerCursor, right.ServerCursor, StringComparison.Ordinal);

    /// <summary>Converts a bounded capture into the validator binding shape.</summary>
    /// <param name="capture">The capture to convert.</param>
    /// <returns>The recovered stream binding.</returns>
    private static RecoveredStream CreateRecoveredStream(LocalSnapshotRecoveryCapture capture) =>
        new(
            capture.SubscriptionId,
            capture.ServerCursor,
            capture.Snapshot,
            capture.PendingOperations,
            [],
            capture.NextClientSequence) { ReplayOperations = capture.ReplayOperations };

    /// <summary>Validates one local capture against current in-memory state.</summary>
    /// <param name="capture">The captured state.</param>
    /// <param name="observed">The observed committer state.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateSnapshotRecoveryCapture(
        LocalSnapshotRecoveryCapture capture,
        LocalStreamCommitterState<TState> observed) =>
        ValidateReplayRecovery(CreateRecoveredStream(capture), observed);

    /// <summary>Validates the recovered payload and pending operation count.</summary>
    /// <param name="commit">The durable local commit result.</param>
    /// <param name="payload">The optimistic payload.</param>
    /// <param name="dispositions">The validated remote operation dispositions.</param>
    /// <returns><see langword="true"/> when payload and pending count match recovery expectations.</returns>
    private static bool SnapshotRecoveryCommitMatchesPayload(
        LocalSnapshotRecoveryResult commit,
        PayloadEnvelope payload,
        IReadOnlyList<SnapshotOperationDisposition> dispositions) =>
        PayloadEnvelopeComparison.ContentEquals(commit.Snapshot.State, payload)
        && commit.PreservedPendingOperationCount == CountPreservedPendingDispositions(dispositions);

    /// <summary>Creates local and remote captures with validated bounded operation roles.</summary>
    /// <param name="capture">The store-returned capture.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <returns>The local full-union capture and the remote disjoint-role capture.</returns>
    private SnapshotRecoveryNormalizedCapture NormalizeSnapshotRecoveryCapture(
        LocalSnapshotRecoveryCapture capture,
        SnapshotRecoveryLimits limits)
    {
        var roles = CreateSnapshotRecoveryOperationRoles(capture, limits);
        var local = capture with { ReplayOperations = roles.LocalReplayOperations };
        var remote = capture with { ReplayOperations = roles.RemoteReplayOperations };
        return new(local, remote);
    }

    /// <summary>Creates local full-union and remote replay-only roles while validating bounded overlap.</summary>
    /// <param name="capture">The store-returned capture.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <returns>The normalized operation roles.</returns>
    /// <exception cref="ArgumentException">The capture contains duplicate or conflicting operations.</exception>
    /// <exception cref="SnapshotRecoveryCapacityExceededException">The returned capture exceeds limits.</exception>
    private SnapshotRecoveryOperationRoles CreateSnapshotRecoveryOperationRoles(
        LocalSnapshotRecoveryCapture capture,
        SnapshotRecoveryLimits limits)
    {
        ValidateSnapshotRecoveryRoleCounts(capture, limits);
        var logicalBytes = GetSnapshotRecoveryCaptureHeaderLogicalBytes(capture, limits);
        var pending = capture.PendingOperations;
        var replay = capture.ReplayOperations;
        var roles = new SnapshotRecoveryOperationRoleBuilder(
            [with(capacity: pending.Count)],
            [],
            [with(capacity: pending.Count)],
            [with(capacity: replay.Count)]);
        for (var index = 0; index < pending.Count; index++)
        {
            logicalBytes = AddSnapshotRecoveryPendingOperation(roles, pending[index], logicalBytes, limits);
        }

        for (var index = 0; index < replay.Count; index++)
        {
            logicalBytes = AddSnapshotRecoveryReplayOperation(roles, replay[index], logicalBytes, limits);
        }

        return new(roles.LocalReplayOperations.ToArray(), roles.RemoteReplayOperations.ToArray());
    }

    /// <summary>Gets bounded logical bytes for the returned capture header and snapshot.</summary>
    /// <param name="capture">The returned capture.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <returns>The logical byte count before operation roles.</returns>
    private long GetSnapshotRecoveryCaptureHeaderLogicalBytes(
        LocalSnapshotRecoveryCapture capture,
        SnapshotRecoveryLimits limits)
    {
        ValidateSnapshotRecoveryCaptureIdentity(capture);
        var streamIdBytes = GetRequiredSnapshotRecoveryUtf8Bytes(capture.StreamId.Value);
        ThrowIfSnapshotRecoveryCapacityExceeded(
            streamIdBytes,
            limits.MaximumStreamIdUtf8Bytes,
            nameof(SnapshotRecoveryLimits.MaximumStreamIdUtf8Bytes));
        var cursorBytes = GetOptionalSnapshotRecoveryUtf8Bytes(capture.ServerCursor);
        ThrowIfSnapshotRecoveryCapacityExceeded(
            cursorBytes,
            limits.MaximumCursorUtf8Bytes,
            nameof(SnapshotRecoveryLimits.MaximumCursorUtf8Bytes));
        var logicalBytes = checked(
            streamIdBytes
            + SnapshotRecoveryGuidLogicalBytes
            + cursorBytes
            + SnapshotRecoveryInt64LogicalBytes
            + SnapshotRecoveryInt32LogicalBytes
            + SnapshotRecoveryInt32LogicalBytes);
        ThrowIfSnapshotRecoveryCapacityExceeded(
            logicalBytes,
            limits.MaximumLogicalBytes,
            nameof(SnapshotRecoveryLimits.MaximumLogicalBytes));
        return AddSnapshotRecoveryCaptureSnapshotBytes(capture.Snapshot, logicalBytes, limits);
    }

    /// <summary>Validates the returned capture identity before deriving secondary copies.</summary>
    /// <param name="capture">The returned capture.</param>
    /// <exception cref="ArgumentException">The capture belongs to another stream or subscription.</exception>
    private void ValidateSnapshotRecoveryCaptureIdentity(LocalSnapshotRecoveryCapture capture)
    {
        if (capture.StreamId == _options.StreamId
            && capture.SubscriptionId == _options.SubscriptionId
            && capture.NextClientSequence >= 0)
        {
            return;
        }

        throw new ArgumentException("Snapshot recovery capture identity is malformed.", nameof(capture));
    }

    /// <summary>Adds one pending operation to the bounded unique union.</summary>
    /// <param name="roles">The normalized operation role builder.</param>
    /// <param name="operation">The pending operation.</param>
    /// <param name="logicalBytes">The logical bytes observed so far.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <returns>The updated logical byte count.</returns>
    /// <exception cref="ArgumentException">A pending operation identity is duplicated.</exception>
    /// <exception cref="SnapshotRecoveryCapacityExceededException">The operation exceeds configured limits.</exception>
    private long AddSnapshotRecoveryPendingOperation(
        in SnapshotRecoveryOperationRoleBuilder roles,
        SyncOperation operation,
        long logicalBytes,
        SnapshotRecoveryLimits limits)
    {
        ValidateSnapshotRecoveryOperation(operation);
        if (roles.Union.ContainsKey(operation.OperationId))
        {
            throw new ArgumentException(
                "Snapshot recovery pending operations contain duplicate operation ids.",
                nameof(operation));
        }

        ThrowIfSnapshotRecoveryCapacityExceeded(
            roles.Union.Count + 1L,
            limits.MaximumPendingOperations,
            nameof(SnapshotRecoveryLimits.MaximumPendingOperations));
        var updated = AddSnapshotRecoveryOperationBytes(logicalBytes, operation, limits);
        roles.Union.Add(operation.OperationId, operation);
        roles.LocalReplayOperations.Add(operation);
        return updated;
    }

    /// <summary>Adds one replay operation to the bounded unique union or validates a pending overlap.</summary>
    /// <param name="roles">The normalized operation role builder.</param>
    /// <param name="operation">The replay operation.</param>
    /// <param name="logicalBytes">The logical bytes observed so far.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <returns>The updated logical byte count.</returns>
    /// <exception cref="ArgumentException">A replay operation identity is duplicated or conflicts with pending.</exception>
    /// <exception cref="SnapshotRecoveryCapacityExceededException">The operation exceeds configured limits.</exception>
    private long AddSnapshotRecoveryReplayOperation(
        in SnapshotRecoveryOperationRoleBuilder roles,
        SyncOperation operation,
        long logicalBytes,
        SnapshotRecoveryLimits limits)
    {
        ValidateSnapshotRecoveryOperation(operation);
        if (!roles.ReplayIds.Add(operation.OperationId))
        {
            throw new ArgumentException(
                "Snapshot recovery replay operations contain duplicate operation ids.",
                nameof(operation));
        }

        if (roles.Union.TryGetValue(operation.OperationId, out var pending))
        {
            ValidateSnapshotRecoveryOperationBounds(operation, limits);
            ValidateSnapshotRecoveryOverlap(pending, operation);
            return logicalBytes;
        }

        ThrowIfSnapshotRecoveryCapacityExceeded(
            roles.Union.Count + 1L,
            limits.MaximumPendingOperations,
            nameof(SnapshotRecoveryLimits.MaximumPendingOperations));
        var updated = AddSnapshotRecoveryOperationBytes(logicalBytes, operation, limits);
        roles.Union.Add(operation.OperationId, operation);
        roles.LocalReplayOperations.Add(operation);
        roles.RemoteReplayOperations.Add(operation);
        return updated;
    }

    /// <summary>Validates one returned operation belongs to this bounded stream.</summary>
    /// <param name="operation">The returned operation.</param>
    /// <exception cref="ArgumentException">The operation is malformed or belongs to another stream.</exception>
    private void ValidateSnapshotRecoveryOperation(SyncOperation operation)
    {
        ArgumentExceptionHelper.ThrowIfNull(operation);
        if (operation.StreamId == _options.StreamId && operation.OperationId.Value != Guid.Empty)
        {
            return;
        }

        throw new ArgumentException(
            "Snapshot recovery operations must be non-empty and belong to the stream.",
            nameof(operation));
    }

    /// <summary>Captures bounded local recovery state from the optional capture store.</summary>
    /// <param name="store">The capture-capable store.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The bounded local capture.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ValueTask<LocalSnapshotRecoveryCapture> CaptureSnapshotRecoveryStoreAsync(
        ILocalSnapshotRecoveryCaptureStore store,
        SnapshotRecoveryLimits limits,
        CancellationToken cancellationToken) =>
        store.CaptureSnapshotRecoveryAsync(
            new() { StreamId = _options.StreamId, SubscriptionId = _options.SubscriptionId, Limits = limits },
            cancellationToken);

    /// <summary>Gets the optional capture-store facet or fails closed.</summary>
    /// <returns>The capture-store facet.</returns>
    /// <exception cref="InvalidOperationException">The configured local store cannot capture bounded snapshot recovery requests.</exception>
    private ILocalSnapshotRecoveryCaptureStore RequireSnapshotRecoveryCaptureStore()
    {
        if ((_options.Dependencies.Store.Capabilities & LocalStoreCapabilities.AtomicSnapshotRecovery) != 0
            && _options.Dependencies.Store is ILocalSnapshotRecoveryCaptureStore store)
        {
            return store;
        }

        throw new InvalidOperationException("The local store does not support bounded snapshot recovery capture.");
    }

    /// <summary>Gets the optional recovery-store facet or fails closed.</summary>
    /// <returns>The recovery-store facet.</returns>
    /// <exception cref="InvalidOperationException">The configured local store cannot apply atomic snapshot recovery.</exception>
    private ILocalSnapshotRecoveryStore RequireSnapshotRecoveryStore()
    {
        if ((_options.Dependencies.Store.Capabilities & LocalStoreCapabilities.AtomicSnapshotRecovery) != 0
            && _options.Dependencies.Store is ILocalSnapshotRecoveryStore store)
        {
            return store;
        }

        throw new InvalidOperationException("The local store does not support atomic snapshot recovery.");
    }

    /// <summary>Creates the remote recovery request from a local capture.</summary>
    /// <param name="capture">The local capture.</param>
    /// <param name="expiredCursor">The expired cursor, if known.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <returns>The remote recovery request.</returns>
    private RemoteSnapshotRecoveryRequest CreateSnapshotRecoveryRequest(
        LocalSnapshotRecoveryCapture capture,
        string? expiredCursor,
        SnapshotRecoveryLimits limits) =>
        new()
        {
            StreamId = _options.StreamId,
            SubscriptionId = _options.SubscriptionId,
            ExpiredCursor = expiredCursor,
            ClientStateContractId = _options.Contracts.StateContractId,
            ClientStateSchemaVersion = _options.Contracts.StateSchemaVersion,
            SnapshotFormatVersion = _options.Contracts.SnapshotFormatVersion,
            PendingOperations = capture.PendingOperations,
            ReplayOperations = capture.ReplayOperations,
            MaximumResponseBytes = limits.MaximumLogicalBytes,
        };

    /// <summary>Captures the current state and validates that the original remote request remains fenced.</summary>
    /// <param name="prior">The prior recovery capture.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The fresh capture.</returns>
    /// <exception cref="SnapshotRecoveryRetryableConcurrentChangeException">The durable capture fence changed.</exception>
    private async ValueTask<LocalSnapshotRecoveryCapture> CaptureFreshSnapshotRecoveryAsync(
        SnapshotRecoveryCaptureResult prior,
        SnapshotRecoveryLimits limits,
        CancellationToken cancellationToken)
    {
        var store = RequireSnapshotRecoveryCaptureStore();
        var fresh = await CaptureSnapshotRecoveryStoreAsync(store, limits, cancellationToken).ConfigureAwait(false);
        var normalized = NormalizeSnapshotRecoveryCapture(fresh, limits);
        ValidateSnapshotRecoveryCapture(normalized.LocalCapture, Current);
        if (SnapshotRecoveryCaptureMatches(prior.Capture, normalized.LocalCapture))
        {
            return normalized.LocalCapture;
        }

        throw new SnapshotRecoveryRetryableConcurrentChangeException();
    }

    /// <summary>Commits a validated recovered checkpoint and makes live state consistent after the durable commit.</summary>
    /// <param name="capture">The fresh local capture.</param>
    /// <param name="checkpoint">The recovered checkpoint.</param>
    /// <param name="result">The validated remote result.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <param name="cancellationToken">The precommit cancellation token.</param>
    /// <returns>The committed recovery result.</returns>
    private async ValueTask<SnapshotRecoveryStreamCommitResult<TState>> CommitSnapshotRecoveryAsync(
        LocalSnapshotRecoveryCapture capture,
        RemoteSnapshotCheckpoint checkpoint,
        RemoteSnapshotRecoveryResult result,
        SnapshotRecoveryLimits limits,
        CancellationToken cancellationToken)
    {
        var optimistic = await CreateSnapshotRecoveryOptimisticStateAsync(capture, checkpoint, result, cancellationToken)
            .ConfigureAwait(false);
        var payload = await SerializeSnapshotRecoveryStateAsync(optimistic, cancellationToken).ConfigureAwait(false);
        var dispositions = CreateSnapshotRecoveryDispositionsByOperationOrder(
            capture.ReplayOperations,
            result.OperationDispositions);
        var pendingDispositions = CreateSnapshotRecoveryDispositionsByOperationOrder(
            capture.PendingOperations,
            result.OperationDispositions);
        var mutation = CreateSnapshotRecoveryMutation(capture, checkpoint, dispositions, payload);
        SnapshotRecoveryValidator.Validate(mutation, CreateRecoveredStream(capture), limits);
        var commit = await RequireSnapshotRecoveryStore()
            .ApplySnapshotRecoveryAsync(mutation, cancellationToken)
            .ConfigureAwait(false);
        return CompleteSnapshotRecoveryCommit(capture, commit, checkpoint, optimistic, payload, pendingDispositions);
    }

    /// <summary>Builds optimistic state from the authoritative checkpoint plus unresolved local operations.</summary>
    /// <param name="capture">The fresh recovery capture.</param>
    /// <param name="checkpoint">The recovered checkpoint.</param>
    /// <param name="result">The remote recovery result.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The rebuilt optimistic state.</returns>
    private async ValueTask<TState> CreateSnapshotRecoveryOptimisticStateAsync(
        LocalSnapshotRecoveryCapture capture,
        RemoteSnapshotCheckpoint checkpoint,
        RemoteSnapshotRecoveryResult result,
        CancellationToken cancellationToken)
    {
        ValidateStatePayload(checkpoint.ClientState);
        var authoritative = await DecodeRecoveredSnapshotPayloadAsync(
            checkpoint.ClientState,
            checkpoint.FrontierCursor,
            cancellationToken).ConfigureAwait(false);
        var excluded = CreateSnapshotRecoveryExcludedOperations(result.OperationDispositions);
        SyncOperation[] replay = [.. capture.ReplayOperations];
        Array.Sort(replay, static (left, right) => left.ClientSequence.CompareTo(right.ClientSequence));
        return await ReplayResultOperationsAsync(authoritative, replay, excluded, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>Serializes the rebuilt optimistic state for durable storage.</summary>
    /// <param name="state">The optimistic state.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The serialized state payload.</returns>
    private async ValueTask<PayloadEnvelope> SerializeSnapshotRecoveryStateAsync(
        TState state,
        CancellationToken cancellationToken)
    {
        var payload = await _options.Dependencies.Serializer
            .SerializeAsync(_options.Contracts.StateContractId, _options.Contracts.StateSchemaVersion, state, cancellationToken)
            .ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        ValidateStatePayload(payload);
        return payload;
    }

    /// <summary>Creates the durable mutation for a recovered checkpoint.</summary>
    /// <param name="capture">The fresh local capture.</param>
    /// <param name="checkpoint">The recovered checkpoint.</param>
    /// <param name="dispositions">The recovery dispositions ordered by captured operation order.</param>
    /// <param name="payload">The optimistic payload.</param>
    /// <returns>The durable mutation.</returns>
    private LocalSnapshotRecoveryMutation CreateSnapshotRecoveryMutation(
        LocalSnapshotRecoveryCapture capture,
        RemoteSnapshotCheckpoint checkpoint,
        IReadOnlyList<SnapshotOperationDisposition> dispositions,
        PayloadEnvelope payload) =>
        new()
        {
            StreamId = _options.StreamId,
            SubscriptionId = _options.SubscriptionId,
            ExpectedRevision = capture.Snapshot?.Revision ?? 0,
            ExpectedPreviousCursor = capture.ServerCursor,
            Checkpoint = checkpoint,
            OptimisticState = payload,
            SnapshotFormatVersion = _options.Contracts.SnapshotFormatVersion,
            OperationDispositions = dispositions,
        };

    /// <summary>Completes in-memory state after durable snapshot recovery commit returns.</summary>
    /// <param name="capture">The fresh recovery capture.</param>
    /// <param name="commit">The durable recovery result.</param>
    /// <param name="checkpoint">The recovered checkpoint.</param>
    /// <param name="state">The optimistic state.</param>
    /// <param name="payload">The optimistic state payload.</param>
    /// <param name="dispositions">The recovery operation dispositions.</param>
    /// <returns>The committed recovery result.</returns>
    private SnapshotRecoveryStreamCommitResult<TState> CompleteSnapshotRecoveryCommit(
        LocalSnapshotRecoveryCapture capture,
        LocalSnapshotRecoveryResult commit,
        RemoteSnapshotCheckpoint checkpoint,
        TState state,
        PayloadEnvelope payload,
        IReadOnlyList<SnapshotOperationDisposition> dispositions)
    {
        ValidateSnapshotRecoveryCommit(commit, capture, checkpoint, payload, dispositions);
        var next = new LocalStreamCommitterState<TState>(
            _options.StreamId,
            _options.SubscriptionId,
            state,
            commit.Snapshot.Revision,
            capture.NextClientSequence,
            checkpoint.FrontierCursor) { MaterializedPayload = payload, AuthoritativePayload = checkpoint.ClientState };
        SwapCurrent(next);
        var queueSnapshot = CreateSnapshotRecoveryQueueSnapshot(capture, commit, dispositions);
        CommitQueueDiagnosticSnapshot(queueSnapshot);
        var acknowledgement = new ReceiveAcknowledgement(_options.SubscriptionId, _options.StreamId, checkpoint.FrontierCursor);
        return new(next, acknowledgement, queueSnapshot);
    }

    /// <summary>Validates the durable local recovery receipt before publishing live state.</summary>
    /// <param name="commit">The durable local commit result.</param>
    /// <param name="capture">The fresh recovery capture.</param>
    /// <param name="checkpoint">The recovered checkpoint.</param>
    /// <param name="payload">The optimistic payload.</param>
    /// <param name="dispositions">The validated remote operation dispositions.</param>
    /// <exception cref="InvalidOperationException">The durable recovery result is malformed.</exception>
    private void ValidateSnapshotRecoveryCommit(
        LocalSnapshotRecoveryResult commit,
        LocalSnapshotRecoveryCapture capture,
        RemoteSnapshotCheckpoint checkpoint,
        PayloadEnvelope payload,
        IReadOnlyList<SnapshotOperationDisposition> dispositions)
    {
        var expectedRevision = checked((capture.Snapshot?.Revision ?? 0) + 1);
        if (commit.Snapshot.StreamId == _options.StreamId
            && commit.Snapshot.FormatVersion == _options.Contracts.SnapshotFormatVersion
            && commit.Snapshot.Revision == expectedRevision
            && string.Equals(commit.Snapshot.ServerCursor, checkpoint.FrontierCursor, StringComparison.Ordinal)
            && commit.Snapshot.AuthoritativeState is not null
            && PayloadEnvelopeComparison.ContentEquals(commit.Snapshot.AuthoritativeState, checkpoint.ClientState)
            && SnapshotRecoveryCommitMatchesPayload(commit, payload, dispositions))
        {
            return;
        }

        _poisoned = true;
        throw new InvalidOperationException("The local store returned a malformed snapshot recovery result.");
    }

    /// <summary>Builds bounded pending and replay operation roles.</summary>
    /// <param name="Union">The unique pending-then-replay operation union.</param>
    /// <param name="ReplayIds">The replay role identities already observed.</param>
    /// <param name="LocalReplayOperations">The local pending-then-replay union output.</param>
    /// <param name="RemoteReplayOperations">The remote replay-only output.</param>
    private readonly record struct SnapshotRecoveryOperationRoleBuilder(
        Dictionary<OperationId, SyncOperation> Union,
        HashSet<OperationId> ReplayIds,
        List<SyncOperation> LocalReplayOperations,
        List<SyncOperation> RemoteReplayOperations);

    /// <summary>Stores local full-union and remote disjoint-role captures.</summary>
    /// <param name="LocalCapture">The local capture used for rebuild and mutation.</param>
    /// <param name="RemoteCapture">The remote capture used to build the protocol request.</param>
    private readonly record struct SnapshotRecoveryNormalizedCapture(
        LocalSnapshotRecoveryCapture LocalCapture,
        LocalSnapshotRecoveryCapture RemoteCapture);

    /// <summary>Stores derived local and remote replay operation roles.</summary>
    /// <param name="LocalReplayOperations">The local pending-then-replay union used for projection.</param>
    /// <param name="RemoteReplayOperations">The remote replay-only role used for protocol validation.</param>
    private readonly record struct SnapshotRecoveryOperationRoles(
        SyncOperation[] LocalReplayOperations,
        SyncOperation[] RemoteReplayOperations);
}
