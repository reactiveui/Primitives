// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Validates process-local snapshot recovery transactions.</summary>
internal sealed partial class InMemoryLocalStoreAdapter
{
    /// <summary>Captures and validates the requested dispositions into an owned bounded array.</summary>
    /// <param name="dispositions">The source dispositions.</param>
    /// <param name="count">The captured count observed before reservation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The validated disposition facts.</returns>
    /// <exception cref="ArgumentException">A disposition is malformed.</exception>
    /// <exception cref="ArgumentNullException">The disposition list is null.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled.</exception>
    private static SnapshotRecoveryDisposition[] CaptureSnapshotRecoveryDispositions(
        IReadOnlyList<SnapshotOperationDisposition> dispositions,
        int count,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(dispositions);
        var captured = new SnapshotRecoveryDisposition[count];
        for (var index = 0; index < count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            captured[index] = ValidateSnapshotRecoveryDisposition(dispositions[index]);
        }

        return captured;
    }

    /// <summary>Counts one validated disposition for the recovery result.</summary>
    /// <param name="disposition">The disposition.</param>
    /// <param name="replayOnly">Whether the disposition targets a replay-only operation.</param>
    /// <param name="included">The included accepted count.</param>
    /// <param name="terminal">The terminal rejected count.</param>
    /// <param name="preserved">The preserved pending count.</param>
    private static void CountSnapshotRecoveryDisposition(
        in SnapshotRecoveryDisposition disposition,
        bool replayOnly,
        ref int included,
        ref int terminal,
        ref int preserved)
    {
        if (disposition.Kind == SnapshotOperationDispositionKind.IncludedAccepted
            && (replayOnly || disposition.ResultKind == OperationResultKind.Accepted))
        {
            included++;
            return;
        }

        if (disposition.Kind == SnapshotOperationDispositionKind.TerminalRejected)
        {
            terminal++;
            return;
        }

        preserved++;
    }

    /// <summary>Validates the recovery mutation shape before applying live store fences.</summary>
    /// <param name="mutation">The mutation.</param>
    /// <exception cref="ArgumentException">The mutation is malformed.</exception>
    /// <exception cref="ArgumentNullException">The mutation is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A revision or format version is invalid.</exception>
    private static void ValidateSnapshotRecoveryMutationShape(LocalSnapshotRecoveryMutation mutation)
    {
        ArgumentExceptionHelper.ThrowIfNull(mutation);
        InMemoryLocalStoreAdapterValidation.ValidateStreamId(mutation.StreamId, nameof(mutation));
        InMemoryLocalStoreAdapterValidation.ValidateRecoveryInput(mutation.StreamId, mutation.SubscriptionId);
        ArgumentExceptionHelper.ThrowIfNull(mutation.Checkpoint);
        InMemoryLocalStoreAdapterValidation.ValidateStreamId(mutation.Checkpoint.StreamId, nameof(mutation));
        InMemoryLocalStoreAdapterValidation.ValidateRecoveryInput(mutation.Checkpoint.StreamId, mutation.Checkpoint.SubscriptionId);
        ValidateSnapshotRecoveryPayload(mutation.Checkpoint.ClientState, nameof(mutation));
        ValidateSnapshotRecoveryPayload(mutation.OptimisticState, nameof(mutation));
        if (mutation.Checkpoint.StreamId != mutation.StreamId || mutation.Checkpoint.SubscriptionId != mutation.SubscriptionId)
        {
            throw new ArgumentException("Snapshot recovery checkpoint must match the recovered stream and subscription.", nameof(mutation));
        }

        if (string.IsNullOrWhiteSpace(mutation.Checkpoint.FrontierCursor) || string.IsNullOrWhiteSpace(mutation.Checkpoint.ServerVersion))
        {
            throw new ArgumentException("Snapshot recovery checkpoint must include a frontier cursor and server version.", nameof(mutation));
        }

        if (mutation.ExpectedRevision < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(mutation), mutation.ExpectedRevision, "Expected revision must be non-negative.");
        }

        ValidateSnapshotRecoveryVersionValue(mutation.SnapshotFormatVersion, nameof(mutation));
        ValidateSnapshotRecoveryVersionValue(mutation.Checkpoint.SnapshotFormatVersion, nameof(mutation));
    }

    /// <summary>Validates one disposition and carries typed non-null result facts forward.</summary>
    /// <param name="disposition">The disposition.</param>
    /// <returns>The validated disposition facts.</returns>
    /// <exception cref="ArgumentException">The disposition is malformed.</exception>
    /// <exception cref="ArgumentNullException">The disposition is null.</exception>
    private static SnapshotRecoveryDisposition ValidateSnapshotRecoveryDisposition(SnapshotOperationDisposition disposition)
    {
        ArgumentExceptionHelper.ThrowIfNull(disposition);
        InMemoryLocalStoreAdapterValidation.ValidateOperationId(disposition.OperationId, nameof(disposition));
        return disposition.Kind switch
        {
            SnapshotOperationDispositionKind.Unknown => ValidateUnknownSnapshotRecoveryDisposition(disposition),
            SnapshotOperationDispositionKind.IncludedAccepted => ValidateIncludedSnapshotRecoveryDisposition(disposition),
            SnapshotOperationDispositionKind.TerminalRejected => ValidateTerminalSnapshotRecoveryDisposition(disposition),
            _ => throw new ArgumentException("Snapshot recovery disposition kind is not supported.", nameof(disposition)),
        };
    }

    /// <summary>Validates an unknown disposition.</summary>
    /// <param name="disposition">The disposition.</param>
    /// <returns>The validated disposition facts.</returns>
    /// <exception cref="ArgumentException">The disposition carries server result proof.</exception>
    private static SnapshotRecoveryDisposition ValidateUnknownSnapshotRecoveryDisposition(SnapshotOperationDisposition disposition)
    {
        if (disposition.Result is not null)
        {
            throw new ArgumentException("Unknown snapshot recovery dispositions must not include a result.", nameof(disposition));
        }

        return new(disposition.OperationId, disposition.Kind, OperationResultKind.Retryable, null);
    }

    /// <summary>Validates an included accepted disposition.</summary>
    /// <param name="disposition">The disposition.</param>
    /// <returns>The validated disposition facts.</returns>
    /// <exception cref="ArgumentException">The result does not prove an accepted or conflict outcome.</exception>
    private static SnapshotRecoveryDisposition ValidateIncludedSnapshotRecoveryDisposition(SnapshotOperationDisposition disposition)
    {
        var result = ValidateSnapshotRecoveryResult(disposition);
        if (result.Kind is OperationResultKind.Accepted or OperationResultKind.Conflict)
        {
            return new(disposition.OperationId, disposition.Kind, result.Kind, result.ReasonCode);
        }

        throw new ArgumentException(
            "Included snapshot recovery dispositions must carry accepted or conflict result proof.",
            nameof(disposition));
    }

    /// <summary>Validates a terminal rejected disposition.</summary>
    /// <param name="disposition">The disposition.</param>
    /// <returns>The validated disposition facts.</returns>
    /// <exception cref="ArgumentException">The result does not prove a rejected outcome.</exception>
    private static SnapshotRecoveryDisposition ValidateTerminalSnapshotRecoveryDisposition(SnapshotOperationDisposition disposition)
    {
        var result = ValidateSnapshotRecoveryResult(disposition);
        if (result.Kind == OperationResultKind.Rejected)
        {
            return new(disposition.OperationId, disposition.Kind, result.Kind, result.ReasonCode);
        }

        throw new ArgumentException("Terminal snapshot recovery dispositions must carry rejected result proof.", nameof(disposition));
    }

    /// <summary>Validates server result proof attached to a disposition.</summary>
    /// <param name="disposition">The disposition.</param>
    /// <returns>The validated server result.</returns>
    /// <exception cref="ArgumentException">The result is missing or belongs to another operation.</exception>
    private static OperationSyncResult ValidateSnapshotRecoveryResult(SnapshotOperationDisposition disposition)
    {
        if (disposition.Result is { } result && result.OperationId == disposition.OperationId)
        {
            return result;
        }

        throw new ArgumentException("Snapshot recovery result proof must match the disposition operation.", nameof(disposition));
    }

    /// <summary>Validates a snapshot recovery payload envelope.</summary>
    /// <param name="payload">The payload.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentException">The payload is malformed.</exception>
    /// <exception cref="ArgumentNullException">The payload is null.</exception>
    private static void ValidateSnapshotRecoveryPayload(PayloadEnvelope payload, string parameterName)
    {
        ArgumentExceptionHelper.ThrowIfNull(payload);
        var hasMetadata = payload.SchemaVersion > 0
            && !string.IsNullOrWhiteSpace(payload.ContractId)
            && !string.IsNullOrWhiteSpace(payload.ContentType)
            && !string.IsNullOrWhiteSpace(payload.PayloadHash);
        if (hasMetadata)
        {
            return;
        }

        throw new ArgumentException("Snapshot recovery payload metadata must be complete.", parameterName);
    }

    /// <summary>Validates the live stream version expected by a recovery mutation.</summary>
    /// <param name="stream">The live stream record.</param>
    /// <param name="mutation">The recovery mutation.</param>
    /// <exception cref="InvalidOperationException">The live stream version does not match the mutation fence.</exception>
    private static void ValidateSnapshotRecoveryVersion(StreamRecord stream, LocalSnapshotRecoveryMutation mutation)
    {
        if (stream.SubscriptionId != mutation.SubscriptionId)
        {
            throw new InvalidOperationException("Snapshot recovery subscription does not match the local stream.");
        }

        if (!SnapshotRecoveryCursorsEqual(stream.ServerCursor, mutation.ExpectedPreviousCursor))
        {
            throw new InvalidOperationException("Snapshot recovery cursor fence does not match the local stream.");
        }

        var currentRevision = stream.Snapshot?.Revision ?? 0;
        if (currentRevision == mutation.ExpectedRevision)
        {
            return;
        }

        throw new InvalidOperationException("Snapshot recovery revision fence does not match the local stream.");
    }

    /// <summary>Validates a positive snapshot format version.</summary>
    /// <param name="snapshotFormatVersion">The snapshot format version.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentOutOfRangeException">The version is not positive.</exception>
    private static void ValidateSnapshotRecoveryVersionValue(int snapshotFormatVersion, string parameterName)
    {
        if (snapshotFormatVersion > 0)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(parameterName, snapshotFormatVersion, "Snapshot format version must be positive.");
    }

    /// <summary>Validates that recovery dispositions exactly match pending then replay-only operations in order.</summary>
    /// <param name="matches">The selected recovery operation matches.</param>
    /// <param name="dispositions">The validated dispositions.</param>
    /// <exception cref="ArgumentException">The dispositions do not match the local recovery frontier.</exception>
    private static void ValidateSnapshotRecoveryDispositions(
        List<SnapshotRecoveryOperationMatch> matches,
        SnapshotRecoveryDisposition[] dispositions)
    {
        if (matches.Count != dispositions.Length)
        {
            throw new ArgumentException("Snapshot recovery dispositions must exactly match local recovery operations.", nameof(dispositions));
        }

        HashSet<OperationId> seen = [];
        for (var index = 0; index < dispositions.Length; index++)
        {
            var disposition = dispositions[index];
            if (!seen.Add(disposition.OperationId))
            {
                throw new ArgumentException("Snapshot recovery dispositions must not contain duplicate operations.", nameof(dispositions));
            }

            var match = matches[index];
            if (match.Record.Operation.OperationId != disposition.OperationId)
            {
                var message = match.ReplayOnly
                    ? "Snapshot recovery dispositions must preserve replay operation order after pending operations."
                    : "Snapshot recovery dispositions must preserve pending operation order.";
                throw new ArgumentException(message, nameof(dispositions));
            }

            if (!match.ReplayOnly)
            {
                continue;
            }

            if (disposition.Kind == SnapshotOperationDispositionKind.IncludedAccepted
                && disposition.ResultKind == OperationResultKind.Accepted)
            {
                continue;
            }

            throw new ArgumentException("Replay-only snapshot recovery dispositions must carry accepted inclusion proof.", nameof(dispositions));
        }
    }

    /// <summary>Compares optional cursors using ordinal token equality.</summary>
    /// <param name="left">The first cursor.</param>
    /// <param name="right">The second cursor.</param>
    /// <returns>Whether the cursors are equal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool SnapshotRecoveryCursorsEqual(string? left, string? right) =>
        string.Equals(left, right, StringComparison.Ordinal);
}
