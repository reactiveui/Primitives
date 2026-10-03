// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Provides shared validation for internal server snapshot recovery journal requests.</summary>
internal static class ServerSnapshotRecoveryJournalOperations
{
    /// <summary>Creates operation keys and canonical fingerprints for one recovery read.</summary>
    /// <param name="request">The read request.</param>
    /// <param name="fingerprints">The computed fingerprints.</param>
    /// <returns>The trusted operation keys.</returns>
    internal static ServerOperationKey[] CaptureOperationProofs(
        ServerSnapshotRecoveryReadRequest request,
        out ServerCommitFingerprint[] fingerprints)
    {
        fingerprints = CaptureOperationFingerprints(request.StreamKey, request.Subscription, request.RecoveryRequest, request.Limits);
        return CaptureOperationKeys(request.Subscription, request.RecoveryRequest);
    }

    /// <summary>Creates canonical fingerprints for the current pending and replay-only operations.</summary>
    /// <param name="streamKey">The trusted stream key.</param>
    /// <param name="subscription">The trusted subscription identity.</param>
    /// <param name="request">The bounded recovery request.</param>
    /// <param name="limits">The configured limits.</param>
    /// <returns>The trusted operation fingerprints.</returns>
    internal static ServerCommitFingerprint[] CaptureOperationFingerprints(
        ServerStreamKey streamKey,
        ServerSubscriptionIdentity subscription,
        RemoteSnapshotRecoveryRequest request,
        SnapshotRecoveryLimits limits)
    {
        var fingerprints = new ServerCommitFingerprint[GetOperationUnionCount(request)];
        var budget = GetCanonicalFingerprintBudget(limits);
        CaptureOperationRoleFingerprints(streamKey, subscription, request.PendingOperations, budget, fingerprints, 0);
        CaptureOperationRoleFingerprints(streamKey, subscription, request.ReplayOperations, budget, fingerprints, request.PendingOperations.Count);

        return fingerprints;
    }

    /// <summary>Creates retained operation dispositions from a stream snapshot and trusted fingerprints.</summary>
    /// <param name="snapshot">The stream snapshot.</param>
    /// <param name="operationKeys">The requested operation keys.</param>
    /// <param name="fingerprints">The exact requested operation fingerprints.</param>
    /// <returns>The owned operation dispositions.</returns>
    internal static ServerSnapshotOperationDisposition[] CreateOperationDispositions(
        ServerCommitSnapshot snapshot,
        IReadOnlyList<ServerOperationKey> operationKeys,
        IReadOnlyList<ServerCommitFingerprint> fingerprints)
    {
        var dispositions = new ServerSnapshotOperationDisposition[operationKeys.Count];
        for (var index = 0; index < dispositions.Length; index++)
        {
            var entry = FindEntry(snapshot, operationKeys[index]);
            dispositions[index] = entry is not null && entry.Fingerprint.Matches(fingerprints[index])
                ? CreatePositiveDisposition(entry)
                : CreateUnknownDisposition(operationKeys[index].OperationId);
        }

        return dispositions;
    }

    /// <summary>Checks whether all retained positive proofs still match current ledger entries.</summary>
    /// <param name="identity">The trusted subscription identity.</param>
    /// <param name="view">The captured recovery view.</param>
    /// <param name="current">The current stream snapshot.</param>
    /// <returns>Whether positive proofs still match.</returns>
    internal static bool PositiveProofsMatch(
        ServerSubscriptionIdentity identity,
        ServerSnapshotRecoveryView view,
        ServerCommitSnapshot current)
    {
        var dispositions = view.OperationDispositions;
        for (var index = 0; index < dispositions.Count; index++)
        {
            var disposition = dispositions[index];
            if (disposition.Kind == SnapshotOperationDispositionKind.Unknown)
            {
                continue;
            }

            var entry = FindEntry(current, new(identity.ClientId, disposition.OperationId));
            if (entry is null
                || disposition.Fingerprint is null
                || !entry.Fingerprint.Matches(disposition.Fingerprint)
                || !Equals(entry.Result, disposition.Result))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Rejects acknowledging a snapshot offer created for a different subscription generation.</summary>
    /// <param name="offer">The retained offer.</param>
    /// <param name="generation">The current subscription generation.</param>
    /// <exception cref="InvalidOperationException">The snapshot offer belongs to another generation.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void ThrowIfSnapshotOfferGenerationMismatch(ServerSubscriptionOffer offer, long generation)
    {
        if (offer.SnapshotSubscriptionGeneration is null)
        {
            return;
        }

        if (offer.SnapshotSubscriptionGeneration == generation)
        {
            return;
        }

        throw new InvalidOperationException("The acknowledgement cursor belongs to another subscription generation.");
    }

    /// <summary>Checks whether a remote recovered result matches the captured positive/unknown proofs.</summary>
    /// <param name="view">The captured recovery view.</param>
    /// <param name="result">The remote recovery result.</param>
    /// <returns>Whether dispositions are consistent.</returns>
    internal static bool RecoveryResultMatchesView(ServerSnapshotRecoveryView view, RemoteSnapshotRecoveryResult result)
    {
        if (result.Status != RemoteSnapshotRecoveryStatus.Recovered || result.Checkpoint is null)
        {
            return false;
        }

        var server = view.OperationDispositions;
        var remote = result.OperationDispositions;
        if (server.Count != remote.Count)
        {
            return false;
        }

        Dictionary<OperationId, ServerSnapshotOperationDisposition> serverByOperation = [with(capacity: server.Count)];
        for (var index = 0; index < server.Count; index++)
        {
            var serverDisposition = server[index];
            if (serverByOperation.ContainsKey(serverDisposition.OperationId))
            {
                return false;
            }

            serverByOperation.Add(serverDisposition.OperationId, serverDisposition);
        }

        HashSet<OperationId> matchedOperations = [];
        for (var index = 0; index < remote.Count; index++)
        {
            if (!RemoteDispositionMatches(serverByOperation, matchedOperations, remote[index]))
            {
                return false;
            }
        }

        return matchedOperations.Count == serverByOperation.Count;
    }

    /// <summary>Checks whether the offer request is still bound to the exact recovery request that produced the view.</summary>
    /// <param name="request">The offer request.</param>
    /// <returns>Whether the current request matches the captured stream, expired cursor and requested operation intents.</returns>
    internal static bool OfferRequestMatchesView(ServerSnapshotOfferRequest request)
    {
        if (request.View.Snapshot.StreamKey != request.StreamKey
            || request.View.CapturedPendingOperationCount != request.RecoveryRequest.PendingOperations.Count
            || !string.Equals(request.View.RequestedExpiredCursor, request.RecoveryRequest.ExpiredCursor, StringComparison.Ordinal))
        {
            return false;
        }

        var currentFingerprints = CaptureOperationFingerprints(request.StreamKey, request.Subscription, request.RecoveryRequest, request.Limits);
        return OperationFingerprintsMatch(request.View, request.RecoveryRequest, currentFingerprints)
            && ReplayOnlyProofsAreAccepted(request.View, request.RecoveryRequest);
    }

    /// <summary>Checks whether every replay-only operation has retained accepted proof.</summary>
    /// <param name="view">The captured view.</param>
    /// <param name="request">The recovery request.</param>
    /// <returns>Whether replay-only proof is complete and accepted.</returns>
    internal static bool ReplayOnlyProofsAreAccepted(ServerSnapshotRecoveryView view, RemoteSnapshotRecoveryRequest request)
    {
        var replayOperations = request.ReplayOperations;
        var replayStart = view.CapturedPendingOperationCount;
        if (replayStart != request.PendingOperations.Count
            || view.OperationDispositions.Count != replayStart + replayOperations.Count)
        {
            return false;
        }

        for (var index = 0; index < replayOperations.Count; index++)
        {
            var disposition = view.OperationDispositions[replayStart + index];
            var result = disposition.Result;
            if (disposition.OperationId != replayOperations[index].OperationId
                || disposition.Kind != SnapshotOperationDispositionKind.IncludedAccepted
                || result is null
                || result.Kind != OperationResultKind.Accepted)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Checks whether two payload envelopes are identical without relying on reference identity.</summary>
    /// <param name="left">The first payload.</param>
    /// <param name="right">The second payload.</param>
    /// <returns>Whether both payloads are equal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool PayloadMatches(PayloadEnvelope? left, PayloadEnvelope right) =>
        left is not null
        && string.Equals(left.ContractId, right.ContractId, StringComparison.Ordinal)
        && left.SchemaVersion == right.SchemaVersion
        && string.Equals(left.ContentType, right.ContentType, StringComparison.Ordinal)
        && FixedTimeEquals(left.PayloadHash, right.PayloadHash)
        && left.Payload.Span.SequenceEqual(right.Payload.Span);

    /// <summary>Creates the frontier cursor for a complete group frontier.</summary>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="stream">The retained stream.</param>
    /// <param name="snapshot">The atomic snapshot.</param>
    /// <returns>The cursor representing the complete group frontier.</returns>
    internal static string CreateFrontierCursor(ServerStreamKey streamKey, ServerCommitStreamRecord? stream, ServerCommitSnapshot snapshot)
    {
        if (stream is not null && stream.Groups.Count > 0)
        {
            var last = stream.Groups[stream.Groups.Count - 1];
            if (last.GroupSequence == snapshot.LastGroupSequence && last.Entry.Events.Count > 0 && snapshot.LastCursor is not null)
            {
                return snapshot.LastCursor;
            }
        }

        return ServerReceiveGroupCursor.Create(streamKey, snapshot.LastGroupSequence);
    }

    /// <summary>Validates a retained view read request before a transaction can observe state.</summary>
    /// <param name="request">The read request.</param>
    /// <exception cref="ArgumentException">The request is malformed or not bound to the authenticated subscription.</exception>
    internal static void ValidateReadRequest(ServerSnapshotRecoveryReadRequest request)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        ServerCommitJournalGuard.ValidateStreamKey(request.StreamKey);
        ServerSubscriptionJournalOperations.ValidateIdentity(request.Subscription);
        ArgumentExceptionHelper.ThrowIfNull(request.RecoveryRequest);
        ArgumentExceptionHelper.ThrowIfNull(request.Limits);
        if (request.Subscription.StreamKey != request.StreamKey
            || request.RecoveryRequest.StreamId != request.StreamKey.StreamId
            || request.RecoveryRequest.SubscriptionId != request.Subscription.SubscriptionId)
        {
            throw new ArgumentException("The snapshot recovery read request is not bound to the authenticated subscription.", nameof(request));
        }

        SnapshotRecoveryValidator.Validate(request.RecoveryRequest, request.Limits);
    }

    /// <summary>Validates a snapshot cursor offer request before durable mutation.</summary>
    /// <param name="request">The offer request.</param>
    /// <returns>Whether the remote recovery result is structurally valid for the current request.</returns>
    /// <exception cref="ArgumentException">The request is malformed or not bound to the authenticated subscription.</exception>
    internal static bool ValidateOfferRequest(ServerSnapshotOfferRequest request)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        ServerCommitJournalGuard.ValidateStreamKey(request.StreamKey);
        ServerSubscriptionJournalOperations.ValidateIdentity(request.Subscription);
        ArgumentExceptionHelper.ThrowIfNull(request.View);
        ArgumentExceptionHelper.ThrowIfNull(request.RecoveryRequest);
        ArgumentExceptionHelper.ThrowIfNull(request.RecoveryResult);
        ArgumentExceptionHelper.ThrowIfNull(request.Limits);
        if (request.Subscription.StreamKey != request.StreamKey
            || request.RecoveryRequest.StreamId != request.StreamKey.StreamId
            || request.RecoveryRequest.SubscriptionId != request.Subscription.SubscriptionId)
        {
            throw new ArgumentException("The snapshot recovery offer request is not bound to the authenticated subscription.", nameof(request));
        }

        SnapshotRecoveryValidator.Validate(request.RecoveryRequest, request.Limits);
        return RecoveryResultIsValid(request);
    }

    /// <summary>Checks whether a recovered result is structurally valid for the offer request.</summary>
    /// <param name="request">The offer request.</param>
    /// <returns>Whether the result is valid.</returns>
    private static bool RecoveryResultIsValid(ServerSnapshotOfferRequest request)
    {
        try
        {
            SnapshotRecoveryValidator.Validate(request.RecoveryRequest, request.RecoveryResult, request.Limits);
            return true;
        }
        catch (EncoderFallbackException)
        {
            return false;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    /// <summary>Checks whether one remote disposition matches a captured server disposition exactly once.</summary>
    /// <param name="serverByOperation">The captured server dispositions by operation id.</param>
    /// <param name="matchedOperations">The already matched operation ids.</param>
    /// <param name="remoteDisposition">The remote disposition to validate.</param>
    /// <returns>Whether the remote disposition matches the captured proof.</returns>
    private static bool RemoteDispositionMatches(
        Dictionary<OperationId, ServerSnapshotOperationDisposition> serverByOperation,
        HashSet<OperationId> matchedOperations,
        SnapshotOperationDisposition remoteDisposition) =>
        matchedOperations.Add(remoteDisposition.OperationId)
        && serverByOperation.TryGetValue(remoteDisposition.OperationId, out var serverDisposition)
        && serverDisposition.Kind == remoteDisposition.Kind
        && Equals(serverDisposition.Result, remoteDisposition.Result);

    /// <summary>Creates a positive disposition from a retained ledger entry.</summary>
    /// <param name="entry">The retained entry.</param>
    /// <returns>The disposition.</returns>
    private static ServerSnapshotOperationDisposition CreatePositiveDisposition(ServerLedgerEntry entry) =>
        new()
        {
            OperationId = entry.OperationKey.OperationId,
            Kind = entry.Result.Kind == OperationResultKind.Rejected
                ? SnapshotOperationDispositionKind.TerminalRejected
                : SnapshotOperationDispositionKind.IncludedAccepted,
            Result = entry.Result,
            Fingerprint = entry.Fingerprint,
        };

    /// <summary>Creates an unknown disposition for a missing or mismatched retained proof.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The disposition.</returns>
    private static ServerSnapshotOperationDisposition CreateUnknownDisposition(OperationId operationId) =>
        new() { OperationId = operationId, Kind = SnapshotOperationDispositionKind.Unknown, Result = null, Fingerprint = null };

    /// <summary>Creates operation keys for pending and replay-only operations.</summary>
    /// <param name="subscription">The trusted subscription identity.</param>
    /// <param name="request">The recovery request.</param>
    /// <returns>The trusted operation keys.</returns>
    private static ServerOperationKey[] CaptureOperationKeys(ServerSubscriptionIdentity subscription, RemoteSnapshotRecoveryRequest request)
    {
        var keys = new ServerOperationKey[GetOperationUnionCount(request)];
        CaptureOperationRoleKeys(subscription, request.PendingOperations, keys, 0);
        CaptureOperationRoleKeys(subscription, request.ReplayOperations, keys, request.PendingOperations.Count);

        return keys;
    }

    /// <summary>Checks whether current pending and replay-only operations match captured operation ids and fingerprints.</summary>
    /// <param name="view">The captured view.</param>
    /// <param name="request">The current recovery request.</param>
    /// <param name="currentFingerprints">The current operation fingerprints.</param>
    /// <returns>Whether the request is unchanged.</returns>
    private static bool OperationFingerprintsMatch(
        ServerSnapshotRecoveryView view,
        RemoteSnapshotRecoveryRequest request,
        ServerCommitFingerprint[] currentFingerprints)
    {
        if (view.OperationDispositions.Count != currentFingerprints.Length
            || view.OperationFingerprints.Count != currentFingerprints.Length
            || currentFingerprints.Length != GetOperationUnionCount(request))
        {
            return false;
        }

        return OperationRoleFingerprintsMatch(view, request.PendingOperations, currentFingerprints, 0)
            && OperationRoleFingerprintsMatch(view, request.ReplayOperations, currentFingerprints, request.PendingOperations.Count);
    }

    /// <summary>Gets the combined pending and replay-only operation count.</summary>
    /// <param name="request">The recovery request.</param>
    /// <returns>The operation count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetOperationUnionCount(RemoteSnapshotRecoveryRequest request) =>
        request.PendingOperations.Count + request.ReplayOperations.Count;

    /// <summary>Captures canonical fingerprints for one operation role.</summary>
    /// <param name="streamKey">The trusted stream key.</param>
    /// <param name="subscription">The trusted subscription identity.</param>
    /// <param name="operations">The role operations.</param>
    /// <param name="budget">The canonical fingerprint byte budget.</param>
    /// <param name="fingerprints">The target fingerprint array.</param>
    /// <param name="offset">The target offset.</param>
    private static void CaptureOperationRoleFingerprints(
        ServerStreamKey streamKey,
        ServerSubscriptionIdentity subscription,
        IReadOnlyList<SyncOperation> operations,
        int budget,
        ServerCommitFingerprint[] fingerprints,
        int offset)
    {
        for (var index = 0; index < operations.Count; index++)
        {
            fingerprints[offset + index] = new(CanonicalOperationFingerprint.Compute(
                streamKey.TenantId,
                subscription.ClientId,
                operations[index],
                budget));
        }
    }

    /// <summary>Captures operation keys for one operation role.</summary>
    /// <param name="subscription">The trusted subscription identity.</param>
    /// <param name="operations">The role operations.</param>
    /// <param name="keys">The target key array.</param>
    /// <param name="offset">The target offset.</param>
    private static void CaptureOperationRoleKeys(
        ServerSubscriptionIdentity subscription,
        IReadOnlyList<SyncOperation> operations,
        ServerOperationKey[] keys,
        int offset)
    {
        for (var index = 0; index < operations.Count; index++)
        {
            keys[offset + index] = new(subscription.ClientId, operations[index].OperationId);
        }
    }

    /// <summary>Checks whether one operation role matches captured operation ids and fingerprints.</summary>
    /// <param name="view">The captured view.</param>
    /// <param name="operations">The role operations.</param>
    /// <param name="currentFingerprints">The current operation fingerprints.</param>
    /// <param name="offset">The role offset.</param>
    /// <returns>Whether the operation role is unchanged.</returns>
    private static bool OperationRoleFingerprintsMatch(
        ServerSnapshotRecoveryView view,
        IReadOnlyList<SyncOperation> operations,
        ServerCommitFingerprint[] currentFingerprints,
        int offset)
    {
        for (var index = 0; index < operations.Count; index++)
        {
            var unionIndex = offset + index;
            if (view.OperationDispositions[unionIndex].OperationId != operations[index].OperationId
                || !view.OperationFingerprints[unionIndex].Matches(currentFingerprints[unionIndex]))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Finds a retained entry by operation key.</summary>
    /// <param name="snapshot">The stream snapshot.</param>
    /// <param name="operationKey">The operation key.</param>
    /// <returns>The entry or null.</returns>
    private static ServerLedgerEntry? FindEntry(ServerCommitSnapshot snapshot, ServerOperationKey operationKey)
    {
        for (var index = 0; index < snapshot.Entries.Count; index++)
        {
            if (snapshot.Entries[index].OperationKey == operationKey)
            {
                return snapshot.Entries[index];
            }
        }

        return null;
    }

    /// <summary>Gets the bounded canonical fingerprint budget from snapshot limits.</summary>
    /// <param name="limits">The limits.</param>
    /// <returns>The fingerprint byte budget.</returns>
    private static int GetCanonicalFingerprintBudget(SnapshotRecoveryLimits limits) =>
        limits.MaximumLogicalBytes > int.MaxValue ? int.MaxValue : (int)limits.MaximumLogicalBytes;

    /// <summary>Compares two hashes without early exit.</summary>
    /// <param name="left">The left hash.</param>
    /// <param name="right">The right hash.</param>
    /// <returns>Whether the values are equal.</returns>
    private static bool FixedTimeEquals(string left, string right)
    {
        var leftBytes = Encoding.UTF8.GetBytes(left);
        var rightBytes = Encoding.UTF8.GetBytes(right);
#if NETFRAMEWORK
        var difference = leftBytes.Length ^ rightBytes.Length;
        var count = Math.Min(leftBytes.Length, rightBytes.Length);
        for (var index = 0; index < count; index++)
        {
            difference |= leftBytes[index] ^ rightBytes[index];
        }

        return difference == 0;
#else
        return CryptographicOperations.FixedTimeEquals(leftBytes, rightBytes);
#endif
    }
}
