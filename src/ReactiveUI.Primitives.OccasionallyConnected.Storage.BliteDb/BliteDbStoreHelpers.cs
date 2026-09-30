// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.BliteDb;

/// <summary>Provides stateless helpers for the BLite store state.</summary>
internal static class BliteDbStoreHelpers
{
    /// <summary>Determines whether a terminal operation is safe to remove.</summary>
    /// <param name="state">The full state used to check leases and snapshot inclusion.</param>
    /// <param name="operation">The operation considered for removal.</param>
    /// <param name="cutoffUtc">The retention cutoff.</param>
    /// <returns><see langword="true"/> when the operation meets all compaction rules.</returns>
    internal static bool CanCompact(
        BliteDbLocalStoreAdapter.StoreState state,
        BliteDbLocalStoreAdapter.OperationState operation,
        DateTimeOffset cutoffUtc)
    {
        if (!operation.Terminal || operation.Status.ChangedAtUtc >= cutoffUtc)
        {
            return false;
        }

        foreach (var lease in state.Leases.Values)
        {
            if (lease.OperationIds.Contains(operation.Operation.OperationId.Value))
            {
                return false;
            }
        }

        return operation.Status.State is SyncOperationState.Rejected or SyncOperationState.DeadLettered
            || (operation.Status.State == SyncOperationState.Synchronized
                && state.IncludedOperations.Contains(operation.Operation.OperationId.Value));
    }

    /// <summary>Gets the durable state for a stream.</summary>
    /// <param name="state">The store state.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The stream state.</returns>
    /// <exception cref="InvalidOperationException">The stream has no durable subscription identity.</exception>
    internal static BliteDbLocalStoreAdapter.StreamState GetStream(
        BliteDbLocalStoreAdapter.StoreState state,
        StreamId streamId) =>
        state.Streams.TryGetValue(streamId.Value, out var stream)
            ? stream
            : throw new InvalidOperationException("The stream has no durable subscription identity.");

    /// <summary>Finds a durable operation or throws when it is missing.</summary>
    /// <param name="state">The store state.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The matching operation state.</returns>
    /// <exception cref="InvalidOperationException">The operation is not present in the durable store.</exception>
    internal static BliteDbLocalStoreAdapter.OperationState FindOperation(
        BliteDbLocalStoreAdapter.StoreState state,
        OperationId operationId) =>
        FindOperationOrNull(state, operationId)
            ?? throw new InvalidOperationException("The operation is not present in the durable store.");

    /// <summary>Finds an operation across the stored streams.</summary>
    /// <param name="state">The store state.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The matching operation, or <see langword="null"/> when it is absent.</returns>
    internal static BliteDbLocalStoreAdapter.OperationState? FindOperationOrNull(
        BliteDbLocalStoreAdapter.StoreState state,
        OperationId operationId)
    {
        foreach (var stream in state.Streams.Values)
        {
            foreach (var operation in stream.Operations.Values)
            {
                if (operation.Operation.OperationId == operationId)
                {
                    return operation;
                }
            }
        }

        return null;
    }

    /// <summary>Validates that a server result matches the complete leased batch.</summary>
    /// <param name="state">The store state.</param>
    /// <param name="lease">The active lease.</param>
    /// <param name="result">The server result.</param>
    /// <exception cref="InvalidOperationException">The server result does not match the leased operation batch.</exception>
    internal static void ValidateResult(
        BliteDbLocalStoreAdapter.StoreState state,
        BliteDbLocalStoreAdapter.LeaseState lease,
        RemoteSyncResult result)
    {
        if (result.Operations.Count != lease.OperationIds.Count)
        {
            throw new InvalidOperationException("The remote result does not exactly match the leased operation batch.");
        }

        var seenIds = new HashSet<Guid>();
        foreach (var decision in result.Operations)
        {
            var operationId = decision.OperationId.Value;
            if (!seenIds.Add(operationId)
                || !lease.OperationIds.Contains(operationId)
                || FindOperationOrNull(state, decision.OperationId) is null)
            {
                throw new InvalidOperationException("The remote result does not exactly match the leased operation batch.");
            }
        }
    }
}
