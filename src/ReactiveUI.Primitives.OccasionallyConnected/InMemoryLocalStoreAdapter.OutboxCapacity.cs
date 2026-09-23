// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Tracks unresolved outbox capacity independently of retained store records.</summary>
internal sealed partial class InMemoryLocalStoreAdapter
{
    /// <summary>Rejects different outbox limits when this instance has already been initialized.</summary>
    /// <param name="options">The requested limits.</param>
    /// <exception cref="InvalidOperationException">This instance has different outbox limits.</exception>
    private void ValidateOutboxBinding(OutboxOptions? options)
    {
        if (_storeIdentity is not null && _outboxOptions != options)
        {
            throw new InvalidOperationException("The in-memory local store has already been initialized with different outbox limits.");
        }
    }

    /// <summary>Rejects an operation before any commit state changes when global outbox capacity is full.</summary>
    /// <param name="operation">The proposed operation.</param>
    /// <exception cref="QueueCapacityExceededException">The unresolved outbox would exceed its limits.</exception>
    private void EnsureOutboxCapacityFor(SyncOperation operation)
    {
        var options = _outboxOptions;
        if (options is null)
        {
            return;
        }

        var candidateBytes = checked(OperationCapacityBytes(operation) + MetadataCapacity(operation.Metadata).EncodedBytes);
        var unresolvedCount = 0L;
        var unresolvedBytes = 0L;
        foreach (var record in _operations.Values)
        {
            if (IsDefinitiveTerminal(record.Status.State))
            {
                continue;
            }

            unresolvedCount++;
            unresolvedBytes = checked(unresolvedBytes
                + OperationCapacityBytes(record.Operation)
                + MetadataCapacity(record.Operation.Metadata).EncodedBytes);
        }

        if (unresolvedCount < options.MaxOperations && candidateBytes <= options.MaxBytes - unresolvedBytes)
        {
            return;
        }

        throw new QueueCapacityExceededException("The unresolved outbox capacity would be exceeded.", candidateBytes <= options.MaxBytes);
    }
}
