// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Contains storage records for <see cref="InMemoryLocalStoreAdapter"/>.</summary>
internal sealed partial class InMemoryLocalStoreAdapter
{
    /// <summary>Describes retained logical records and encoded data bytes.</summary>
    /// <param name="Records">The logical retained record count.</param>
    /// <param name="EncodedBytes">The retained encoded data bytes.</param>
    private readonly record struct CapacityUsage(int Records, long EncodedBytes);

    /// <summary>Identifies a remote event inbox entry.</summary>
    /// <param name="StreamId">The stream identifier.</param>
    /// <param name="EventId">The remote event identifier.</param>
    private readonly record struct InboxKey(StreamId StreamId, Guid EventId);

    /// <summary>Stores current lease ownership.</summary>
    private sealed class LeaseRecord
    {
        /// <summary>Initializes a new instance of the <see cref="LeaseRecord"/> class.</summary>
        /// <param name="leaseId">The lease identifier.</param>
        /// <param name="expiresAtUtc">The expiry timestamp.</param>
        /// <param name="operationIds">The leased operation identifiers.</param>
        internal LeaseRecord(Guid leaseId, DateTimeOffset expiresAtUtc, List<OperationId> operationIds)
        {
            LeaseId = leaseId;
            ExpiresAtUtc = expiresAtUtc;
            OperationIds = new(operationIds);
        }

        /// <summary>Gets the lease identifier.</summary>
        internal Guid LeaseId { get; }

        /// <summary>Gets or sets the expiry timestamp.</summary>
        internal DateTimeOffset ExpiresAtUtc { get; set; }

        /// <summary>Gets the leased operation identifiers.</summary>
        internal ReadOnlyCollection<OperationId> OperationIds { get; }

        /// <summary>Determines whether the lease owns an operation.</summary>
        /// <param name="operationId">The operation identifier.</param>
        /// <returns>Whether the operation is owned by this lease.</returns>
        internal bool Owns(OperationId operationId)
        {
            for (var index = 0; index < OperationIds.Count; index++)
            {
                if (OperationIds[index] == operationId)
                {
                    return true;
                }
            }

            return false;
        }
    }

    /// <summary>Stores one local operation and its durable state.</summary>
    private sealed class OperationRecord
    {
        /// <summary>Initializes a new instance of the <see cref="OperationRecord"/> class.</summary>
        /// <param name="operation">The operation.</param>
        /// <param name="snapshotMutation">The committed snapshot mutation.</param>
        /// <param name="receipt">The original local commit receipt.</param>
        /// <param name="status">The current operation status.</param>
        internal OperationRecord(
            SyncOperation operation,
            SnapshotMutation snapshotMutation,
            LocalCommitResult receipt,
            SyncOperationStatus status)
        {
            Operation = operation;
            SnapshotMutation = snapshotMutation;
            Receipt = receipt;
            Status = status;
        }

        /// <summary>Gets or sets the upload attempt count.</summary>
        internal int Attempt { get; set; }

        /// <summary>Gets or sets the current lease expiry timestamp.</summary>
        internal DateTimeOffset? LeaseExpiresAtUtc { get; set; }

        /// <summary>Gets or sets the current lease identifier.</summary>
        internal Guid? LeaseId { get; set; }

        /// <summary>Gets the operation.</summary>
        internal SyncOperation Operation { get; }

        /// <summary>Gets the original local commit receipt.</summary>
        internal LocalCommitResult Receipt { get; }

        /// <summary>Gets or sets the durable retry state.</summary>
        internal RetryState? RetryState { get; set; }

        /// <summary>Gets the committed snapshot mutation.</summary>
        internal SnapshotMutation SnapshotMutation { get; }

        /// <summary>Gets or sets the current operation status.</summary>
        internal SyncOperationStatus Status { get; set; }

        /// <summary>Gets or sets the terminal timestamp.</summary>
        internal DateTimeOffset? TerminalAtUtc { get; set; }
    }

    /// <summary>Stores current state for one stream.</summary>
    private sealed class StreamRecord
    {
        /// <summary>The first client sequence.</summary>
        private const long FirstClientSequence = 1;

        /// <summary>Initializes a new instance of the <see cref="StreamRecord"/> class.</summary>
        /// <param name="subscriptionId">The subscription identifier.</param>
        internal StreamRecord(SubscriptionId subscriptionId) => SubscriptionId = subscriptionId;

        /// <summary>Gets or sets the next client sequence.</summary>
        internal long NextClientSequence { get; set; } = FirstClientSequence;

        /// <summary>Gets or sets the server cursor.</summary>
        internal string? ServerCursor { get; set; }

        /// <summary>Gets or sets the current local snapshot.</summary>
        internal LocalSnapshot? Snapshot { get; set; }

        /// <summary>Gets the subscription identifier.</summary>
        internal SubscriptionId SubscriptionId { get; }
    }
}
