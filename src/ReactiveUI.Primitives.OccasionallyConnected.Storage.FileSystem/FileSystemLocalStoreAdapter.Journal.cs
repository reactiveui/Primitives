// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem;

/// <summary>Incremental journal types owned by the adapter.</summary>
public sealed partial class FileSystemLocalStoreAdapter
{
    /// <summary>The version of incremental transaction records.</summary>
    internal const int IncrementalRecordVersion = 2;

    /// <summary>Contains changed state records for one atomic transaction.</summary>
    internal sealed class JournalDelta
    {
        /// <summary>Gets or sets the bound client identity.</summary>
        [JsonRequired]
        public string? ClientId { get; set; }

        /// <summary>Gets or sets the outbox configuration.</summary>
        public OutboxOptions? Outbox { get; set; }

        /// <summary>Gets changed streams.</summary>
        [JsonRequired]
        public Dictionary<string, StreamDelta> Streams { get; init; } = [];

        /// <summary>Gets changed leases.</summary>
        [JsonRequired]
        public Dictionary<Guid, LeaseState> Leases { get; init; } = [];

        /// <summary>Gets or sets removed leases.</summary>
        [JsonRequired]
        public Guid[] RemovedLeases { get; set; } = [];

        /// <summary>Gets or sets new remote event identities.</summary>
        [JsonRequired]
        public string[] Inbox { get; set; } = [];

        /// <summary>Gets or sets new authoritative operation inclusions.</summary>
        [JsonRequired]
        public Guid[] IncludedOperations { get; set; } = [];
    }

    /// <summary>Contains changed records and stream metadata, not the retained operation history.</summary>
    internal sealed class StreamDelta
    {
        /// <summary>Gets or sets the subscription identity.</summary>
        [JsonRequired]
        public SubscriptionId? SubscriptionId { get; set; }

        /// <summary>Gets or sets the next sequence.</summary>
        [JsonRequired]
        public long NextSequence { get; set; }

        /// <summary>Gets or sets the remote cursor.</summary>
        [JsonRequired]
        public string? Cursor { get; set; }

        /// <summary>Gets or sets whether this transaction changes the snapshot.</summary>
        [JsonRequired]
        public bool ReplaceSnapshot { get; set; }

        /// <summary>Gets or sets the replacement snapshot.</summary>
        [JsonRequired]
        public LocalSnapshot? Snapshot { get; set; }

        /// <summary>Gets changed operations.</summary>
        [JsonRequired]
        public Dictionary<Guid, OperationState> Operations { get; init; } = [];

        /// <summary>Gets or sets new dead letters.</summary>
        [JsonRequired]
        public DeadLetterRecord[] DeadLetters { get; set; } = [];
    }
}
