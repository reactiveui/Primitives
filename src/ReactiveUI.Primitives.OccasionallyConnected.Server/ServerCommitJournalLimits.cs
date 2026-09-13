// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Configures finite storage limits for an owned server commit journal.</summary>
[System.Diagnostics.DebuggerDisplay("Streams={MaximumStreams}, Entries={MaximumLedgerEntries}, Events={MaximumEvents}")]
public sealed record ServerCommitJournalLimits
{
    /// <summary>The default maximum retained stream count.</summary>
    private const int DefaultMaximumStreams = 1_024;

    /// <summary>The default maximum retained terminal operation count.</summary>
    private const int DefaultMaximumLedgerEntries = 8_192;

    /// <summary>The default maximum retained event count.</summary>
    private const int DefaultMaximumEvents = 65_536;

    /// <summary>The default maximum captured operations per read or commit plan.</summary>
    private const int DefaultMaximumOperationCaptureCount = 512;

    /// <summary>The default maximum events in one terminal ledger entry.</summary>
    private const int DefaultMaximumEntryEventCount = 512;

    /// <summary>The default maximum retained subscription acknowledgement count.</summary>
    private const int DefaultMaximumSubscriptions = 1_024;

    /// <summary>The default maximum retained subscription offer count.</summary>
    private const int DefaultMaximumSubscriptionOffers = 8_192;

    /// <summary>The default operation retention duration in minutes.</summary>
    private const int DefaultOperationRetentionMinutes = 5;

    /// <summary>The default subscription binding retention duration in minutes.</summary>
    private const int DefaultSubscriptionRetentionMinutes = 30;

    /// <summary>The number of bytes in one mebibyte.</summary>
    private const long BytesPerMebibyte = 1_024L * 1_024L;

    /// <summary>The default retained logical byte budget.</summary>
    private const long DefaultMaximumLogicalBytes = 64L * BytesPerMebibyte;

    /// <summary>Gets the maximum retained stream count.</summary>
    public int MaximumStreams { get; init; } = DefaultMaximumStreams;

    /// <summary>Gets the maximum retained terminal operation count.</summary>
    public int MaximumLedgerEntries { get; init; } = DefaultMaximumLedgerEntries;

    /// <summary>Gets the maximum retained event count.</summary>
    public int MaximumEvents { get; init; } = DefaultMaximumEvents;

    /// <summary>Gets the maximum logical encoded bytes retained by the journal.</summary>
    public long MaximumLogicalBytes { get; init; } = DefaultMaximumLogicalBytes;

    /// <summary>Gets the maximum operations accepted by one read or commit plan.</summary>
    public int MaximumOperationCaptureCount { get; init; } = DefaultMaximumOperationCaptureCount;

    /// <summary>Gets the maximum events accepted inside one terminal ledger entry.</summary>
    public int MaximumEntryEventCount { get; init; } = DefaultMaximumEntryEventCount;

    /// <summary>Gets the maximum retained subscription acknowledgement rows.</summary>
    public int MaximumSubscriptions { get; init; } = DefaultMaximumSubscriptions;

    /// <summary>Gets the maximum retained subscription offer rows.</summary>
    public int MaximumSubscriptionOffers { get; init; } = DefaultMaximumSubscriptionOffers;

    /// <summary>Gets the finite terminal operation retention interval.</summary>
    public TimeSpan OperationRetention { get; init; } = TimeSpan.FromMinutes(DefaultOperationRetentionMinutes);

    /// <summary>Gets the finite subscription binding retention interval.</summary>
    public TimeSpan SubscriptionRetention { get; init; } = TimeSpan.FromMinutes(DefaultSubscriptionRetentionMinutes);
}
