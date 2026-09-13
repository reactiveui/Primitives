// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Configures the concrete authorized server stream hub facade.</summary>
[System.Diagnostics.DebuggerDisplay("Calls={MaximumActiveCalls}, Subscriptions={MaximumActiveSubscriptions}")]
public sealed record ServerStreamHubOptions
{
    /// <summary>The default maximum active call count.</summary>
    private const int DefaultMaximumActiveCalls = 128;

    /// <summary>The default maximum active subscription count.</summary>
    private const int DefaultMaximumActiveSubscriptions = 128;

    /// <summary>The default maximum operations per publish batch.</summary>
    private const int DefaultMaximumBatchOperations = 512;

    /// <summary>The default maximum operation groups per receive page.</summary>
    private const int DefaultMaximumReceiveGroups = 64;

    /// <summary>The default maximum events per receive page.</summary>
    private const int DefaultMaximumReceiveEvents = 512;

    /// <summary>The default empty-poll delay in seconds.</summary>
    private const int DefaultEmptyPollDelaySeconds = 1;

    /// <summary>The number of bytes in one mebibyte.</summary>
    private const long BytesPerMebibyte = 1_024L * 1_024L;

    /// <summary>The default maximum logical bytes per publish batch.</summary>
    private const long DefaultMaximumBatchLogicalBytes = BytesPerMebibyte;

    /// <summary>The default maximum logical bytes per receive page.</summary>
    private const long DefaultMaximumReceiveLogicalBytes = BytesPerMebibyte;

    /// <summary>Gets the conflict and domain handling configuration.</summary>
    public required ServerConflictHandlerOptions ConflictHandler { get; init; }

    /// <summary>Gets the required authorization policy.</summary>
    public required IServerStreamAuthorizationPolicy AuthorizationPolicy { get; init; }

    /// <summary>Gets the server clock used for commits, retention and subscription polling.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    /// <summary>Gets the maximum active public calls admitted by the hub.</summary>
    public int MaximumActiveCalls { get; init; } = DefaultMaximumActiveCalls;

    /// <summary>Gets the maximum active receive enumerations admitted by the hub.</summary>
    public int MaximumActiveSubscriptions { get; init; } = DefaultMaximumActiveSubscriptions;

    /// <summary>Gets the maximum operation count accepted in one batch.</summary>
    public int MaximumBatchOperations { get; init; } = DefaultMaximumBatchOperations;

    /// <summary>Gets the maximum logical byte count accepted in one batch.</summary>
    public long MaximumBatchLogicalBytes { get; init; } = DefaultMaximumBatchLogicalBytes;

    /// <summary>Gets the maximum complete operation groups returned in one receive page.</summary>
    public int MaximumReceiveGroups { get; init; } = DefaultMaximumReceiveGroups;

    /// <summary>Gets the maximum events returned in one receive page.</summary>
    public int MaximumReceiveEvents { get; init; } = DefaultMaximumReceiveEvents;

    /// <summary>Gets the maximum logical byte count returned in one receive page.</summary>
    public long MaximumReceiveLogicalBytes { get; init; } = DefaultMaximumReceiveLogicalBytes;

    /// <summary>Gets the delay between empty receive polls when no publish wakeup occurs.</summary>
    public TimeSpan EmptyPollDelay { get; init; } = TimeSpan.FromSeconds(DefaultEmptyPollDelaySeconds);

    /// <summary>Gets the owned journal limits.</summary>
    public ServerCommitJournalLimits JournalLimits { get; init; } = new();
}
