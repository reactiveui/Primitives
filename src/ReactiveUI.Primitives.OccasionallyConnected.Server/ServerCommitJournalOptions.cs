// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Defines finite process-local bounds for <see cref="InMemoryServerCommitJournal"/>.</summary>
/// <remarks>
/// Counts and logical bytes bound admitted retained rows plus each read or commit capture. They are not a global
/// reservation system for every concurrent caller preparing bounded plans before admission.
/// </remarks>
internal sealed class ServerCommitJournalOptions
{
    /// <summary>The default retained stream count.</summary>
    private const int DefaultMaximumStreams = 1024;

    /// <summary>The default retained terminal operation count.</summary>
    private const int DefaultMaximumLedgerEntries = 8192;

    /// <summary>The default retained event count.</summary>
    private const int DefaultMaximumEvents = 65_536;

    /// <summary>The number of bytes in a mebibyte.</summary>
    private const long BytesPerMebibyte = 1024L * 1024L;

    /// <summary>The default logical byte retention in mebibytes.</summary>
    private const long DefaultMaximumLogicalMebibytes = 64;

    /// <summary>The default captured operation count.</summary>
    private const int DefaultMaximumOperationCaptureCount = 512;

    /// <summary>The default captured event count per terminal entry.</summary>
    private const int DefaultMaximumEntryEventCount = 512;

    /// <summary>The default retained subscription count.</summary>
    private const int DefaultMaximumSubscriptions = 1024;

    /// <summary>The default retained offered cursor count.</summary>
    private const int DefaultMaximumSubscriptionOffers = 8192;

    /// <summary>The default terminal operation retention in minutes.</summary>
    private const int DefaultOperationRetentionMinutes = 5;

    /// <summary>The default subscription binding retention in minutes.</summary>
    private const int DefaultSubscriptionRetentionMinutes = 30;

    /// <summary>Gets the maximum retained stream count.</summary>
    internal int MaximumStreams { get; init; } = DefaultMaximumStreams;

    /// <summary>Gets the maximum retained terminal operation count.</summary>
    internal int MaximumLedgerEntries { get; init; } = DefaultMaximumLedgerEntries;

    /// <summary>Gets the maximum retained event count.</summary>
    internal int MaximumEvents { get; init; } = DefaultMaximumEvents;

    /// <summary>Gets the maximum logical encoded bytes retained by the journal.</summary>
    internal long MaximumLogicalBytes { get; init; } = DefaultMaximumLogicalMebibytes * BytesPerMebibyte;

    /// <summary>Gets the maximum operations accepted by one read or one commit plan.</summary>
    internal int MaximumOperationCaptureCount { get; init; } = DefaultMaximumOperationCaptureCount;

    /// <summary>Gets the maximum events accepted inside one terminal ledger entry.</summary>
    internal int MaximumEntryEventCount { get; init; } = DefaultMaximumEntryEventCount;

    /// <summary>Gets the maximum retained subscription acknowledgement rows.</summary>
    internal int MaximumSubscriptions { get; init; } = DefaultMaximumSubscriptions;

    /// <summary>Gets the maximum retained subscription offer rows.</summary>
    internal int MaximumSubscriptionOffers { get; init; } = DefaultMaximumSubscriptionOffers;

    /// <summary>Gets the finite terminal operation retention interval.</summary>
    internal TimeSpan OperationRetention { get; init; } = TimeSpan.FromMinutes(DefaultOperationRetentionMinutes);

    /// <summary>Gets the finite subscription binding retention interval.</summary>
    internal TimeSpan SubscriptionRetention { get; init; } = TimeSpan.FromMinutes(DefaultSubscriptionRetentionMinutes);

    /// <summary>Gets the clock used for commit and explicit compaction decisions.</summary>
    internal TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    /// <summary>Validates option bounds.</summary>
    /// <exception cref="ArgumentOutOfRangeException">A configured bound is invalid.</exception>
    /// <exception cref="ArgumentNullException">The time provider is missing.</exception>
    internal void Validate()
    {
        ArgumentExceptionHelper.ThrowIfNull(TimeProvider);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumStreams);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumLedgerEntries);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumEvents);
        ThrowIfNegativeOrZero(MaximumLogicalBytes, nameof(MaximumLogicalBytes));
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumOperationCaptureCount);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumEntryEventCount);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumSubscriptions);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumSubscriptionOffers);
        ThrowIfInvalidRetention(OperationRetention, nameof(OperationRetention), "Operation retention must be positive and finite.");
        ThrowIfInvalidRetention(SubscriptionRetention, nameof(SubscriptionRetention), "Subscription retention must be positive and finite.");
    }

    /// <summary>Throws when a retention interval is not positive and finite.</summary>
    /// <param name="retention">The retention interval.</param>
    /// <param name="parameterName">The source parameter name.</param>
    /// <param name="message">The exception message.</param>
    /// <exception cref="ArgumentOutOfRangeException">The retention interval is not positive or finite.</exception>
    private static void ThrowIfInvalidRetention(TimeSpan retention, string parameterName, string message)
    {
        if (retention > TimeSpan.Zero && retention != TimeSpan.MaxValue)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(parameterName, retention, message);
    }

    /// <summary>Throws when a long value is not positive.</summary>
    /// <param name="value">The value to inspect.</param>
    /// <param name="parameterName">The source parameter name.</param>
    /// <exception cref="ArgumentOutOfRangeException">The value is not positive.</exception>
    private static void ThrowIfNegativeOrZero(long value, string parameterName)
    {
        if (value > 0)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(parameterName, value, null);
    }
}
