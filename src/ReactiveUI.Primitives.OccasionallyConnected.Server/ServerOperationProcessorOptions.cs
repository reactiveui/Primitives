// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Defines finite bounds for the internal server operation processor.</summary>
internal sealed class ServerOperationProcessorOptions
{
    /// <summary>The default maximum operation count accepted in one batch.</summary>
    private const int DefaultMaximumBatchOperations = 512;

    /// <summary>The default maximum canonical operation byte count.</summary>
    private const int DefaultMaximumCanonicalOperationBytes = 64 * 1024;

    /// <summary>The default maximum logical byte count accepted in one batch.</summary>
    private const long DefaultMaximumBatchLogicalBytes = 1024 * 1024;

    /// <summary>The default maximum prepared conflict count.</summary>
    private const int DefaultMaximumPreparedConflicts = 512;

    /// <summary>The default maximum prepared event count.</summary>
    private const int DefaultMaximumPreparedEvents = 512;

    /// <summary>The default maximum prepared logical byte count.</summary>
    private const long DefaultMaximumPreparedLogicalBytes = 1024 * 1024;

    /// <summary>The default maximum active request count.</summary>
    private const int DefaultMaximumActiveRequests = 128;

    /// <summary>The default maximum compare-and-swap attempts for one operation.</summary>
    private const int DefaultMaximumCommitAttempts = 4;

    /// <summary>Gets the maximum operation count accepted in one batch.</summary>
    internal int MaximumBatchOperations { get; init; } = DefaultMaximumBatchOperations;

    /// <summary>Gets the maximum canonical operation byte count.</summary>
    internal int MaximumCanonicalOperationBytes { get; init; } = DefaultMaximumCanonicalOperationBytes;

    /// <summary>Gets the maximum logical byte count accepted in one batch.</summary>
    internal long MaximumBatchLogicalBytes { get; init; } = DefaultMaximumBatchLogicalBytes;

    /// <summary>Gets the maximum prepared conflict count accepted from a handler.</summary>
    internal int MaximumPreparedConflicts { get; init; } = DefaultMaximumPreparedConflicts;

    /// <summary>Gets the maximum prepared event count accepted from a handler.</summary>
    internal int MaximumPreparedEvents { get; init; } = DefaultMaximumPreparedEvents;

    /// <summary>Gets the maximum prepared logical byte count accepted from a handler.</summary>
    internal long MaximumPreparedLogicalBytes { get; init; } = DefaultMaximumPreparedLogicalBytes;

    /// <summary>Gets the maximum active request count accepted by the processor.</summary>
    internal int MaximumActiveRequests { get; init; } = DefaultMaximumActiveRequests;

    /// <summary>Gets the maximum journal compare-and-swap attempts for one operation.</summary>
    internal int MaximumCommitAttempts { get; init; } = DefaultMaximumCommitAttempts;

    /// <summary>Gets the optional retry delay returned when bounded admission cannot complete.</summary>
    internal TimeSpan? RetryAfter { get; init; }

    /// <summary>Gets the server clock used for prepared write stamps.</summary>
    internal TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    /// <summary>Validates the processor bounds.</summary>
    /// <exception cref="ArgumentOutOfRangeException">A configured bound is invalid.</exception>
    /// <exception cref="ArgumentNullException">The time provider is missing.</exception>
    internal void Validate()
    {
        ArgumentExceptionHelper.ThrowIfNull(TimeProvider);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumBatchOperations);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumCanonicalOperationBytes);
        ThrowIfNegativeOrZero(MaximumBatchLogicalBytes, nameof(MaximumBatchLogicalBytes));
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumPreparedConflicts);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumPreparedEvents);
        ThrowIfNegativeOrZero(MaximumPreparedLogicalBytes, nameof(MaximumPreparedLogicalBytes));
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumActiveRequests);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumCommitAttempts);
        if (RetryAfter.GetValueOrDefault() >= TimeSpan.Zero)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(RetryAfter), RetryAfter, "Retry delay cannot be negative.");
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
