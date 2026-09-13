// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures bounded prepared upload attempt behavior.</summary>
internal sealed record PreparedUploadAttemptOptions
{
    /// <summary>The default maximum operation count.</summary>
    private const int DefaultMaximumOperations = 100;

    /// <summary>The default maximum encoded byte count.</summary>
    private const long DefaultMaximumEncodedSizeBytes = 1024L * 1024L;

    /// <summary>The default lease renewal interval in seconds.</summary>
    private const int DefaultLeaseRenewalIntervalSeconds = 30;

    /// <summary>The default lease renewal duration in minutes.</summary>
    private const int DefaultLeaseRenewalDurationMinutes = 1;

    /// <summary>Gets the maximum operations accepted in one prepared upload attempt.</summary>
    public int MaximumOperations { get; init; } = DefaultMaximumOperations;

    /// <summary>Gets the maximum encoded bytes accepted in one prepared upload attempt.</summary>
    public long MaximumEncodedSizeBytes { get; init; } = DefaultMaximumEncodedSizeBytes;

    /// <summary>Gets the lease renewal interval.</summary>
    public TimeSpan LeaseRenewalInterval { get; init; } = TimeSpan.FromSeconds(DefaultLeaseRenewalIntervalSeconds);

    /// <summary>Gets the target lease lifetime maintained by renewal.</summary>
    public TimeSpan LeaseRenewalDuration { get; init; } = TimeSpan.FromMinutes(DefaultLeaseRenewalDurationMinutes);

    /// <summary>Gets the clock driving lease renewal.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    /// <summary>Validates the configured bounds.</summary>
    /// <exception cref="ArgumentNullException"><see cref="TimeProvider"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A configured bound is not finite and positive.</exception>
    internal void Validate()
    {
        ArgumentExceptionHelper.ThrowIfNull(TimeProvider);
        ThrowIfNotPositive(MaximumOperations, nameof(MaximumOperations));
        ThrowIfNotPositive(MaximumEncodedSizeBytes, nameof(MaximumEncodedSizeBytes));
        ThrowIfNotFinitePositive(LeaseRenewalInterval, nameof(LeaseRenewalInterval));
        ThrowIfNotFinitePositive(LeaseRenewalDuration, nameof(LeaseRenewalDuration));
        _ = LeaseRenewalInterval >= LeaseRenewalDuration
            ? throw new ArgumentOutOfRangeException(nameof(LeaseRenewalInterval), LeaseRenewalInterval, "The renewal interval must be shorter than the renewal duration.")
            : true;
    }

    /// <summary>Validates an integer bound.</summary>
    /// <param name="value">The configured value.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="value"/> is not positive.</exception>
    private static void ThrowIfNotPositive(int value, string parameterName) =>
        _ = value <= 0 ? throw new ArgumentOutOfRangeException(parameterName, value, "The configured value must be positive.") : true;

    /// <summary>Validates a long bound.</summary>
    /// <param name="value">The configured value.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="value"/> is not positive.</exception>
    private static void ThrowIfNotPositive(long value, string parameterName) =>
        _ = value <= 0 ? throw new ArgumentOutOfRangeException(parameterName, value, "The configured value must be positive.") : true;

    /// <summary>Validates a renewal interval.</summary>
    /// <param name="value">The configured value.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="value"/> is not finite and positive.</exception>
    private static void ThrowIfNotFinitePositive(TimeSpan value, string parameterName) =>
        _ = value <= TimeSpan.Zero || value == Timeout.InfiniteTimeSpan || value == TimeSpan.MaxValue
            ? throw new ArgumentOutOfRangeException(parameterName, value, "The configured value must be finite and positive.")
            : true;
}
