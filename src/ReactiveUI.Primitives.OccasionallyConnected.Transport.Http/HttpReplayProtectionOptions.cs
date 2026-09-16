// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Configures HTTP replay protection for server-side endpoint primitives.</summary>
/// <remarks>
/// Replay retention is bounded by <see cref="MaximumRetainedBytes"/> after admission. Canonical request construction is
/// limited per request by <see cref="MaximumCanonicalRequestBytes"/> and is expected to be composed with the endpoint's
/// concurrent request gate.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("Enabled = {Enabled}, Entries = {MaximumEntries,nq}")]
public sealed record HttpReplayProtectionOptions
{
    /// <summary>The byte count in one kibibyte.</summary>
    private const int BytesPerKilobyte = 1024;

    /// <summary>The byte count in one mebibyte.</summary>
    private const int BytesPerMebibyte = BytesPerKilobyte * BytesPerKilobyte;

    /// <summary>The default retained nonce entry count.</summary>
    private const int DefaultMaximumEntries = 8192;

    /// <summary>The default active duplicate waiter count.</summary>
    private const int DefaultMaximumActiveReplayWaiters = 1024;

    /// <summary>The default active replay session count.</summary>
    private const int DefaultMaximumReplaySessions = 4096;

    /// <summary>The default total retained replay byte budget.</summary>
    private const long DefaultMaximumRetainedBytes = 8L * BytesPerMebibyte;

    /// <summary>The default canonical request byte limit.</summary>
    private const int DefaultMaximumCanonicalRequestBytes = 2 * BytesPerMebibyte;

    /// <summary>The default cached response byte limit.</summary>
    private const int DefaultMaximumCachedResponseBytes = BytesPerMebibyte;

    /// <summary>The default freshness window in minutes.</summary>
    private const int DefaultFreshnessWindowMinutes = 5;

    /// <summary>The default nonce retention window in minutes.</summary>
    private const int DefaultNonceRetentionMinutes = 10;

    /// <summary>The default replay session retention window in minutes.</summary>
    private const int DefaultReplaySessionRetentionMinutes = 30;

    /// <summary>The multiplier used for the future-skew retention invariant.</summary>
    private const int FutureSkewRetentionMultiplier = 2;

    /// <summary>Gets whether HTTP replay protection is enabled.</summary>
    public bool Enabled { get; init; } = true;

    /// <summary>Gets the clock used for freshness admission and replay retention.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    /// <summary>Gets the maximum retained nonce cache entries.</summary>
    public int MaximumEntries { get; init; } = DefaultMaximumEntries;

    /// <summary>Gets the maximum active byte-identical duplicate waiters.</summary>
    public int MaximumActiveReplayWaiters { get; init; } = DefaultMaximumActiveReplayWaiters;

    /// <summary>Gets the maximum active replay sessions.</summary>
    public int MaximumReplaySessions { get; init; } = DefaultMaximumReplaySessions;

    /// <summary>Gets the shared retained byte budget for sessions and nonce cache entries after admission.</summary>
    public long MaximumRetainedBytes { get; init; } = DefaultMaximumRetainedBytes;

    /// <summary>Gets the maximum canonical request bytes accepted for one admission attempt.</summary>
    public int MaximumCanonicalRequestBytes { get; init; } = DefaultMaximumCanonicalRequestBytes;

    /// <summary>Gets the maximum response bytes retained for byte-identical replay.</summary>
    public int MaximumCachedResponseBytes { get; init; } = DefaultMaximumCachedResponseBytes;

    /// <summary>Gets the inclusive timestamp skew accepted in either direction.</summary>
    public TimeSpan FreshnessWindow { get; init; } = TimeSpan.FromMinutes(DefaultFreshnessWindowMinutes);

    /// <summary>Gets the minimum interval for retaining admitted nonces.</summary>
    public TimeSpan NonceRetention { get; init; } = TimeSpan.FromMinutes(DefaultNonceRetentionMinutes);

    /// <summary>Gets the minimum interval for retaining endpoint-issued replay sessions.</summary>
    public TimeSpan ReplaySessionRetention { get; init; } = TimeSpan.FromMinutes(DefaultReplaySessionRetentionMinutes);

    /// <summary>Validates this option set.</summary>
    /// <exception cref="ArgumentNullException"><see cref="TimeProvider"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A configured bound or time window is invalid.</exception>
    public void Validate()
    {
        ArgumentExceptionHelper.ThrowIfNull(TimeProvider);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumEntries);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumActiveReplayWaiters);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumReplaySessions);
        ThrowIfNegativeOrZero(MaximumRetainedBytes, nameof(MaximumRetainedBytes));
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumCanonicalRequestBytes);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumCachedResponseBytes);
        ValidateWindow(FreshnessWindow, nameof(FreshnessWindow));
        ValidateWindow(NonceRetention, nameof(NonceRetention));
        ValidateWindow(ReplaySessionRetention, nameof(ReplaySessionRetention));
        ValidateNonceRetentionCoversFreshness();
        var doubledFreshness = GetDoubleFreshnessWindow();
        var minimumSessionRetention = NonceRetention >= doubledFreshness ? NonceRetention : doubledFreshness;
        ValidateReplaySessionRetentionCoversMinimum(minimumSessionRetention);
    }

    /// <summary>Validates a positive long option.</summary>
    /// <param name="value">The option value.</param>
    /// <param name="parameterName">The option name.</param>
    /// <exception cref="ArgumentOutOfRangeException">The value is not positive.</exception>
    private static void ThrowIfNegativeOrZero(long value, string parameterName)
    {
        if (value > 0)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(parameterName, value, "HTTP replay limits must be positive.");
    }

    /// <summary>Validates a positive finite time window.</summary>
    /// <param name="value">The time window.</param>
    /// <param name="parameterName">The option name.</param>
    /// <exception cref="ArgumentOutOfRangeException">The time window is not positive and finite.</exception>
    private static void ValidateWindow(TimeSpan value, string parameterName)
    {
        if (value > TimeSpan.Zero && value < TimeSpan.MaxValue)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(parameterName, value, "HTTP replay time windows must be positive and finite.");
    }

    /// <summary>Gets two freshness windows after proving the calculation cannot overflow.</summary>
    /// <returns>The doubled freshness window.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The freshness window is too large to double safely.</exception>
    private TimeSpan GetDoubleFreshnessWindow()
    {
        if (FreshnessWindow.Ticks > TimeSpan.MaxValue.Ticks / FutureSkewRetentionMultiplier)
        {
            throw new ArgumentOutOfRangeException(
                nameof(FreshnessWindow),
                FreshnessWindow,
                "Freshness window must be small enough to derive replay session retention bounds.");
        }

        return TimeSpan.FromTicks(FreshnessWindow.Ticks * FutureSkewRetentionMultiplier);
    }

    /// <summary>Validates nonce retention against timestamp freshness.</summary>
    /// <exception cref="ArgumentOutOfRangeException">Nonce retention does not cover freshness.</exception>
    private void ValidateNonceRetentionCoversFreshness()
    {
        if (NonceRetention >= FreshnessWindow)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(
            nameof(NonceRetention),
            NonceRetention,
            "Nonce retention must cover the freshness window.");
    }

    /// <summary>Validates replay session retention against the derived minimum.</summary>
    /// <param name="minimumSessionRetention">The minimum replay session retention.</param>
    /// <exception cref="ArgumentOutOfRangeException">Replay session retention does not cover the derived minimum.</exception>
    private void ValidateReplaySessionRetentionCoversMinimum(TimeSpan minimumSessionRetention)
    {
        if (ReplaySessionRetention >= minimumSessionRetention)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(
            nameof(ReplaySessionRetention),
            ReplaySessionRetention,
            "Replay session retention must cover nonce retention and two freshness windows.");
    }
}
