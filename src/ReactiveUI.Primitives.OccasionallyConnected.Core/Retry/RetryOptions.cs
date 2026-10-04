// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures bounded deterministic retry behavior.</summary>
[System.Diagnostics.DebuggerDisplay("MinimumDelay = {MinimumDelay}, MaximumDelay = {MaximumDelay}, MaximumRetryAttempts = {MaximumRetryAttempts}")]
public sealed record RetryOptions
{
    /// <summary>The default maximum transient retry count.</summary>
    private const int DefaultMaximumRetryAttempts = 8;

    /// <summary>The default minimum retry delay in milliseconds.</summary>
    private const int DefaultMinimumDelayMilliseconds = 500;

    /// <summary>The default maximum retry delay in seconds.</summary>
    private const int DefaultMaximumDelaySeconds = 30;

    /// <summary>The default maximum retry age in minutes.</summary>
    private const int DefaultMaximumRetryAgeMinutes = 15;

    /// <summary>Gets the default retry options: decorrelated jitter from 500 ms to 30 s, 8 retries, and 15 minutes of retry age.</summary>
    public static RetryOptions Default { get; } = new();

    /// <summary>Gets the minimum decorrelated jitter delay.</summary>
    public TimeSpan MinimumDelay { get; init; } = TimeSpan.FromMilliseconds(DefaultMinimumDelayMilliseconds);

    /// <summary>Gets the maximum decorrelated jitter delay before applying server retry lower bounds.</summary>
    public TimeSpan MaximumDelay { get; init; } = TimeSpan.FromSeconds(DefaultMaximumDelaySeconds);

    /// <summary>Gets the maximum number of transient retry attempts.</summary>
    public int MaximumRetryAttempts { get; init; } = DefaultMaximumRetryAttempts;

    /// <summary>Gets the maximum age of a retryable durable operation.</summary>
    public TimeSpan MaximumRetryAge { get; init; } = TimeSpan.FromMinutes(DefaultMaximumRetryAgeMinutes);

    /// <summary>Validates the retry options.</summary>
    /// <exception cref="ArgumentOutOfRangeException">An option is outside its allowed range.</exception>
    public void Validate()
    {
        var exception = CreateValidationException();

        if (exception is null)
        {
            return;
        }

        throw exception;
    }

    /// <summary>Creates a validation exception for the first invalid option.</summary>
    /// <returns>The validation exception when an option is invalid; otherwise null.</returns>
    private ArgumentOutOfRangeException? CreateValidationException()
    {
        if (MinimumDelay <= TimeSpan.Zero)
        {
            return new(nameof(MinimumDelay), MinimumDelay, "Minimum retry delay must be positive.");
        }

        if (MaximumDelay < MinimumDelay)
        {
            return new(nameof(MaximumDelay), MaximumDelay, "Maximum retry delay cannot be less than the minimum delay.");
        }

        if (MaximumRetryAttempts < 0)
        {
            return new(nameof(MaximumRetryAttempts), MaximumRetryAttempts, "Maximum retry attempts cannot be negative.");
        }

        return MaximumRetryAge <= TimeSpan.Zero
            ? new(nameof(MaximumRetryAge), MaximumRetryAge, "Maximum retry age must be positive.")
            : null;
    }
}
