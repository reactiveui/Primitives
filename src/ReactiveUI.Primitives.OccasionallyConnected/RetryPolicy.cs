// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Computes bounded decorrelated-jitter retry decisions for durable operations.</summary>
[System.Diagnostics.DebuggerDisplay("MinimumDelay = {_options.MinimumDelay}, MaximumDelay = {_options.MaximumDelay}")]
public sealed class RetryPolicy : IRetryPolicy
{
    /// <summary>The multiplier used by decorrelated jitter.</summary>
    private const int DecorrelatedJitterMultiplier = 3;

    /// <summary>The default retry random source.</summary>
    private static readonly IRetryRandomSource DefaultRandomSource = new SharedRetryRandomSource();

    /// <summary>The retry options.</summary>
    private readonly RetryOptions _options;

    /// <summary>The retry random source.</summary>
    private readonly IRetryRandomSource _randomSource;

    /// <summary>The time provider.</summary>
    private readonly TimeProvider _timeProvider;

    /// <summary>Initializes a new instance of the <see cref="RetryPolicy"/> class.</summary>
    public RetryPolicy()
        : this(RetryOptions.Default, TimeProvider.System, DefaultRandomSource)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="RetryPolicy"/> class.</summary>
    /// <param name="options">The retry options.</param>
    public RetryPolicy(RetryOptions options)
        : this(options, TimeProvider.System, DefaultRandomSource)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="RetryPolicy"/> class.</summary>
    /// <param name="options">The retry options.</param>
    /// <param name="timeProvider">The time provider used to compute due times.</param>
    /// <param name="randomSource">The deterministic random source used for jitter.</param>
    public RetryPolicy(
        RetryOptions options,
        TimeProvider timeProvider,
        IRetryRandomSource randomSource)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        ArgumentExceptionHelper.ThrowIfNull(timeProvider);
        ArgumentExceptionHelper.ThrowIfNull(randomSource);

        options.Validate();
        _options = options;
        _timeProvider = timeProvider;
        _randomSource = randomSource;
    }

    /// <inheritdoc/>
    public RetryDecision GetDecision(RetryFailure failure, RetryState state)
    {
        ArgumentExceptionHelper.ThrowIfNull(failure);
        ArgumentExceptionHelper.ThrowIfNull(state);
        if (state.TransientAttemptCount < 0 || state.PreviousDelay < TimeSpan.Zero
            || state.AuthenticationState is not (RetryAuthenticationState.None or RetryAuthenticationState.RenewalRetryUsed))
        {
            throw new ArgumentException("Persisted retry state contains an invalid count, delay, or authentication state.", nameof(state));
        }

        if (failure.RetryAfter < TimeSpan.Zero)
        {
            throw new ArgumentException("The server retry delay cannot be negative.", nameof(failure));
        }

        return failure.Kind switch
        {
            RetryFailureKind.Transient or RetryFailureKind.AmbiguousTransportOutcome => GetTransientDecision(failure, state),
            RetryFailureKind.Authentication => GetAuthenticationDecision(failure, state),
            _ => Stop(RetryStopReason.PermanentFailure, state),
        };
    }

    /// <summary>Creates a stop decision.</summary>
    /// <param name="reason">The stop reason.</param>
    /// <param name="state">The state to preserve.</param>
    /// <returns>The stop decision.</returns>
    private static RetryDecision Stop(RetryStopReason reason, RetryState state) =>
        new(RetryDecisionKind.Stop, reason, null, null, state);

    /// <summary>Computes a transient retry decision.</summary>
    /// <param name="failure">The transient failure.</param>
    /// <param name="state">The current retry state.</param>
    /// <returns>The retry decision.</returns>
    private RetryDecision GetTransientDecision(RetryFailure failure, RetryState state)
    {
        var nowUtc = _timeProvider.GetUtcNow();
        if (nowUtc - state.StartedUtc >= _options.MaximumRetryAge)
        {
            return Stop(RetryStopReason.RetryAgeExhausted, state);
        }

        if (state.TransientAttemptCount >= _options.MaximumRetryAttempts)
        {
            return Stop(RetryStopReason.AttemptsExhausted, state);
        }

        var delay = GetDecorrelatedDelay(state.PreviousDelay);
        if (failure.RetryAfter is { } retryAfter && retryAfter > delay)
        {
            delay = retryAfter;
        }

        var elapsed = nowUtc > state.StartedUtc ? nowUtc - state.StartedUtc : TimeSpan.Zero;
        if (delay >= _options.MaximumRetryAge - elapsed || delay > DateTimeOffset.MaxValue - nowUtc)
        {
            return Stop(RetryStopReason.RetryAgeExhausted, state);
        }

        var dueUtc = nowUtc.Add(delay);
        var nextState = state with
        {
            DueUtc = dueUtc,
            PreviousDelay = delay,
            TransientAttemptCount = state.TransientAttemptCount + 1,
        };

        return new(RetryDecisionKind.Retry, RetryStopReason.None, delay, dueUtc, nextState);
    }

    /// <summary>Computes the authentication retry decision.</summary>
    /// <param name="failure">The authentication failure.</param>
    /// <param name="state">The current retry state.</param>
    /// <returns>The retry decision.</returns>
    private RetryDecision GetAuthenticationDecision(RetryFailure failure, RetryState state)
    {
        var nowUtc = _timeProvider.GetUtcNow();
        if (nowUtc - state.StartedUtc >= _options.MaximumRetryAge)
        {
            return Stop(RetryStopReason.RetryAgeExhausted, state);
        }

        var credentialChanged = !StringComparer.Ordinal.Equals(state.CredentialsVersion, failure.CredentialsVersion);
        if (string.IsNullOrEmpty(failure.CredentialsVersion)
            || (state.AuthenticationState == RetryAuthenticationState.RenewalRetryUsed && !credentialChanged))
        {
            return Stop(RetryStopReason.PermanentUntilCredentialsChange, state);
        }

        var nextState = state with
        {
            AuthenticationState = RetryAuthenticationState.RenewalRetryUsed,
            CredentialsVersion = failure.CredentialsVersion,
            DueUtc = nowUtc,
        };

        return new(RetryDecisionKind.Retry, RetryStopReason.None, TimeSpan.Zero, nowUtc, nextState);
    }

    /// <summary>Gets the next decorrelated jitter delay.</summary>
    /// <param name="previousDelay">The previous delay, if any.</param>
    /// <returns>The computed delay.</returns>
    /// <exception cref="InvalidOperationException">The random source returns a value outside the inclusive range from zero through one.</exception>
    private TimeSpan GetDecorrelatedDelay(TimeSpan? previousDelay)
    {
        var minimumTicks = _options.MinimumDelay.Ticks;
        var previousTicks = Math.Max(minimumTicks, previousDelay?.Ticks ?? minimumTicks);
        var maximumTicks = _options.MaximumDelay.Ticks;
        var maximumJitterTicks = previousTicks > maximumTicks / DecorrelatedJitterMultiplier
            ? maximumTicks
            : previousTicks * DecorrelatedJitterMultiplier;
        var jitterRangeTicks = maximumJitterTicks - minimumTicks;
        var sample = _randomSource.NextDouble();
        if (sample < 0 || sample > 1 || double.IsNaN(sample))
        {
            throw new InvalidOperationException("Retry random source must return a value from 0 through 1.");
        }

        var jitterTicks = (long)decimal.Round(jitterRangeTicks * (decimal)sample, 0, MidpointRounding.AwayFromZero);
        return TimeSpan.FromTicks(minimumTicks + jitterTicks);
    }

    /// <summary>Default random source used outside deterministic tests.</summary>
    private sealed class SharedRetryRandomSource : IRetryRandomSource
    {
        /// <summary>The byte count needed for a 32-bit random sample.</summary>
        private const int SampleByteCount = 4;

        /// <summary>The cryptographic random number generator used by the default source.</summary>
        private static readonly System.Security.Cryptography.RandomNumberGenerator Generator =
            System.Security.Cryptography.RandomNumberGenerator.Create();

        /// <inheritdoc/>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public double NextDouble()
        {
            var bytes = new byte[SampleByteCount];
            Generator.GetBytes(bytes);
            var sample = BitConverter.ToUInt32(bytes, 0);
            return (double)sample / uint.MaxValue;
        }
    }
}
