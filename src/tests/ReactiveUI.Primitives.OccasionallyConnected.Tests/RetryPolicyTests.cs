// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="RetryPolicy"/>.</summary>
public sealed class RetryPolicyTests
{
    /// <summary>The first credential version used by authentication retry tests.</summary>
    private const string FirstCredentialsVersion = "token-1";

    /// <summary>The second credential version used by authentication retry tests.</summary>
    private const string SecondCredentialsVersion = "token-2";

    /// <summary>The default minimum retry delay in milliseconds.</summary>
    private const int DefaultMinimumDelayMilliseconds = 500;

    /// <summary>The default maximum retry delay in seconds.</summary>
    private const int DefaultMaximumDelaySeconds = 30;

    /// <summary>The default maximum retry attempts.</summary>
    private const int DefaultMaximumRetryAttempts = 8;

    /// <summary>The default maximum retry age in minutes.</summary>
    private const int DefaultMaximumRetryAgeMinutes = 15;

    /// <summary>The second decorrelated jitter delay in milliseconds when the random source returns one.</summary>
    private const int SecondJitterDelayMilliseconds = 1500;

    /// <summary>The server retry lower bound in minutes.</summary>
    private const int ServerRetryAfterMinutes = 2;

    /// <summary>The single retry attempt used by exhaustion tests.</summary>
    private const int SingleRetryAttempt = 1;

    /// <summary>A configured retry age in minutes for constructor tests.</summary>
    private const int CustomMaximumRetryAgeMinutes = 1;

    /// <summary>An invalid jitter value below zero.</summary>
    private const double JitterBelowMinimum = -0.1;

    /// <summary>An invalid jitter value above one.</summary>
    private const double JitterAboveMaximum = 1.1;

    /// <summary>The later state delay in seconds.</summary>
    private const int PersistedDelaySeconds = 5;

    /// <summary>The divisor used to exercise persisted jitter multiplication overflow.</summary>
    private const int PersistedDelayDivisor = 2;

    /// <summary>The persisted transient attempt count used by state tests.</summary>
    private const int PersistedTransientAttemptCount = 2;

    /// <summary>The deterministic start timestamp used by retry tests.</summary>
    private static readonly DateTimeOffset StartUtc = new(2026, 9, 11, 12, 0, 0, TimeSpan.Zero);

    /// <summary>Verifies the first transient retry starts at the configured minimum delay.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TransientFailureUsesMinimumDelayForFirstRetry()
    {
        var timeProvider = new FixedTimeProvider(StartUtc);
        var policy = new RetryPolicy(RetryOptions.Default, timeProvider, new SequenceRetryRandomSource(0));
        var state = RetryState.Start(StartUtc);

        var decision = policy.GetDecision(RetryFailure.Transient(), state);

        await Assert.That(decision.Kind).IsEqualTo(RetryDecisionKind.Retry);
        await Assert.That(decision.Delay).IsEqualTo(TimeSpan.FromMilliseconds(DefaultMinimumDelayMilliseconds));
        await Assert.That(decision.DueUtc).IsEqualTo(StartUtc.AddMilliseconds(DefaultMinimumDelayMilliseconds));
        await Assert.That(decision.NextState.TransientAttemptCount).IsEqualTo(SingleRetryAttempt);
        await Assert.That(decision.NextState.PreviousDelay).IsEqualTo(TimeSpan.FromMilliseconds(DefaultMinimumDelayMilliseconds));
    }

    /// <summary>Verifies the default constructor uses system time and a random source to produce a bounded retry.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DefaultConstructorProducesBoundedTransientRetry()
    {
        var policy = new RetryPolicy();
        var state = RetryState.Start(TimeProvider.System.GetUtcNow());

        var decision = policy.GetDecision(RetryFailure.Transient(), state);

        await Assert.That(decision.Kind).IsEqualTo(RetryDecisionKind.Retry);
        await Assert.That(decision.Delay).IsNotNull();
        await Assert.That(decision.Delay!.Value).IsGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(DefaultMinimumDelayMilliseconds));
        await Assert.That(decision.Delay.Value).IsLessThanOrEqualTo(TimeSpan.FromMilliseconds(SecondJitterDelayMilliseconds));
        await Assert.That(decision.DueUtc).IsNotNull();
    }

    /// <summary>Verifies the options constructor uses the configured retry bounds.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OptionsConstructorUsesConfiguredRetryBounds()
    {
        var options = RetryOptions.Default with { MaximumRetryAge = TimeSpan.FromMinutes(CustomMaximumRetryAgeMinutes) };
        var policy = new RetryPolicy(options);
        var state = RetryState.Start(TimeProvider.System.GetUtcNow().AddMinutes(-DefaultMaximumRetryAgeMinutes));

        var decision = policy.GetDecision(RetryFailure.Transient(), state);

        await Assert.That(decision.Kind).IsEqualTo(RetryDecisionKind.Stop);
        await Assert.That(decision.StopReason).IsEqualTo(RetryStopReason.RetryAgeExhausted);
    }

    /// <summary>Verifies decorrelated jitter uses the previous retry delay as the next upper bound seed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TransientFailureUsesPreviousDelayForDecorrelatedJitter()
    {
        var timeProvider = new FixedTimeProvider(StartUtc);
        var options = new RetryOptions
        {
            MinimumDelay = TimeSpan.FromMilliseconds(DefaultMinimumDelayMilliseconds),
            MaximumDelay = TimeSpan.FromSeconds(DefaultMaximumDelaySeconds),
            MaximumRetryAttempts = DefaultMaximumRetryAttempts,
            MaximumRetryAge = TimeSpan.FromMinutes(DefaultMaximumRetryAgeMinutes),
        };

        var policy = new RetryPolicy(options, timeProvider, new SequenceRetryRandomSource(1));
        var state = RetryState.Start(StartUtc) with
        {
            PreviousDelay = TimeSpan.FromMilliseconds(DefaultMinimumDelayMilliseconds),
            TransientAttemptCount = SingleRetryAttempt,
        };

        var decision = policy.GetDecision(RetryFailure.Transient(), state);

        await Assert.That(decision.Delay).IsEqualTo(TimeSpan.FromMilliseconds(SecondJitterDelayMilliseconds));
        await Assert.That(decision.NextState.PreviousDelay).IsEqualTo(TimeSpan.FromMilliseconds(SecondJitterDelayMilliseconds));
    }

    /// <summary>Verifies server retry hints are a lower bound even above the configured maximum jitter delay.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TransientFailureUsesRetryAfterAsLowerBoundAboveMaximumDelay()
    {
        var timeProvider = new FixedTimeProvider(StartUtc);
        var policy = new RetryPolicy(RetryOptions.Default, timeProvider, new SequenceRetryRandomSource(0));
        var retryAfter = TimeSpan.FromMinutes(ServerRetryAfterMinutes);

        var decision = policy.GetDecision(RetryFailure.Transient(retryAfter), RetryState.Start(StartUtc));

        await Assert.That(decision.Kind).IsEqualTo(RetryDecisionKind.Retry);
        await Assert.That(decision.Delay).IsEqualTo(retryAfter);
        await Assert.That(decision.DueUtc).IsEqualTo(StartUtc.Add(retryAfter));
    }

    /// <summary>Verifies bounded attempts eventually stop retrying transient failures.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TransientFailureStopsWhenAttemptsAreExhausted()
    {
        var timeProvider = new FixedTimeProvider(StartUtc);
        var options = RetryOptions.Default with { MaximumRetryAttempts = SingleRetryAttempt };

        var policy = new RetryPolicy(options, timeProvider, new SequenceRetryRandomSource(0));
        var state = RetryState.Start(StartUtc) with { TransientAttemptCount = SingleRetryAttempt };

        var decision = policy.GetDecision(RetryFailure.Transient(), state);

        await Assert.That(decision.Kind).IsEqualTo(RetryDecisionKind.Stop);
        await Assert.That(decision.StopReason).IsEqualTo(RetryStopReason.AttemptsExhausted);
        await Assert.That(decision.NextState).IsEqualTo(state);
    }

    /// <summary>Verifies bounded retry age prevents restarts from creating a tight loop.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TransientFailureStopsWhenRetryAgeIsExhausted()
    {
        var nowUtc = StartUtc.AddMinutes(DefaultMaximumRetryAgeMinutes);
        var timeProvider = new FixedTimeProvider(nowUtc);
        var policy = new RetryPolicy(RetryOptions.Default, timeProvider, new SequenceRetryRandomSource(0));
        var state = RetryState.Start(StartUtc);

        var decision = policy.GetDecision(RetryFailure.Transient(), state);

        await Assert.That(decision.Kind).IsEqualTo(RetryDecisionKind.Stop);
        await Assert.That(decision.StopReason).IsEqualTo(RetryStopReason.RetryAgeExhausted);
    }

    /// <summary>Verifies authentication token renewal gets exactly one immediate retry per credential version.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AuthenticationFailureRetriesImmediatelyOnceAfterTokenRenewal()
    {
        var timeProvider = new FixedTimeProvider(StartUtc);
        var policy = new RetryPolicy(RetryOptions.Default, timeProvider, new SequenceRetryRandomSource(1));
        var state = RetryState.Start(StartUtc);

        var decision = policy.GetDecision(RetryFailure.AuthenticationTokenRenewed(FirstCredentialsVersion), state);

        await Assert.That(decision.Kind).IsEqualTo(RetryDecisionKind.Retry);
        await Assert.That(decision.Delay).IsEqualTo(TimeSpan.Zero);
        await Assert.That(decision.DueUtc).IsEqualTo(StartUtc);
        await Assert.That(decision.NextState.AuthenticationState).IsEqualTo(RetryAuthenticationState.RenewalRetryUsed);
        await Assert.That(decision.NextState.CredentialsVersion).IsEqualTo(FirstCredentialsVersion);
    }

    /// <summary>Verifies repeated authentication failure is permanent until credentials change.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AuthenticationFailureStopsUntilCredentialsChange()
    {
        var timeProvider = new FixedTimeProvider(StartUtc);
        var policy = new RetryPolicy(RetryOptions.Default, timeProvider, new SequenceRetryRandomSource(1));
        var state = RetryState.Start(StartUtc) with
        {
            AuthenticationState = RetryAuthenticationState.RenewalRetryUsed,
            CredentialsVersion = FirstCredentialsVersion,
        };

        var decision = policy.GetDecision(RetryFailure.AuthenticationTokenRenewed(FirstCredentialsVersion), state);

        await Assert.That(decision.Kind).IsEqualTo(RetryDecisionKind.Stop);
        await Assert.That(decision.StopReason).IsEqualTo(RetryStopReason.PermanentUntilCredentialsChange);
    }

    /// <summary>Verifies a new credential version allows the single authentication retry again.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AuthenticationFailureRetriesAgainWhenCredentialsChange()
    {
        var timeProvider = new FixedTimeProvider(StartUtc);
        var policy = new RetryPolicy(RetryOptions.Default, timeProvider, new SequenceRetryRandomSource(1));
        var state = RetryState.Start(StartUtc) with
        {
            AuthenticationState = RetryAuthenticationState.RenewalRetryUsed,
            CredentialsVersion = FirstCredentialsVersion,
        };

        var decision = policy.GetDecision(RetryFailure.AuthenticationTokenRenewed(SecondCredentialsVersion), state);

        await Assert.That(decision.Kind).IsEqualTo(RetryDecisionKind.Retry);
        await Assert.That(decision.Delay).IsEqualTo(TimeSpan.Zero);
        await Assert.That(decision.NextState.CredentialsVersion).IsEqualTo(SecondCredentialsVersion);
    }

    /// <summary>Verifies authorization and other deterministic rejections are not retried as transient failures.</summary>
    /// <param name="kind">The permanent failure kind.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments(RetryFailureKind.AuthorizationDenied)]
    [Arguments(RetryFailureKind.ValidationRejected)]
    [Arguments(RetryFailureKind.SchemaIncompatible)]
    [Arguments(RetryFailureKind.PayloadTooLarge)]
    [Arguments(RetryFailureKind.DeterministicConflictRejected)]
    public async Task PermanentFailureKindsStopWithoutRetry(RetryFailureKind kind)
    {
        var timeProvider = new FixedTimeProvider(StartUtc);
        var policy = new RetryPolicy(RetryOptions.Default, timeProvider, new SequenceRetryRandomSource(1));

        var decision = policy.GetDecision(new(kind), RetryState.Start(StartUtc));

        await Assert.That(decision.Kind).IsEqualTo(RetryDecisionKind.Stop);
        await Assert.That(decision.StopReason).IsEqualTo(RetryStopReason.PermanentFailure);
        await Assert.That(decision.Delay).IsNull();
        await Assert.That(decision.DueUtc).IsNull();
    }

    /// <summary>Verifies durable state carries the due time and previous delay needed for restart recovery.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RetryStateCarriesDueTimePreviousDelayAndAttempts()
    {
        var dueUtc = StartUtc.AddSeconds(PersistedDelaySeconds);

        var state = RetryState.Start(StartUtc) with
        {
            DueUtc = dueUtc,
            PreviousDelay = TimeSpan.FromSeconds(PersistedDelaySeconds),
            TransientAttemptCount = PersistedTransientAttemptCount,
            AuthenticationState = RetryAuthenticationState.RenewalRetryUsed,
            CredentialsVersion = FirstCredentialsVersion,
        };

        await Assert.That(state.StartedUtc).IsEqualTo(StartUtc);
        await Assert.That(state.DueUtc).IsEqualTo(dueUtc);
        await Assert.That(state.PreviousDelay).IsEqualTo(TimeSpan.FromSeconds(PersistedDelaySeconds));
        await Assert.That(state.TransientAttemptCount).IsEqualTo(PersistedTransientAttemptCount);
        await Assert.That(state.AuthenticationState).IsEqualTo(RetryAuthenticationState.RenewalRetryUsed);
        await Assert.That(state.CredentialsVersion).IsEqualTo(FirstCredentialsVersion);
    }

    /// <summary>Verifies an unrepresentable calendar deadline stops without overflowing or immediately retrying.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CalendarLimitStopsWithoutOverflow()
    {
        var policy = new RetryPolicy(RetryOptions.Default, new FixedTimeProvider(DateTimeOffset.MaxValue), new SequenceRetryRandomSource(0));
        var decision = policy.GetDecision(RetryFailure.Transient(), RetryState.Start(DateTimeOffset.MaxValue));
        await Assert.That(decision.Kind).IsEqualTo(RetryDecisionKind.Stop);
        await Assert.That(decision.DueUtc).IsNull();
    }

    /// <summary>Verifies a backwards clock does not grant additional retry age beyond the configured budget.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task BackwardsClockDoesNotExtendTheConfiguredAgeBudget()
    {
        var policy = new RetryPolicy(RetryOptions.Default, new FixedTimeProvider(StartUtc), new SequenceRetryRandomSource(0));
        var state = RetryState.Start(StartUtc.AddTicks(1));
        var decision = policy.GetDecision(RetryFailure.Transient(RetryOptions.Default.MaximumRetryAge), state);
        await Assert.That(decision.StopReason).IsEqualTo(RetryStopReason.RetryAgeExhausted);
    }

    /// <summary>Verifies the largest supported delay cannot overflow jitter arithmetic.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FullTimeSpanRangeStopsAtAgeLimitWithoutOverflow()
    {
        var options = new RetryOptions { MaximumDelay = TimeSpan.MaxValue, MaximumRetryAge = TimeSpan.MaxValue };
        var policy = new RetryPolicy(options, new FixedTimeProvider(StartUtc), new SequenceRetryRandomSource(1));
        var state = RetryState.Start(StartUtc) with { PreviousDelay = TimeSpan.MaxValue };
        var decision = policy.GetDecision(RetryFailure.Transient(), state);
        await Assert.That(decision.StopReason).IsEqualTo(RetryStopReason.RetryAgeExhausted);
    }

    /// <summary>Verifies invalid jitter samples are rejected.</summary>
    /// <param name="sample">The invalid random sample.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments(JitterBelowMinimum)]
    [Arguments(JitterAboveMaximum)]
    [Arguments(double.NaN)]
    public async Task WhenRandomSourceReturnsInvalidSample_ThenThrowsInvalidOperationException(double sample)
    {
        var timeProvider = new FixedTimeProvider(StartUtc);
        var policy = new RetryPolicy(RetryOptions.Default, timeProvider, new SequenceRetryRandomSource(sample));
        var state = RetryState.Start(StartUtc);

        var action = () => policy.GetDecision(RetryFailure.Transient(), state);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies an authentication error cannot retry until a renewed credential is supplied.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AuthenticationWithoutRenewedCredentialsStops()
    {
        var policy = new RetryPolicy(RetryOptions.Default, new FixedTimeProvider(StartUtc), new SequenceRetryRandomSource(0));
        var decision = policy.GetDecision(new(RetryFailureKind.Authentication), RetryState.Start(StartUtc));

        await Assert.That(decision.Kind).IsEqualTo(RetryDecisionKind.Stop);
        await Assert.That(decision.StopReason).IsEqualTo(RetryStopReason.PermanentUntilCredentialsChange);
    }

    /// <summary>Verifies an operation cannot be scheduled at or beyond its maximum retry age.</summary>
    /// <param name="extraTicks">Ticks beyond the configured age limit.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments(0L)]
    [Arguments(1L)]
    public async Task RetryAfterAtOrBeyondAgeLimitStops(long extraTicks)
    {
        var policy = new RetryPolicy(RetryOptions.Default, new FixedTimeProvider(StartUtc), new SequenceRetryRandomSource(0));
        var retryAfter = RetryOptions.Default.MaximumRetryAge + TimeSpan.FromTicks(extraTicks);
        var decision = policy.GetDecision(RetryFailure.Transient(retryAfter), RetryState.Start(StartUtc));

        await Assert.That(decision.Kind).IsEqualTo(RetryDecisionKind.Stop);
        await Assert.That(decision.StopReason).IsEqualTo(RetryStopReason.RetryAgeExhausted);
    }

    /// <summary>Verifies a large persisted server hint cannot overflow the next jitter calculation.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task LargePersistedDelayRemainsWithinConfiguredJitterBounds()
    {
        var policy = new RetryPolicy(RetryOptions.Default, new FixedTimeProvider(StartUtc), new SequenceRetryRandomSource(1));
        var state = RetryState.Start(StartUtc) with { PreviousDelay = TimeSpan.FromTicks(long.MaxValue / PersistedDelayDivisor) };

        var decision = policy.GetDecision(RetryFailure.Transient(), state);

        await Assert.That(decision.Delay).IsEqualTo(RetryOptions.Default.MaximumDelay);
        await Assert.That(decision.DueUtc).IsEqualTo(StartUtc + RetryOptions.Default.MaximumDelay);
    }

    /// <summary>Verifies a restored delay below the current minimum is safely rebased after reconfiguration.</summary>
    /// <param name="ticks">The small persisted delay.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments(0L)]
    [Arguments(1L)]
    public async Task PersistedDelayBelowMinimumCannotCreateImmediateRetries(long ticks)
    {
        var policy = new RetryPolicy(RetryOptions.Default, new FixedTimeProvider(StartUtc), new SequenceRetryRandomSource(1));
        var state = RetryState.Start(StartUtc) with { PreviousDelay = TimeSpan.FromTicks(ticks) };

        var decision = policy.GetDecision(RetryFailure.Transient(), state);

        await Assert.That(decision.Delay.GetValueOrDefault()).IsGreaterThanOrEqualTo(RetryOptions.Default.MinimumDelay);
    }

    /// <summary>Verifies credential changes do not bypass the operation's retry lifetime.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ExpiredOperationsDoNotRetryAfterCredentialRenewal()
    {
        var clock = new FixedTimeProvider(StartUtc + RetryOptions.Default.MaximumRetryAge);
        var policy = new RetryPolicy(RetryOptions.Default, clock, new SequenceRetryRandomSource(0));

        var decision = policy.GetDecision(RetryFailure.AuthenticationTokenRenewed(FirstCredentialsVersion), RetryState.Start(StartUtc));

        await Assert.That(decision.Kind).IsEqualTo(RetryDecisionKind.Stop);
        await Assert.That(decision.StopReason).IsEqualTo(RetryStopReason.RetryAgeExhausted);
    }

    /// <summary>Verifies incomplete persisted state fails before a decision is returned.</summary>
    /// <returns>A task representing the assertion.</returns>
    [Test]
    public async Task NullStateIsRejected()
    {
        var policy = new RetryPolicy();
        await Assert.That(() => policy.GetDecision(new(RetryFailureKind.AuthorizationDenied), null!))
            .ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies corrupt persisted attempt counters cannot restart the retry budget.</summary>
    /// <returns>A task representing the assertion.</returns>
    [Test]
    public async Task NegativeAttemptCountIsRejected()
    {
        var policy = new RetryPolicy(RetryOptions.Default, new FixedTimeProvider(StartUtc), new SequenceRetryRandomSource(0));
        var state = RetryState.Start(StartUtc) with { TransientAttemptCount = -1 };
        await Assert.That(() => policy.GetDecision(RetryFailure.Transient(), state)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies corrupt persisted delays cannot produce negative jitter.</summary>
    /// <returns>A task representing the assertion.</returns>
    [Test]
    public async Task NegativePersistedDelayIsRejected()
    {
        var policy = new RetryPolicy(RetryOptions.Default, new FixedTimeProvider(StartUtc), new SequenceRetryRandomSource(0));
        var state = RetryState.Start(StartUtc) with { PreviousDelay = TimeSpan.FromTicks(-1) };
        await Assert.That(() => policy.GetDecision(RetryFailure.Transient(), state)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies an invalid server delay cannot silently become a normal retry hint.</summary>
    /// <returns>A task representing the assertion.</returns>
    [Test]
    public async Task NegativeRetryAfterIsRejected()
    {
        var policy = new RetryPolicy(RetryOptions.Default, new FixedTimeProvider(StartUtc), new SequenceRetryRandomSource(0));
        var state = RetryState.Start(StartUtc);
        await Assert.That(() => policy.GetDecision(RetryFailure.Transient(TimeSpan.FromTicks(-1)), state)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies an unknown persisted authentication state fails closed.</summary>
    /// <returns>A task representing the assertion.</returns>
    [Test]
    public async Task UndefinedAuthenticationStateIsRejected()
    {
        var policy = new RetryPolicy(RetryOptions.Default, new FixedTimeProvider(StartUtc), new SequenceRetryRandomSource(0));
        var state = RetryState.Start(StartUtc) with { AuthenticationState = (RetryAuthenticationState)(-1) };
        await Assert.That(() => policy.GetDecision(RetryFailure.Transient(), state)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Provides a deterministic time source.</summary>
    /// <param name="utcNow">The current UTC time.</param>
    private sealed class FixedTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => utcNow;
    }

    /// <summary>Provides deterministic random values for retry jitter.</summary>
    /// <param name="values">The values to return.</param>
    private sealed class SequenceRetryRandomSource(params double[] values) : IRetryRandomSource
    {
        /// <summary>The current value index.</summary>
        private int _index;

        /// <inheritdoc/>
        public double NextDouble()
        {
            if (_index >= values.Length)
            {
                return values[^1];
            }

            var value = values[_index];
            _index++;
            return value;
        }
    }
}
