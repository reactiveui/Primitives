// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reflection;
using Microsoft.Extensions.Time.Testing;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="CircuitBreaker"/>.</summary>
public sealed class CircuitBreakerTests
{
    /// <summary>Defines the standard transient failure threshold.</summary>
    private const int FailureThreshold = 5;

    /// <summary>Defines a stable endpoint identity for tests.</summary>
    private const string Endpoint = "https://sync.example.test";

    /// <summary>Defines the standard open interval.</summary>
    private static readonly TimeSpan OpenDuration = TimeSpan.FromSeconds(30);

    /// <summary>Verifies the configured consecutive transient failures open the breaker.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task FiveConsecutiveTransientFailuresOpenTheBreaker()
    {
        var timeProvider = new FakeTimeProvider();
        var breaker = CreateBreaker(timeProvider);

        for (var failure = 0; failure < FailureThreshold; failure++)
        {
            breaker.RecordTransientFailure();
        }

        var snapshot = breaker.Snapshot;

        await Assert.That(snapshot.State).IsEqualTo(CircuitBreakerState.Open);
        await Assert.That(snapshot.ConsecutiveTransientFailures).IsEqualTo(FailureThreshold);
        await Assert.That(breaker.TryAcquire()).IsFalse();
    }

    /// <summary>Verifies an open breaker permits one probe after its delay and rejects concurrent peers.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task OpenBreakerPermitsExactlyOneProbeAfterDelay()
    {
        var timeProvider = new FakeTimeProvider();
        var breaker = CreateOpenBreaker(timeProvider);

        timeProvider.Advance(OpenDuration);

        await Assert.That(breaker.TryAcquire()).IsTrue();
        await Assert.That(breaker.TryAcquire()).IsFalse();
        await Assert.That(breaker.Snapshot.State).IsEqualTo(CircuitBreakerState.HalfOpen);
    }

    /// <summary>Verifies a successful probe resets the breaker for normal admission.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task SuccessfulHandshakeResetsTheBreaker()
    {
        var timeProvider = new FakeTimeProvider();
        var breaker = CreateOpenBreaker(timeProvider);

        timeProvider.Advance(OpenDuration);
        _ = breaker.TryAcquire();
        breaker.RecordSuccess();

        var snapshot = breaker.Snapshot;

        await Assert.That(snapshot.State).IsEqualTo(CircuitBreakerState.Closed);
        await Assert.That(snapshot.ConsecutiveTransientFailures).IsEqualTo(0);
        await Assert.That(snapshot.RetryAfterUtc).IsNull();
        await Assert.That(breaker.TryAcquire()).IsTrue();
    }

    /// <summary>Verifies a failed probe returns the breaker to the configured open delay.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task FailedHalfOpenProbeReopensTheBreaker()
    {
        var timeProvider = new FakeTimeProvider();
        var breaker = CreateOpenBreaker(timeProvider);

        timeProvider.Advance(OpenDuration);
        _ = breaker.TryAcquire();
        breaker.RecordTransientFailure();

        await Assert.That(breaker.Snapshot.State).IsEqualTo(CircuitBreakerState.Open);
        await Assert.That(breaker.TryAcquire()).IsFalse();
    }

    /// <summary>Verifies an abandoned probe returns the breaker to an open, recoverable state.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AbandonedHalfOpenProbeReopensTheBreaker()
    {
        var timeProvider = new FakeTimeProvider();
        var breaker = CreateOpenBreaker(timeProvider);

        timeProvider.Advance(OpenDuration);
        _ = breaker.TryAcquire();
        breaker.AbandonProbe();

        await Assert.That(breaker.Snapshot.State).IsEqualTo(CircuitBreakerState.Open);
        await Assert.That(breaker.TryAcquire()).IsFalse();

        timeProvider.Advance(OpenDuration);

        await Assert.That(breaker.TryAcquire()).IsTrue();
    }

    /// <summary>Verifies default construction starts with an independent closed endpoint.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task NewEndpointAdmitsWorkAndPreservesItsIdentity()
    {
        var breaker = new CircuitBreaker(Endpoint);

        await Assert.That(breaker.TryAcquire()).IsTrue();
        await Assert.That(breaker.Snapshot.Endpoint).IsEqualTo(Endpoint);
        await Assert.That(breaker.Snapshot.RetryAfterUtc).IsNull();
    }

    /// <summary>Verifies admission remains exclusive when callers race at the recovery deadline.</summary>
    /// <returns>A task representing the concurrent calls and assertions.</returns>
    [Test]
    public async Task ConcurrentRecoveryAttemptsAdmitOneProbe()
    {
        var timeProvider = new FakeTimeProvider();
        var breaker = CreateOpenBreaker(timeProvider);
        timeProvider.Advance(OpenDuration);
        var start = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = Task.Run(async () =>
        {
            await start.Task;
            return breaker.TryAcquire();
        });
        var second = Task.Run(async () =>
        {
            await start.Task;
            return breaker.TryAcquire();
        });

        start.SetResult(true);
        var results = await Task.WhenAll(first, second);

        await Assert.That(results.Count(static admitted => admitted)).IsEqualTo(1);
    }

    /// <summary>Verifies failures reported by outstanding calls cannot extend an open circuit indefinitely.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task LateFailuresPreserveOpenDeadlineAndSaturatedCount()
    {
        var timeProvider = new FakeTimeProvider();
        var breaker = CreateOpenBreaker(timeProvider);
        var opened = breaker.Snapshot;
        timeProvider.Advance(TimeSpan.FromTicks(1));

        breaker.RecordTransientFailure();
        breaker.AbandonProbe();

        await Assert.That(breaker.Snapshot).IsEqualTo(opened);
    }

    /// <summary>Verifies cancelling an ordinary closed-state call does not open the endpoint.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AbandonWithoutProbePreservesClosedState()
    {
        var breaker = CreateBreaker(new());
        breaker.AbandonProbe();

        await Assert.That(breaker.Snapshot.State).IsEqualTo(CircuitBreakerState.Closed);
    }

    /// <summary>Verifies each successful handshake breaks the consecutive-failure sequence.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task SuccessfulHandshakeRestartsFailureCounting()
    {
        var breaker = CreateBreaker(new());
        breaker.RecordTransientFailure();
        breaker.RecordSuccess();
        breaker.RecordTransientFailure();

        await Assert.That(breaker.Snapshot.ConsecutiveTransientFailures).IsEqualTo(1);
        await Assert.That(breaker.Snapshot.State).IsEqualTo(CircuitBreakerState.Closed);
    }

    /// <summary>Verifies extreme clocks cannot make failure reporting overflow.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task RetryDeadlineSaturatesAtMaximumUtcTime()
    {
        var timeProvider = new FakeTimeProvider(DateTimeOffset.MaxValue - TimeSpan.FromTicks(1));
        var breaker = new CircuitBreaker(Endpoint, new() { FailureThreshold = 1 }, timeProvider);

        breaker.RecordTransientFailure();

        await Assert.That(breaker.Snapshot.RetryAfterUtc).IsEqualTo(DateTimeOffset.MaxValue);
        await Assert.That(breaker.TryAcquire()).IsFalse();
    }

    /// <summary>Verifies an endpoint cannot be missing or whitespace.</summary>
    /// <param name="endpoint">The invalid endpoint.</param>
    /// <returns>A task representing the assertion.</returns>
    [Test]
    [Arguments("")]
    [Arguments(" \t")]
    public async Task BlankEndpointsAreRejected(string endpoint) =>
        await Assert.That(() => new CircuitBreaker(endpoint)).ThrowsExactly<ArgumentException>();

    /// <summary>Verifies null dependencies fail before any state is created.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task NullDependenciesAreRejected()
    {
        var endpointConstructor = typeof(CircuitBreaker).GetConstructor([typeof(string)]);
        ArgumentNullException.ThrowIfNull(endpointConstructor);
        await Assert.That(() => endpointConstructor.Invoke(BindingFlags.DoNotWrapExceptions, null, [null], null)).ThrowsExactly<ArgumentNullException>();
        var optionsConstructor = typeof(CircuitBreaker).GetConstructor([typeof(string), typeof(CircuitBreakerOptions), typeof(TimeProvider)]);
        ArgumentNullException.ThrowIfNull(optionsConstructor);
        await Assert.That(() => optionsConstructor.Invoke(BindingFlags.DoNotWrapExceptions, null, [Endpoint, null, TimeProvider.System], null)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => optionsConstructor.Invoke(BindingFlags.DoNotWrapExceptions, null, [Endpoint, new CircuitBreakerOptions(), null], null)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies invalid configuration cannot create a breaker.</summary>
    /// <returns>A task representing the assertion.</returns>
    [Test]
    public async Task ConstructionValidatesOptions() =>
        await Assert.That(static () => new CircuitBreaker(Endpoint, new() { FailureThreshold = 0 }, TimeProvider.System))
            .ThrowsExactly<InvalidOperationException>();

    /// <summary>Creates a breaker with test defaults.</summary>
    /// <param name="timeProvider">The deterministic time source.</param>
    /// <returns>A configured breaker.</returns>
    private static CircuitBreaker CreateBreaker(FakeTimeProvider timeProvider) =>
        new(Endpoint, new() { FailureThreshold = FailureThreshold, OpenDuration = OpenDuration }, timeProvider);

    /// <summary>Creates an opened test breaker.</summary>
    /// <param name="timeProvider">The deterministic time source.</param>
    /// <returns>An open breaker.</returns>
    private static CircuitBreaker CreateOpenBreaker(FakeTimeProvider timeProvider)
    {
        var breaker = CreateBreaker(timeProvider);

        for (var failure = 0; failure < FailureThreshold; failure++)
        {
            breaker.RecordTransientFailure();
        }

        return breaker;
    }
}
