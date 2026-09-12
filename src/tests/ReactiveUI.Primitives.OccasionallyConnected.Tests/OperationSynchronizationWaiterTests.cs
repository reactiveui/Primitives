// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Extensions.Time.Testing;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OperationSynchronizationWaiter"/>.</summary>
public sealed partial class OperationSynchronizationWaiterTests
{
    /// <summary>The logical stream being synchronized.</summary>
    private static readonly StreamId Stream = new("orders");

    /// <summary>The bounded wait duration used with virtual time.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(20);

    /// <summary>The real-time guard for a blocked adapter callback regression.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies waiting survives unrelated operations and pending progress.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenOperationIsPending_ThenOnlyItsSuccessfulTerminalStateCompletesWait()
    {
        using var states = new Signal<SyncOperationStatus>();
        var clock = new FakeTimeProvider();
        var operationId = OperationId.New();
        var pending = Status(operationId, SyncOperationState.QueuedForUpload);
        var wait = OperationSynchronizationWaiter.WaitAsync(
            states,
            _ => new(pending),
            operationId,
            WaitTimeout,
            clock,
            CancellationToken.None);

        await Assert.That(wait.IsCompleted).IsFalse();
        states.OnNext(Status(OperationId.New(), SyncOperationState.Synchronized));
        states.OnNext(Status(operationId, SyncOperationState.Uploading));
        states.OnNext(Status(operationId, SyncOperationState.SavedLocally));
        states.OnNext(Status(operationId, SyncOperationState.Conflict));
        await Assert.That(wait.IsCompleted).IsFalse();
        states.OnNext(Status(operationId, SyncOperationState.Synchronized));
        await wait;
        await Assert.That(states.HasObservers).IsFalse();
    }

    /// <summary>Verifies a result arriving during lookup is never lost.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenLiveResultArrivesDuringLookup_ThenWaitCompletesBeforeLookupReturns()
    {
        using var states = new Signal<SyncOperationStatus>();
        var operationId = OperationId.New();
        var lookup = new TaskCompletionSource<SyncOperationStatus?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var token = CancellationToken.None;
        await OperationSynchronizationWaiter.WaitAsync(
            states,
            cancellationToken =>
            {
                token = cancellationToken;
                states.OnNext(Status(operationId, SyncOperationState.Synchronized));
                return new(lookup.Task);
            },
            operationId,
            WaitTimeout,
            new FakeTimeProvider(),
            CancellationToken.None);

        await Assert.That(token).IsEqualTo(CancellationToken.None);
        await Assert.That(states.HasObservers).IsFalse();
        lookup.SetException(new InvalidOperationException("late lookup failure"));
    }

    /// <summary>Verifies timeout removes notifications without changing the operation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenTimeoutElapses_ThenWaitEndsWithoutChangingOperation()
    {
        using var states = new Signal<SyncOperationStatus>();
        var operationId = OperationId.New();
        var clock = new FakeTimeProvider();
        var lookup = new TaskCompletionSource<SyncOperationStatus?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var token = CancellationToken.None;
        var wait = OperationSynchronizationWaiter.WaitAsync(
            states,
            cancellationToken =>
            {
                token = cancellationToken;
                return new(lookup.Task);
            },
            operationId,
            WaitTimeout,
            clock,
            CancellationToken.None);

        clock.Advance(WaitTimeout);
        await Assert.That(() => wait).ThrowsExactly<TimeoutException>();
        await Assert.That(token).IsEqualTo(CancellationToken.None);
        await Assert.That(states.HasObservers).IsFalse();
        lookup.SetException(new InvalidOperationException("lookup failed after timeout"));
        await Assert.That(() => wait).ThrowsExactly<TimeoutException>();
    }

    /// <summary>Verifies timeout completion cannot be blocked by an adapter cancellation callback.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenLookupCancellationCallbackBlocks_ThenTimeoutStillEndsWait()
    {
        using var states = new Signal<SyncOperationStatus>();
        using var releaseCallback = new ManualResetEventSlim();
        var clock = new FakeTimeProvider();
        var lookup = new TaskCompletionSource<SyncOperationStatus?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var registration = default(CancellationTokenRegistration);
        var wait = OperationSynchronizationWaiter.WaitAsync(
            states,
            token =>
            {
                registration = token.UnsafeRegister(WaitForRelease, releaseCallback);
                return new(lookup.Task);
            },
            OperationId.New(),
            WaitTimeout,
            clock,
            CancellationToken.None);
        try
        {
            clock.Advance(WaitTimeout);
            await Assert.That(() => wait.WaitAsync(GuardTimeout)).ThrowsExactly<TimeoutException>();
            await Assert.That(wait.IsCompleted).IsTrue();
            await Assert.That(states.HasObservers).IsFalse();
        }
        finally
        {
            releaseCallback.Set();
            await registration.DisposeAsync();
            lookup.SetResult(null);
        }
    }

    /// <summary>Verifies cancellation does not require a pending status lookup to finish.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenCallerCancels_ThenWaitReleasesItsSubscription()
    {
        using var states = new Signal<SyncOperationStatus>();
        using var cancellation = new CancellationTokenSource();
        var lookupToken = CancellationToken.None;
        var wait = OperationSynchronizationWaiter.WaitAsync(
            states,
            token =>
            {
                lookupToken = token;
                return new((SyncOperationStatus?)null);
            },
            OperationId.New(),
            Timeout.InfiniteTimeSpan,
            new FakeTimeProvider(),
            cancellation.Token);

        await cancellation.CancelAsync();
        await Assert.That(() => wait).Throws<TaskCanceledException>();
        await Assert.That(lookupToken).IsEqualTo(cancellation.Token);
        await Assert.That(states.HasObservers).IsFalse();
    }

    /// <summary>Verifies a terminal failure cannot be reported as synchronized.</summary>
    /// <param name="terminalState">The persisted terminal state.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(SyncOperationState.Rejected)]
    [Arguments(SyncOperationState.DeadLettered)]
    [Arguments(SyncOperationState.Ambiguous)]
    [Arguments(SyncOperationState.GuaranteeExpired)]
    public async Task WhenOperationCannotSynchronize_ThenWaitFails(SyncOperationState terminalState)
    {
        using var states = new Signal<SyncOperationStatus>();
        var operationId = OperationId.New();
        var wait = OperationSynchronizationWaiter.WaitAsync(
            states,
            _ => new(Status(operationId, terminalState)),
            operationId,
            WaitTimeout,
            new FakeTimeProvider(),
            CancellationToken.None);

        await Assert.That(() => wait).ThrowsExactly<InvalidOperationException>();
        await Assert.That(states.HasObservers).IsFalse();
    }

    /// <summary>Verifies a completed live source still permits recovery of a persisted success.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenSourceAlreadyCompleted_ThenPersistedSuccessStillCompletesWait()
    {
        using var states = new Signal<SyncOperationStatus>();
        states.OnCompleted();
        var operationId = OperationId.New();
        await OperationSynchronizationWaiter.WaitAsync(
            states,
            _ => new(Status(operationId, SyncOperationState.Synchronized)),
            operationId,
            WaitTimeout,
            new FakeTimeProvider(),
            CancellationToken.None);
    }

    /// <summary>Verifies lookup and notification failures are propagated without wrapping.</summary>
    /// <param name="failLookup">Whether the failure originates from the persisted lookup.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task WhenStatusSourceFails_ThenOriginalFailureIsReturned(bool failLookup)
    {
        using var states = new Signal<SyncOperationStatus>();
        var error = new InvalidOperationException("status unavailable");
        var wait = OperationSynchronizationWaiter.WaitAsync(
            states,
            _ => failLookup ? ValueTask.FromException<SyncOperationStatus?>(error) : new((SyncOperationStatus?)null),
            OperationId.New(),
            WaitTimeout,
            new FakeTimeProvider(),
            CancellationToken.None);
        if (!failLookup)
        {
            states.OnError(error);
        }

        var actual = await Assert.That(() => wait).ThrowsExactly<InvalidOperationException>();
        await Assert.That(actual).IsSameReferenceAs(error);
        await Assert.That(states.HasObservers).IsFalse();
    }

    /// <summary>Verifies an exhausted source does not leave an unknown operation waiting forever.</summary>
    /// <param name="completeBeforeLookup">Whether the source closes before the lookup finishes.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task WhenSourceCompletesWithoutTerminalStatus_ThenWaitFails(bool completeBeforeLookup)
    {
        using var states = new Signal<SyncOperationStatus>();
        var lookup = new TaskCompletionSource<SyncOperationStatus?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var wait = OperationSynchronizationWaiter.WaitAsync(
            states,
            _ => completeBeforeLookup ? new(lookup.Task) : new((SyncOperationStatus?)null),
            OperationId.New(),
            WaitTimeout,
            new FakeTimeProvider(),
            CancellationToken.None);
        if (completeBeforeLookup)
        {
            states.OnCompleted();
        }

        lookup.SetResult(null);
        if (!completeBeforeLookup)
        {
            states.OnCompleted();
        }

        await Assert.That(() => wait).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies persisted success is observed after restart without a new notification.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenOperationFinishedBeforeRestart_ThenPersistedStatusCompletesWait()
    {
        using var states = new Signal<SyncOperationStatus>();
        var operationId = OperationId.New();
        var persisted = Status(operationId, SyncOperationState.Synchronized);
        var reads = 0;
        await OperationSynchronizationWaiter.WaitAsync(
            states,
            _ =>
            {
                reads++;
                return new(persisted);
            },
            operationId,
            WaitTimeout,
            new FakeTimeProvider(),
            CancellationToken.None);

        await Assert.That(reads).IsEqualTo(1);
        await Assert.That(states.HasObservers).IsFalse();
    }

    /// <summary>Creates a durable status for an operation.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="state">The durable state.</param>
    /// <returns>The operation status.</returns>
    private static SyncOperationStatus Status(OperationId operationId, SyncOperationState state) =>
        new(operationId, Stream, state, 1, DateTimeOffset.UnixEpoch, null);

    /// <summary>Blocks an adapter cancellation callback until test cleanup releases it.</summary>
    /// <param name="state">The release signal.</param>
    private static void WaitForRelease(object? state)
    {
        if (state is not ManualResetEventSlim signal)
        {
            return;
        }

        signal.Wait();
    }
}
