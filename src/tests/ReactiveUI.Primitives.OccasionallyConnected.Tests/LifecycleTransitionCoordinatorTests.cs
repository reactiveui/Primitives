// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="LifecycleTransitionCoordinator"/>.</summary>
public sealed class LifecycleTransitionCoordinatorTests
{
    /// <summary>The expected count after a start and cleanup pair.</summary>
    private const int TwoTransitions = 2;

    /// <summary>The expected order entry count for a stop racing startup.</summary>
    private const int ThreeOrderEntries = 3;

    /// <summary>The startup failure message used by failure-path tests.</summary>
    private const string StartupFailedMessage = "startup failed";

    /// <summary>The guard used for released asynchronous transitions.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies concurrent startup requests share one callback.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConcurrentStartCallersShareStartupTransition()
    {
        var entered = CreateSignal();
        var release = CreateSignal();
        var starts = 0;
        var stops = 0;
        var coordinator = new LifecycleTransitionCoordinator(
            () =>
            {
                starts++;
                entered.SetResult();
                return new(release.Task);
            },
            () =>
            {
                stops++;
                return default;
            });

        var first = coordinator.StartAsync(CancellationToken.None);
        await entered.Task.WaitAsync(GuardTimeout);
        var second = coordinator.StartAsync(CancellationToken.None);

        await Assert.That(starts).IsEqualTo(1);
        await Assert.That(first.IsCompleted).IsFalse();
        await Assert.That(second.IsCompleted).IsFalse();

        release.SetResult();
        await first.WaitAsync(GuardTimeout);
        await second.WaitAsync(GuardTimeout);
        await coordinator.StartAsync(CancellationToken.None).WaitAsync(GuardTimeout);

        await Assert.That(starts).IsEqualTo(1);
        await Assert.That(stops).IsEqualTo(0);
    }

    /// <summary>Verifies concurrent stop requests share one cleanup callback.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConcurrentStopCallersShareCleanupTransition()
    {
        var entered = CreateSignal();
        var release = CreateSignal();
        var stops = 0;
        var coordinator = new LifecycleTransitionCoordinator(static () => default, () =>
        {
            stops++;
            entered.SetResult();
            return new(release.Task);
        });

        await coordinator.StartAsync(CancellationToken.None);
        var first = coordinator.StopAsync(CancellationToken.None);
        await entered.Task.WaitAsync(GuardTimeout);
        var second = coordinator.StopAsync(CancellationToken.None);

        await Assert.That(stops).IsEqualTo(1);
        await Assert.That(first.IsCompleted).IsFalse();
        await Assert.That(second.IsCompleted).IsFalse();

        release.SetResult();
        await first.WaitAsync(GuardTimeout);
        await second.WaitAsync(GuardTimeout);
        await coordinator.StopAsync(CancellationToken.None).WaitAsync(GuardTimeout);

        await Assert.That(stops).IsEqualTo(1);
    }

    /// <summary>Verifies stop waits for startup and then cleans the started resources.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopDuringStartupWaitsForStartupThenCleanup()
    {
        var entered = CreateSignal();
        var release = CreateSignal();
        var order = new List<string>();
        var coordinator = new LifecycleTransitionCoordinator(
            async ValueTask () =>
            {
                order.Add("start-enter");
                entered.SetResult();
                await release.Task.ConfigureAwait(false);
                order.Add("start-exit");
            },
            () =>
            {
                order.Add("cleanup");
                return default;
            });

        var start = coordinator.StartAsync(CancellationToken.None);
        await entered.Task.WaitAsync(GuardTimeout);
        var stop = coordinator.StopAsync(CancellationToken.None);

        await Assert.That(stop.IsCompleted).IsFalse();
        release.SetResult();
        await start.WaitAsync(GuardTimeout);
        await stop.WaitAsync(GuardTimeout);

        await Assert.That(order.Count).IsEqualTo(ThreeOrderEntries);
        await Assert.That(order[0]).IsEqualTo("start-enter");
        await Assert.That(order[1]).IsEqualTo("start-exit");
        await Assert.That(order[2]).IsEqualTo("cleanup");
    }

    /// <summary>Verifies overlapping startup calls join startup without replacing an accepted stop intent.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StartCallOverlappingStartupJoinsCurrentCallbackEvenWhenStopIntentIsPending()
    {
        var entered = CreateSignal();
        var release = CreateSignal();
        var cleaned = CreateSignal();
        var starts = 0;
        var stops = 0;
        var coordinator = new LifecycleTransitionCoordinator(
            () =>
            {
                starts++;
                entered.SetResult();
                return new(release.Task);
            },
            () =>
            {
                stops++;
                cleaned.SetResult();
                return default;
            });

        var first = coordinator.StartAsync(CancellationToken.None);
        await entered.Task.WaitAsync(GuardTimeout);
        var stop = coordinator.StopAsync(CancellationToken.None);
        var joined = coordinator.StartAsync(CancellationToken.None);

        await Assert.That(joined.IsCompleted).IsFalse();

        release.SetResult();
        await first.WaitAsync(GuardTimeout);
        await joined.WaitAsync(GuardTimeout);
        await cleaned.Task.WaitAsync(GuardTimeout);
        await stop.WaitAsync(GuardTimeout);

        await Assert.That(starts).IsEqualTo(1);
        await Assert.That(stops).IsEqualTo(1);
    }

    /// <summary>Verifies caller cancellation only releases that caller's wait.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CancelledStartWaiterDoesNotCancelSharedStartup()
    {
        var entered = CreateSignal();
        var release = CreateSignal();
        var starts = 0;
        var stops = 0;
        using var cancellation = new CancellationTokenSource();
        var coordinator = new LifecycleTransitionCoordinator(
            () =>
            {
                starts++;
                entered.SetResult();
                return new(release.Task);
            },
            () =>
            {
                stops++;
                return default;
            });

        var owner = coordinator.StartAsync(CancellationToken.None);
        await entered.Task.WaitAsync(GuardTimeout);
        var cancelled = coordinator.StartAsync(cancellation.Token);

        await cancellation.CancelAsync();
        var exception = await Assert.ThrowsExactlyAsync<TaskCanceledException>(() => cancelled);
        await Assert.That(exception?.CancellationToken).IsEqualTo(cancellation.Token);
        await Assert.That(owner.IsCompleted).IsFalse();

        release.SetResult();
        await owner.WaitAsync(GuardTimeout);
        await coordinator.StopAsync(CancellationToken.None).WaitAsync(GuardTimeout);

        await Assert.That(starts).IsEqualTo(1);
        await Assert.That(stops).IsEqualTo(1);
    }

    /// <summary>Verifies a cancelled stop waiter does not abandon accepted cleanup intent.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CancelledStopWaiterStillCleansAfterStartupCompletes()
    {
        var entered = CreateSignal();
        var release = CreateSignal();
        var cleaned = CreateSignal();
        var starts = 0;
        var stops = 0;
        using var cancellation = new CancellationTokenSource();
        var coordinator = new LifecycleTransitionCoordinator(
            () =>
            {
                starts++;
                entered.SetResult();
                return new(release.Task);
            },
            () =>
            {
                stops++;
                cleaned.SetResult();
                return default;
            });

        var startup = coordinator.StartAsync(CancellationToken.None);
        await entered.Task.WaitAsync(GuardTimeout);
        var stopped = coordinator.StopAsync(cancellation.Token);

        await cancellation.CancelAsync();
        var exception = await Assert.ThrowsExactlyAsync<TaskCanceledException>(() => stopped);
        await Assert.That(exception?.CancellationToken).IsEqualTo(cancellation.Token);

        release.SetResult();
        await startup.WaitAsync(GuardTimeout);
        await cleaned.Task.WaitAsync(GuardTimeout);

        await Assert.That(starts).IsEqualTo(1);
        await Assert.That(stops).IsEqualTo(1);
    }

    /// <summary>Verifies a cancelled restart waiter does not abandon accepted startup intent.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CancelledStartWaiterDuringCleanupStillStartsAfterCleanupCompletes()
    {
        var cleanupEntered = CreateSignal();
        var releaseCleanup = CreateSignal();
        var restarted = CreateSignal();
        var starts = 0;
        var stops = 0;
        using var cancellation = new CancellationTokenSource();
        var coordinator = new LifecycleTransitionCoordinator(
            () =>
            {
                starts++;
                if (starts != TwoTransitions)
                {
                    return default;
                }

                restarted.SetResult();
                return default;
            },
            () =>
            {
                stops++;
                cleanupEntered.SetResult();
                return new(releaseCleanup.Task);
            });

        await coordinator.StartAsync(CancellationToken.None);
        var stopped = coordinator.StopAsync(CancellationToken.None);
        await cleanupEntered.Task.WaitAsync(GuardTimeout);
        var restart = coordinator.StartAsync(cancellation.Token);

        await cancellation.CancelAsync();
        var exception = await Assert.ThrowsExactlyAsync<TaskCanceledException>(() => restart);
        await Assert.That(exception?.CancellationToken).IsEqualTo(cancellation.Token);

        releaseCleanup.SetResult();
        await stopped.WaitAsync(GuardTimeout);
        await restarted.Task.WaitAsync(GuardTimeout);

        await Assert.That(starts).IsEqualTo(TwoTransitions);
        await Assert.That(stops).IsEqualTo(1);
    }

    /// <summary>Verifies disposal resolves a startup queued behind cleanup as disposed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeDuringCleanupRejectsQueuedStartupAsDisposed()
    {
        var cleanupEntered = CreateSignal();
        var releaseCleanup = CreateSignal();
        var starts = 0;
        var coordinator = new LifecycleTransitionCoordinator(
            () =>
            {
                starts++;
                return default;
            },
            () =>
            {
                cleanupEntered.SetResult();
                return new(releaseCleanup.Task);
            });

        await coordinator.StartAsync(CancellationToken.None);
        var stopped = coordinator.StopAsync(CancellationToken.None);
        await cleanupEntered.Task.WaitAsync(GuardTimeout);
        var restart = coordinator.StartAsync(CancellationToken.None);
        var dispose = coordinator.DisposeAsync().AsTask();

        await Assert.ThrowsExactlyAsync<ObjectDisposedException>(() => restart);
        releaseCleanup.SetResult();
        await stopped.WaitAsync(GuardTimeout);
        await dispose.WaitAsync(GuardTimeout);

        await Assert.That(starts).IsEqualTo(1);
    }

    /// <summary>Verifies disposal cleanup failure stops automatic retries and permits deliberate retry.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FailedCleanupDuringDisposeDoesNotRetryUntilDisposeIsCalledAgain()
    {
        var cleanupFailure = new InvalidOperationException("cleanup failed");
        var stops = 0;
        var coordinator = new LifecycleTransitionCoordinator(
            static () => default,
            () =>
            {
                stops++;
                return stops == 1 ? ValueTask.FromException(cleanupFailure) : default;
            });

        await coordinator.StartAsync(CancellationToken.None);
        var thrown = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => coordinator.DisposeAsync().AsTask());
        await Assert.That(thrown).IsSameReferenceAs(cleanupFailure);
        await Assert.That(stops).IsEqualTo(1);

        await coordinator.DisposeAsync().AsTask().WaitAsync(GuardTimeout);

        await Assert.That(stops).IsEqualTo(TwoTransitions);
    }

    /// <summary>Verifies failed startup leaves cleanup due before a deliberate retry.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FailedStartupCleansPartialResourcesBeforeRetry()
    {
        var failure = new InvalidOperationException(StartupFailedMessage);
        var starts = 0;
        var stops = 0;
        var coordinator = new LifecycleTransitionCoordinator(
            () =>
            {
                starts++;
                return starts == 1 ? ValueTask.FromException(failure) : default;
            },
            () =>
            {
                stops++;
                return default;
            });

        var thrown = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => coordinator.StartAsync(CancellationToken.None));
        await Assert.That(thrown).IsSameReferenceAs(failure);
        await Assert.That(stops).IsEqualTo(0);

        await coordinator.StartAsync(CancellationToken.None).WaitAsync(GuardTimeout);

        await Assert.That(starts).IsEqualTo(TwoTransitions);
        await Assert.That(stops).IsEqualTo(1);
    }

    /// <summary>Verifies a stop accepted during failed startup still cleans partial resources.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopDuringFailedStartupCleansPartialResources()
    {
        var entered = CreateSignal();
        var release = CreateSignal();
        var failure = new InvalidOperationException(StartupFailedMessage);
        var starts = 0;
        var stops = 0;
        var coordinator = new LifecycleTransitionCoordinator(
            async ValueTask () =>
            {
                starts++;
                entered.SetResult();
                await release.Task.ConfigureAwait(false);
                throw failure;
            },
            () =>
            {
                stops++;
                return default;
            });

        var startup = coordinator.StartAsync(CancellationToken.None);
        await entered.Task.WaitAsync(GuardTimeout);
        var stop = coordinator.StopAsync(CancellationToken.None);

        release.SetResult();
        var thrown = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => startup);
        await stop.WaitAsync(GuardTimeout);

        await Assert.That(thrown).IsSameReferenceAs(failure);
        await Assert.That(starts).IsEqualTo(1);
        await Assert.That(stops).IsEqualTo(1);
    }

    /// <summary>Verifies disposal accepted during failed startup still cleans partial resources.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeDuringFailedStartupCleansPartialResources()
    {
        var entered = CreateSignal();
        var release = CreateSignal();
        var failure = new InvalidOperationException(StartupFailedMessage);
        var starts = 0;
        var stops = 0;
        var coordinator = new LifecycleTransitionCoordinator(
            async ValueTask () =>
            {
                starts++;
                entered.SetResult();
                await release.Task.ConfigureAwait(false);
                throw failure;
            },
            () =>
            {
                stops++;
                return default;
            });

        var startup = coordinator.StartAsync(CancellationToken.None);
        await entered.Task.WaitAsync(GuardTimeout);
        var dispose = coordinator.DisposeAsync().AsTask();

        release.SetResult();
        var thrown = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => startup);
        await dispose.WaitAsync(GuardTimeout);

        await Assert.That(thrown).IsSameReferenceAs(failure);
        await Assert.That(starts).IsEqualTo(1);
        await Assert.That(stops).IsEqualTo(1);
        await Assert.That(() => coordinator.StartAsync(CancellationToken.None)).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies failed cleanup must be retried before restart.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FailedCleanupIsRetriedBeforeRestart()
    {
        var cleanupFailure = new InvalidOperationException("cleanup failed");
        var starts = 0;
        var stops = 0;
        var coordinator = new LifecycleTransitionCoordinator(
            () =>
            {
                starts++;
                return default;
            },
            () =>
            {
                stops++;
                return stops == 1 ? ValueTask.FromException(cleanupFailure) : default;
            });

        await coordinator.StartAsync(CancellationToken.None);
        var thrown = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => coordinator.StopAsync(CancellationToken.None));
        await Assert.That(thrown).IsSameReferenceAs(cleanupFailure);

        await coordinator.StartAsync(CancellationToken.None).WaitAsync(GuardTimeout);

        await Assert.That(starts).IsEqualTo(TwoTransitions);
        await Assert.That(stops).IsEqualTo(TwoTransitions);
    }

    /// <summary>Verifies disposal waits for startup, cleans resources, and prevents restart.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeDuringStartupCleansAndPreventsRestart()
    {
        var entered = CreateSignal();
        var release = CreateSignal();
        var stops = 0;
        var coordinator = new LifecycleTransitionCoordinator(
            () =>
            {
                entered.SetResult();
                return new(release.Task);
            },
            () =>
            {
                stops++;
                return default;
            });

        var start = coordinator.StartAsync(CancellationToken.None);
        await entered.Task.WaitAsync(GuardTimeout);
        var dispose = coordinator.DisposeAsync().AsTask();

        await Assert.That(dispose.IsCompleted).IsFalse();
        release.SetResult();
        await start.WaitAsync(GuardTimeout);
        await dispose.WaitAsync(GuardTimeout);
        await coordinator.StopAsync(CancellationToken.None).WaitAsync(GuardTimeout);

        await Assert.That(stops).IsEqualTo(1);
        await Assert.That(() => coordinator.StartAsync(CancellationToken.None)).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies disposing a never-started coordinator does not invoke cleanup.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeBeforeStartupCompletesWithoutCleanup()
    {
        var stops = 0;
        var coordinator = new LifecycleTransitionCoordinator(static () => default, () =>
        {
            stops++;
            return default;
        });

        await coordinator.DisposeAsync();
        await coordinator.StopAsync(CancellationToken.None).WaitAsync(GuardTimeout);

        await Assert.That(stops).IsEqualTo(0);
    }

    /// <summary>Verifies caller-cancelled operations fail before transitions begin.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PreCancelledCallsFail()
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        var coordinator = new LifecycleTransitionCoordinator(static () => default, static () => default);

        await Assert.That(() => coordinator.StartAsync(cancellation.Token)).ThrowsExactly<OperationCanceledException>();
        await Assert.That(() => coordinator.StopAsync(cancellation.Token)).ThrowsExactly<OperationCanceledException>();
    }

    /// <summary>Creates a completion signal whose continuations cannot run inline.</summary>
    /// <returns>The completion signal.</returns>
    private static TaskCompletionSource CreateSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
