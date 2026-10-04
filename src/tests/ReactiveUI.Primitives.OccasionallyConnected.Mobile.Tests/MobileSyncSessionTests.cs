// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Maui.Networking;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile.Tests;

/// <summary>Tests bounded lifecycle composition, races, cancellation and disposal.</summary>
public sealed class MobileSyncSessionTests
{
    /// <summary>The burst size used to verify bounded coalescing.</summary>
    private const int RequestCount = 100;

    /// <summary>The deterministic barrier timeout.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(10);

    /// <summary>Checks initial state and real Essentials abstraction hints drive the existing engine.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task RefreshAndConnectivityHintUseExistingEngine()
    {
        var context = new MobileTestContext();
        var lifecycle = new MobileTestLifecycle();
        var network = new MobileTestConnectivity();
        network.SetAccess(NetworkAccess.None);
        using var hints = new MauiConnectivityHint(network);
        await using var session = new MobileSyncSession(context, lifecycle, hints);
        await session.RefreshAsync(CancellationToken.None);
        await Assert.That(context.Starts).IsEqualTo(1);
        await Assert.That(context.Triggers).IsEqualTo(0);
        network.SetAccess(NetworkAccess.Internet);
        await session.Transition;
        await Assert.That(context.Triggers).IsEqualTo(1);
        lifecycle.SetSuspended(true);
        await session.Transition;
        var starts = context.Starts;
        network.SetAccess(NetworkAccess.Internet);
        await session.Transition;
        await Assert.That(context.Starts).IsEqualTo(starts);
    }

    /// <summary>Checks a suspend cancels blocked foreground startup and then drains the context.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task SuspendCancelsStartupAndStopsWithoutCancellation()
    {
        var entered = NewCompletion();
        Func<CancellationToken, ValueTask> start = async token =>
        {
            entered.SetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, token);
        };
        var context = new MobileTestContext { Start = start };
        var lifecycle = new MobileTestLifecycle();
        using var hints = new MauiConnectivityHint(new MobileTestConnectivity());
        await using var session = new MobileSyncSession(context, lifecycle, hints);
        var starting = session.ResumeAsync(CancellationToken.None).AsTask();
        await entered.Task.WaitAsync(WaitTimeout);
        CancellationToken stopToken = default;
        context.Stop = token =>
        {
            stopToken = token;
            return ValueTask.CompletedTask;
        };
        await session.SuspendAsync(CancellationToken.None).AsTask().WaitAsync(WaitTimeout);
        await starting;
        await Assert.That(context.Stops).IsEqualTo(1);
        await Assert.That(stopToken.CanBeCanceled).IsFalse();
        await Assert.That(context.Triggers).IsEqualTo(0);
    }

    /// <summary>Checks suspension also cancels a blocked connectivity-trigger operation.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task SuspendCancelsTriggerBeforeDurableStop()
    {
        var entered = NewCompletion();
        Func<CancellationToken, ValueTask> trigger = async token =>
        {
            entered.SetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, token);
        };
        var context = new MobileTestContext { Trigger = trigger };
        using var hints = new MauiConnectivityHint(new MobileTestConnectivity());
        await using var session = new MobileSyncSession(context, new MobileTestLifecycle(), hints);
        var starting = session.ResumeAsync(CancellationToken.None).AsTask();
        await entered.Task.WaitAsync(WaitTimeout);
        await session.SuspendAsync(CancellationToken.None).AsTask().WaitAsync(WaitTimeout);
        await starting;
        await Assert.That(context.Stops).IsEqualTo(1);
    }

    /// <summary>Checks cancelling a caller cannot abandon an accepted durable drain.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task CancelledSuspendWaitLeavesStopRunning()
    {
        var entered = NewCompletion();
        var release = NewCompletion();
        Func<CancellationToken, ValueTask> stop = async token =>
        {
            entered.SetResult();
            await release.Task;
        };
        var context = new MobileTestContext { Stop = stop };
        using var hints = new MauiConnectivityHint(new MobileTestConnectivity());
        await using var session = new MobileSyncSession(context, new MobileTestLifecycle(), hints);
        using var cancellation = new CancellationTokenSource();
        var stopping = session.SuspendAsync(cancellation.Token).AsTask();
        await entered.Task.WaitAsync(WaitTimeout);
        await cancellation.CancelAsync();
        await Assert.That(() => stopping).Throws<OperationCanceledException>();
        await Assert.That(session.Transition.IsCompleted).IsFalse();
        release.SetResult();
        await session.Transition;
        context.Stop = null;
    }

    /// <summary>Checks the latest intent wins a suspend/resume race without overlapping context calls.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task ResumeWaitsForInflightStopAndLatestIntentWins()
    {
        var entered = NewCompletion();
        var release = NewCompletion();
        Func<CancellationToken, ValueTask> stop = async token =>
        {
            _ = entered.TrySetResult();
            await release.Task;
        };
        var context = new MobileTestContext { Stop = stop };
        using var hints = new MauiConnectivityHint(new MobileTestConnectivity());
        await using var session = new MobileSyncSession(context, new MobileTestLifecycle(), hints);
        var stopping = session.SuspendAsync(CancellationToken.None).AsTask();
        await entered.Task.WaitAsync(WaitTimeout);
        var requests = Enumerable.Range(0, RequestCount)
            .Select(_ => session.ResumeAsync(CancellationToken.None).AsTask()).ToArray();
        await Assert.That(context.Starts).IsEqualTo(0);
        release.SetResult();
        await Task.WhenAll(requests);
        await stopping;
        await Assert.That(context.Starts).IsEqualTo(1);
        await Assert.That(context.Stops).IsEqualTo(1);
        context.Stop = null;
    }

    /// <summary>Checks a foreground failure does not prevent an already accepted suspend drain.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task ForegroundFailureStillDrainsAcceptedSuspend()
    {
        var entered = NewCompletion();
        var release = NewCompletion();
        Func<CancellationToken, ValueTask> startHook = async token =>
        {
            entered.SetResult();
            await release.Task;
            throw new InvalidOperationException("foreground failure");
        };
        var context = new MobileTestContext { Start = startHook };
        using var hints = new MauiConnectivityHint(new MobileTestConnectivity());
        await using var session = new MobileSyncSession(context, new MobileTestLifecycle(), hints);
        var start = session.ResumeAsync(CancellationToken.None).AsTask();
        await entered.Task.WaitAsync(WaitTimeout);
        var stop = session.SuspendAsync(CancellationToken.None).AsTask();
        release.SetResult();
        await Task.WhenAll(start, stop);
        await Assert.That(context.Stops).IsEqualTo(1);
        await Assert.That(session.LastFailure).IsTypeOf<InvalidOperationException>();
    }

    /// <summary>Checks a failed lifecycle operation remains observable and later requests can retry.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task FailedTransitionIsObservableAndRetryable()
    {
        var context = new MobileTestContext { Start = static _ => ValueTask.FromException(new InvalidOperationException("failure")) };
        var lifecycle = new MobileTestLifecycle();
        using var hints = new MauiConnectivityHint(new MobileTestConnectivity());
        await using var session = new MobileSyncSession(context, lifecycle, hints);
        lifecycle.SetSuspended(false);
        await Assert.That(() => session.Transition).ThrowsExactly<InvalidOperationException>();
        await Assert.That(session.LastFailure).IsTypeOf<InvalidOperationException>();
        context.Start = null;
        await session.ResumeAsync(CancellationToken.None);
        await Assert.That(context.Triggers).IsEqualTo(1);
    }

    /// <summary>Checks disposal shares one stop, removes event admission and preserves borrowed ownership.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task DisposalDrainsOnceAndRejectsFurtherWork()
    {
        var context = new MobileTestContext();
        var lifecycle = new MobileTestLifecycle();
        var network = new MobileTestConnectivity();
        using var hints = new MauiConnectivityHint(network);
        var session = new MobileSyncSession(context, lifecycle, hints);
        await session.ResumeAsync(CancellationToken.None);
        await Task.WhenAll(session.DisposeAsync().AsTask(), session.DisposeAsync().AsTask());
        var starts = context.Starts;
        lifecycle.SetSuspended(false);
        network.SetAccess(NetworkAccess.Internet);
        await Assert.That(context.Starts).IsEqualTo(starts);
        await Assert.That(context.Stops).IsEqualTo(1);
        await Assert.That(context.Disposed).IsFalse();
        await Assert.That(() => session.ResumeAsync(CancellationToken.None).AsTask()).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Checks cancellation before admission cannot start background work.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task PreCancelledIntentDoesNotStart()
    {
        var context = new MobileTestContext();
        using var hints = new MauiConnectivityHint(new MobileTestConnectivity());
        await using var session = new MobileSyncSession(context, new MobileTestLifecycle(), hints);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        await Assert.That(() => session.ResumeAsync(cancellation.Token).AsTask()).Throws<OperationCanceledException>();
        await Assert.That(context.Starts).IsEqualTo(0);
    }

    /// <summary>Checks resume cannot erase an accepted suspension before its durable stop.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task RapidResumeCannotSkipAnAcceptedSuspendDrain()
    {
        var entered = NewCompletion();
        var release = NewCompletion();
        var calls = new List<string>();
        var context = new MobileTestContext();
        context.Start = async token =>
        {
            calls.Add("start");
            if (context.Starts == 1)
            {
                entered.SetResult();
                await release.Task;
                token.ThrowIfCancellationRequested();
            }
        };
        context.Stop = _ =>
        {
            calls.Add("stop");
            return ValueTask.CompletedTask;
        };
        using var hints = new MauiConnectivityHint(new MobileTestConnectivity());
        await using var session = new MobileSyncSession(context, new MobileTestLifecycle(), hints);
        var start = session.ResumeAsync(CancellationToken.None).AsTask();
        await entered.Task.WaitAsync(WaitTimeout);
        var stop = session.SuspendAsync(CancellationToken.None).AsTask();
        var resume = session.ResumeAsync(CancellationToken.None).AsTask();
        release.SetResult();
        await Task.WhenAll(start, stop, resume);
        await Assert.That(string.Join(",", calls)).IsEqualTo("start,stop,start");
    }

    /// <summary>Checks disposal cancels foreground startup and waits for the final stop.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task DisposalCancelsStartupAndWaitsForDrain()
    {
        var entered = NewCompletion();
        var stopped = NewCompletion();
        var release = NewCompletion();
        Func<CancellationToken, ValueTask> startHook = async token =>
        {
            entered.SetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, token);
        };
        Func<CancellationToken, ValueTask> stopHook = async token =>
        {
            stopped.SetResult();
            await release.Task;
        };
        var context = new MobileTestContext { Start = startHook, Stop = stopHook };
        using var hints = new MauiConnectivityHint(new MobileTestConnectivity());
        var session = new MobileSyncSession(context, new MobileTestLifecycle(), hints);
        var start = session.ResumeAsync(CancellationToken.None).AsTask();
        await entered.Task.WaitAsync(WaitTimeout);
        var disposal = session.DisposeAsync().AsTask();
        await stopped.Task.WaitAsync(WaitTimeout);
        await Assert.That(disposal.IsCompleted).IsFalse();
        release.SetResult();
        await Task.WhenAll(start, disposal);
        await Assert.That(context.Stops).IsEqualTo(1);
        await Assert.That(context.Disposed).IsFalse();
    }

    /// <summary>Checks failure of the final durable drain remains visible on repeated disposal.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task FailedDisposalRetainsItsSharedFailure()
    {
        var context = new MobileTestContext { Stop = static _ => ValueTask.FromException(new InvalidOperationException("drain failed")) };
        using var hints = new MauiConnectivityHint(new MobileTestConnectivity());
        var session = new MobileSyncSession(context, new MobileTestLifecycle(), hints);
        await Assert.That(() => session.DisposeAsync().AsTask()).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => session.DisposeAsync().AsTask()).ThrowsExactly<InvalidOperationException>();
        await Assert.That(context.Stops).IsEqualTo(1);
        await Assert.That(session.LastFailure).IsTypeOf<InvalidOperationException>();
    }

    /// <summary>Checks a racing resume cannot pass a failed durable drain and a retry drains before starting.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task FailedStopBlocksRacingResumeUntilADrainSucceeds()
    {
        var entered = NewCompletion();
        var release = NewCompletion();
        Func<CancellationToken, ValueTask> stopHook = async token =>
        {
            entered.SetResult();
            await release.Task;
            throw new InvalidOperationException("drain failed");
        };
        var context = new MobileTestContext { Stop = stopHook };
        using var hints = new MauiConnectivityHint(new MobileTestConnectivity());
        await using var session = new MobileSyncSession(context, new MobileTestLifecycle(), hints);
        var stop = session.SuspendAsync(CancellationToken.None).AsTask();
        await entered.Task.WaitAsync(WaitTimeout);
        var resume = session.ResumeAsync(CancellationToken.None).AsTask();
        release.SetResult();
        await Assert.That(() => stop).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => resume).ThrowsExactly<InvalidOperationException>();
        await Assert.That(context.Starts).IsEqualTo(0);
        context.Stop = null;
        await session.ResumeAsync(CancellationToken.None);
        await Assert.That(context.Starts).IsEqualTo(1);
        await Assert.That(context.Triggers).IsEqualTo(1);
    }

    /// <summary>Creates a deterministic asynchronous barrier.</summary>
    /// <returns>The barrier.</returns>
    private static TaskCompletionSource NewCompletion() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
