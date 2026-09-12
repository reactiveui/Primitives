// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using Avalonia.Threading;
using ReactiveUI.Primitives.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Avalonia.Reactive.Tests;

/// <summary>Tests for <see cref="AvaloniaScheduler"/> against a pumped Avalonia headless dispatcher.</summary>
public sealed class AvaloniaSchedulerTests
{
    /// <summary>Delay used to exercise the native dispatcher-timer path.</summary>
    private static readonly TimeSpan DelayedDueTime = TimeSpan.FromMilliseconds(50);

    /// <summary>Delay used for work that is cancelled before its timer fires.</summary>
    private static readonly TimeSpan CancellationDueTime = TimeSpan.FromMilliseconds(100);

    /// <summary>Delay for work that falls due after the cancelled work, on the same dispatcher and priority.</summary>
    private static readonly TimeSpan FollowingDueTime = TimeSpan.FromMilliseconds(200);

    /// <summary>Verifies constructor validation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullDispatcher() =>
        await Assert.That(static () => new AvaloniaScheduler(null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>Verifies the singleton uses Avalonia's UI dispatcher and legacy background priority.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task InstanceUsesUiDispatcherAndBackgroundPriority() =>
        await AvaloniaTestSession.Instance.Dispatch(
            static async () =>
            {
                await Assert.That(AvaloniaScheduler.Instance.Dispatcher).IsSameReferenceAs(Dispatcher.UIThread);
                await Assert.That(AvaloniaScheduler.Instance.Priority).IsEqualTo(DispatcherPriority.Background);
                await Assert.That(AvaloniaScheduler.Instance).IsSameReferenceAs(AvaloniaScheduler.Instance);
            },
            CancellationToken.None);

    /// <summary>Verifies immediate scheduler work is posted to and executed on the selected dispatcher thread.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateScheduleExecutesOnDispatcherThread()
    {
        // The session only awaits a dispatched delegate that returns a result, so the facts to assert come back
        // out of the dispatch; an assertion left inside it past the first await is never observed.
        var (dispatcherThreadId, executionThreadId) = await AvaloniaTestSession.Instance.Dispatch(
            static async () =>
            {
                var dispatcherThreadId = Environment.CurrentManagedThreadId;
                AvaloniaScheduler scheduler = new(Dispatcher.UIThread);
                TaskCompletionSource<int> completion =
                    new(TaskCreationOptions.RunContinuationsAsynchronously);

                _ = scheduler.Schedule(
                    () => completion.TrySetResult(Environment.CurrentManagedThreadId));

                return (DispatcherThreadId: dispatcherThreadId, ExecutionThreadId: await completion.Task);
            },
            CancellationToken.None);

        await Assert.That(executionThreadId).IsEqualTo(dispatcherThreadId);
    }

    /// <summary>Verifies delayed scheduler work runs on a timer bound to the selected dispatcher.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedScheduleExecutesOnDispatcherThread()
    {
        var (dispatcherThreadId, executionThreadId, priority) = await AvaloniaTestSession.Instance.Dispatch(
            static async () =>
            {
                var dispatcherThreadId = Environment.CurrentManagedThreadId;
                AvaloniaScheduler scheduler = new(Dispatcher.UIThread, DispatcherPriority.Normal);
                TaskCompletionSource<int> completion =
                    new(TaskCreationOptions.RunContinuationsAsynchronously);

                _ = scheduler.Schedule(
                    DelayedDueTime,
                    () => completion.TrySetResult(Environment.CurrentManagedThreadId));

                return (
                    DispatcherThreadId: dispatcherThreadId,
                    ExecutionThreadId: await completion.Task,
                    scheduler.Priority);
            },
            CancellationToken.None);

        await Assert.That(executionThreadId).IsEqualTo(dispatcherThreadId);
        await Assert.That(priority).IsEqualTo(DispatcherPriority.Normal);
    }

    /// <summary>Verifies disposing delayed work stops its dispatcher timer before execution.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedScheduleCanBeCancelled()
    {
        var executed = await AvaloniaTestSession.Instance.Dispatch(
            static async () =>
            {
                AvaloniaScheduler scheduler = new(Dispatcher.UIThread);
                TaskCompletionSource following = new(TaskCreationOptions.RunContinuationsAsynchronously);
                var executed = false;

                var disposable = scheduler.Schedule(
                    CancellationDueTime,
                    () => executed = true);
                disposable.Dispose();

                // The following work is due after the cancelled work and shares its dispatcher and priority, so the
                // dispatcher passes the cancelled due time first: the follower running means the cancelled action was
                // skipped rather than merely still pending.
                _ = scheduler.Schedule(FollowingDueTime, following.SetResult);

                await following.Task;
                return executed;
            },
            CancellationToken.None);

        await Assert.That(executed).IsFalse();
    }
}
