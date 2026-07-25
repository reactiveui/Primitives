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
    /// <summary>Maximum time to wait for dispatcher work.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Delay used to exercise the native dispatcher-timer path.</summary>
    private static readonly TimeSpan DelayedDueTime = TimeSpan.FromMilliseconds(50);

    /// <summary>Delay used for work that is cancelled before its timer fires.</summary>
    private static readonly TimeSpan CancellationDueTime = TimeSpan.FromMilliseconds(100);

    /// <summary>Time allowed to prove cancelled dispatcher work remains inactive.</summary>
    private static readonly TimeSpan CancellationWaitTime = TimeSpan.FromMilliseconds(150);

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
    public async Task ImmediateScheduleExecutesOnDispatcherThread() =>
        await AvaloniaTestSession.Instance.Dispatch(
            static async () =>
            {
                var dispatcherThreadId = Environment.CurrentManagedThreadId;
                AvaloniaScheduler scheduler = new(Dispatcher.UIThread);
                TaskCompletionSource<int> completion =
                    new(TaskCreationOptions.RunContinuationsAsynchronously);

                _ = scheduler.Schedule(
                    () => completion.TrySetResult(Environment.CurrentManagedThreadId));

                var executionThreadId = await completion.Task.WaitAsync(WaitTimeout);
                await Assert.That(executionThreadId).IsEqualTo(dispatcherThreadId);
            },
            CancellationToken.None);

    /// <summary>Verifies delayed scheduler work runs on a timer bound to the selected dispatcher.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedScheduleExecutesOnDispatcherThread() =>
        await AvaloniaTestSession.Instance.Dispatch(
            static async () =>
            {
                var dispatcherThreadId = Environment.CurrentManagedThreadId;
                AvaloniaScheduler scheduler = new(Dispatcher.UIThread, DispatcherPriority.Normal);
                TaskCompletionSource<int> completion =
                    new(TaskCreationOptions.RunContinuationsAsynchronously);

                _ = scheduler.Schedule(
                    DelayedDueTime,
                    () => completion.TrySetResult(Environment.CurrentManagedThreadId));

                var executionThreadId = await completion.Task.WaitAsync(WaitTimeout);
                await Assert.That(executionThreadId).IsEqualTo(dispatcherThreadId);
                await Assert.That(scheduler.Priority).IsEqualTo(DispatcherPriority.Normal);
            },
            CancellationToken.None);

    /// <summary>Verifies disposing delayed work stops its dispatcher timer before execution.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedScheduleCanBeCancelled() =>
        await AvaloniaTestSession.Instance.Dispatch(
            static async () =>
            {
                AvaloniaScheduler scheduler = new(Dispatcher.UIThread);
                var executed = false;

                var disposable = scheduler.Schedule(
                    CancellationDueTime,
                    () => executed = true);
                disposable.Dispose();

                await Task.Delay(CancellationWaitTime);
                await Assert.That(executed).IsFalse();
            },
            CancellationToken.None);
}
