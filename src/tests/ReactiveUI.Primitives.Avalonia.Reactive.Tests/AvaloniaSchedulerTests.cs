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
    /// <summary>Verifies constructor validation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullDispatcher() =>
        await Assert.That(static () => new AvaloniaScheduler(null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>Verifies the singleton uses Avalonia's UI dispatcher and legacy background priority.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task InstanceUsesUiDispatcherAndBackgroundPriority()
    {
        var (scheduler, dispatcher) = await AvaloniaTestSession.Instance.Dispatch(
            static () => (AvaloniaScheduler.Instance, Dispatcher.UIThread),
            CancellationToken.None);

        await Assert.That(scheduler.Dispatcher).IsSameReferenceAs(dispatcher);
        await Assert.That(scheduler.Priority).IsEqualTo(DispatcherPriority.Background);
        await Assert.That(scheduler).IsSameReferenceAs(AvaloniaScheduler.Instance);
    }

    /// <summary>Verifies immediate scheduler work is posted to and executed on the selected dispatcher thread.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateScheduleExecutesOnDispatcherThread()
    {
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

    /// <summary>Verifies due work runs on the selected dispatcher at the configured priority.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DueScheduleExecutesOnDispatcherThread()
    {
        var (dispatcherThreadId, executionThreadId, priority) = await AvaloniaTestSession.Instance.Dispatch(
            static async () =>
            {
                var dispatcherThreadId = Environment.CurrentManagedThreadId;
                AvaloniaScheduler scheduler = new(Dispatcher.UIThread, DispatcherPriority.Normal);
                TaskCompletionSource<int> completion =
                    new(TaskCreationOptions.RunContinuationsAsynchronously);

                _ = scheduler.Schedule(
                    TimeSpan.Zero,
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

    /// <summary>Verifies disposing queued work prevents execution.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DueScheduleCanBeCancelled()
    {
        var executed = await AvaloniaTestSession.Instance.Dispatch(
            static async () =>
            {
                AvaloniaScheduler scheduler = new(Dispatcher.UIThread);
                TaskCompletionSource following = new(TaskCreationOptions.RunContinuationsAsynchronously);
                var executed = false;

                var disposable = scheduler.Schedule(
                    TimeSpan.Zero,
                    () => executed = true);
                disposable.Dispose();

                _ = scheduler.Schedule(TimeSpan.Zero, following.SetResult);

                await following.Task;
                return executed;
            },
            CancellationToken.None);

        await Assert.That(executed).IsFalse();
    }
}
