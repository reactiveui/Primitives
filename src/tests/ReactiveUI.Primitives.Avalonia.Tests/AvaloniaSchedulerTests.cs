// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Avalonia.Threading;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Avalonia.Tests;

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
        await Assert.That(scheduler.Now).IsGreaterThan(DateTimeOffset.MinValue);
    }

    /// <summary>Verifies immediate work is posted to and executed on the selected dispatcher thread.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateScheduleExecutesOnDispatcherThread()
    {
        var (dispatcherThreadId, executionThreadId) = await AvaloniaTestSession.Instance.Dispatch(
            static async () =>
            {
                var dispatcherThreadId = Environment.CurrentManagedThreadId;
                AvaloniaScheduler scheduler = new(Dispatcher.UIThread);
                TaskCompletionSource<int> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                scheduler.Schedule(new DelegateWorkItem(() => completion.TrySetResult(Environment.CurrentManagedThreadId)));
                return (dispatcherThreadId, await completion.Task);
            },
            CancellationToken.None);
        await Assert.That(executionThreadId).IsEqualTo(dispatcherThreadId);
    }

    /// <summary>Verifies due work executes on the selected dispatcher.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DueScheduleExecutesOnDispatcherThread()
    {
        var (dispatcherThreadId, executionThreadId, priority) = await AvaloniaTestSession.Instance.Dispatch(
            static async () =>
            {
                var dispatcherThreadId = Environment.CurrentManagedThreadId;
                AvaloniaScheduler scheduler = new(Dispatcher.UIThread, DispatcherPriority.Normal);
                TaskCompletionSource<int> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                scheduler.Schedule(
                    new DelegateWorkItem(() => completion.TrySetResult(Environment.CurrentManagedThreadId)),
                    scheduler.Timestamp);
                return (dispatcherThreadId, await completion.Task, scheduler.Priority);
            },
            CancellationToken.None);
        await Assert.That(executionThreadId).IsEqualTo(dispatcherThreadId);
        await Assert.That(priority).IsEqualTo(DispatcherPriority.Normal);
    }

    /// <summary>Verifies the sequencer validates both work-item overloads.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduleRejectsNullWorkItems()
    {
        var scheduler = await AvaloniaTestSession.Instance.Dispatch(
            static () => new AvaloniaScheduler(Dispatcher.UIThread),
            CancellationToken.None);
        await Assert.That(() => scheduler.Schedule(null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => scheduler.Schedule(null!, scheduler.Timestamp)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Work item backed by an action.</summary>
    /// <param name="action">Action to invoke.</param>
    private sealed class DelegateWorkItem(Action action) : IWorkItem
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute() => action();
    }
}
