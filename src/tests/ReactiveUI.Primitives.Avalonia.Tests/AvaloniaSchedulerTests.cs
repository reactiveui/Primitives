// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using Avalonia.Threading;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Avalonia.Tests;

/// <summary>Tests for <see cref="AvaloniaScheduler"/> against a pumped Avalonia headless dispatcher.</summary>
public sealed class AvaloniaSchedulerTests
{
    /// <summary>Maximum time to wait for dispatcher work.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Future scheduling delay in stopwatch ticks.</summary>
    private static readonly long ScheduleDelayTicks = Stopwatch.Frequency / 20;

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
                await Assert.That(AvaloniaScheduler.Instance.Now).IsGreaterThan(DateTimeOffset.MinValue);
            },
            CancellationToken.None);

    /// <summary>Verifies immediate work is posted to and executed on the selected dispatcher thread.</summary>
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

                scheduler.Schedule(
                    new DelegateWorkItem(
                        () => completion.TrySetResult(Environment.CurrentManagedThreadId)));

                var executionThreadId = await completion.Task.WaitAsync(WaitTimeout);
                await Assert.That(executionThreadId).IsEqualTo(dispatcherThreadId);
            },
            CancellationToken.None);

    /// <summary>Verifies future work runs through a timer bound to the selected dispatcher.</summary>
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

                scheduler.Schedule(
                    new DelegateWorkItem(
                        () => completion.TrySetResult(Environment.CurrentManagedThreadId)),
                    scheduler.Timestamp + ScheduleDelayTicks);

                var executionThreadId = await completion.Task.WaitAsync(WaitTimeout);
                await Assert.That(executionThreadId).IsEqualTo(dispatcherThreadId);
                await Assert.That(scheduler.Priority).IsEqualTo(DispatcherPriority.Normal);
            },
            CancellationToken.None);

    /// <summary>Verifies the sequencer validates both work-item overloads.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduleRejectsNullWorkItems() =>
        await AvaloniaTestSession.Instance.Dispatch(
            static async () =>
            {
                AvaloniaScheduler scheduler = new(Dispatcher.UIThread);

                await Assert.That(() => scheduler.Schedule(null!)).ThrowsExactly<ArgumentNullException>();
                await Assert.That(() => scheduler.Schedule(null!, scheduler.Timestamp)).ThrowsExactly<ArgumentNullException>();
            },
            CancellationToken.None);

    /// <summary>Work item backed by an action.</summary>
    /// <param name="action">Action to invoke.</param>
    private sealed class DelegateWorkItem(Action action) : IWorkItem
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute() => action();
    }
}
