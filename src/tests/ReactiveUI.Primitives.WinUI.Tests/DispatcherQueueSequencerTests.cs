// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using Microsoft.UI.Dispatching;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.WinUI.Tests;

/// <summary>
/// Tests for <see cref="DispatcherQueueSequencer"/>, exercised against a real WinUI
/// <see cref="DispatcherQueue"/> running on a dedicated thread so both the immediate and timer-based
/// dispatch paths run end to end. Compiled only on Windows builds (see the csproj); the API approval test
/// runs everywhere.
/// </summary>
public sealed class DispatcherQueueSequencerTests
{
    /// <summary>
    /// Maximum time to wait for work to be marshalled onto the dispatcher-queue thread before failing.
    /// </summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Verifies the constructor rejects a null dispatcher queue.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullDispatcherQueue() =>
        await Assert.That(() => new DispatcherQueueSequencer(null!)).Throws<ArgumentNullException>();

    /// <summary>
    /// Verifies immediate work is enqueued to and executed on the dispatcher-queue thread.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateScheduleExecutesOnQueueThread()
    {
        await using var harness = new DispatcherQueueHarness();
        var sequencer = new DispatcherQueueSequencer(harness.DispatcherQueue);
        var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        sequencer.Schedule(new DelegateWorkItem(() => completion.TrySetResult(harness.DispatcherQueue.HasThreadAccess)));

        var ranOnQueueThread = await completion.Task.WaitAsync(WaitTimeout);
        await Assert.That(ranOnQueueThread).IsTrue();
    }

    /// <summary>
    /// Verifies work due in the future is executed on the dispatcher-queue thread via the queue timer.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedScheduleExecutesOnQueueThread()
    {
        await using var harness = new DispatcherQueueHarness();
        var sequencer = new DispatcherQueueSequencer(harness.DispatcherQueue);
        var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var due = sequencer.Timestamp + (Stopwatch.Frequency / 20); // ~50 ms into the future.
        sequencer.Schedule(new DelegateWorkItem(() => completion.TrySetResult(harness.DispatcherQueue.HasThreadAccess)), due);

        var ranOnQueueThread = await completion.Task.WaitAsync(WaitTimeout);
        await Assert.That(ranOnQueueThread).IsTrue();
    }

    /// <summary>
    /// Work item that invokes a delegate when executed.
    /// </summary>
    private sealed class DelegateWorkItem : IWorkItem
    {
        /// <summary>
        /// The action to run on execution.
        /// </summary>
        private readonly Action _action;

        /// <summary>
        /// Initializes a new instance of the <see cref="DelegateWorkItem"/> class.
        /// </summary>
        /// <param name="action">The action to run on execution.</param>
        public DelegateWorkItem(Action action) => _action = action;

        /// <inheritdoc/>
        public void Execute() => _action();
    }

    /// <summary>
    /// Hosts a WinUI <see cref="DispatcherQueue"/> on a dedicated thread and shuts the queue down on disposal.
    /// </summary>
    private sealed class DispatcherQueueHarness : IAsyncDisposable
    {
        /// <summary>
        /// The controller owning the dedicated dispatcher-queue thread.
        /// </summary>
        private readonly DispatcherQueueController _controller;

        /// <summary>
        /// Initializes a new instance of the <see cref="DispatcherQueueHarness"/> class.
        /// </summary>
        public DispatcherQueueHarness()
        {
            _controller = DispatcherQueueController.CreateOnDedicatedThread();
            DispatcherQueue = _controller.DispatcherQueue;
        }

        /// <summary>
        /// Gets the hosted dispatcher queue.
        /// </summary>
        public DispatcherQueue DispatcherQueue { get; }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync() => await _controller.ShutdownQueueAsync().AsTask().WaitAsync(WaitTimeout);
    }
}
