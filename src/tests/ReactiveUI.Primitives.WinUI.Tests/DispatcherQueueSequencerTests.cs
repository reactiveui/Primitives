// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.UI.Dispatching;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.WinUI.Tests;

/// <summary>Tests dispatcher execution on a dedicated WinUI queue thread.</summary>
public sealed class DispatcherQueueSequencerTests
{
    /// <summary>Verifies the constructor rejects a null dispatcher queue.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullDispatcherQueue() =>
        await Assert.That(static () => new DispatcherQueueSequencer(null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>Verifies the clock uses UTC and debugger text identifies the sequencer.</summary>
    /// <returns>A task representing the asynchronous test operation.</returns>
    [Test]
    public async Task ClockUsesUtcAndDebuggerTextIdentifiesSequencer()
    {
        await using var harness = new DispatcherQueueHarness();
        DispatcherQueueSequencer sequencer = new(harness.DispatcherQueue);
        await Assert.That(sequencer.Now.Offset).IsEqualTo(TimeSpan.Zero);
        await Assert.That(sequencer.DebuggerDisplay).IsEqualTo(typeof(DispatcherQueueSequencer).FullName);
    }

    /// <summary>Verifies a stopped dispatcher queue rejects scheduled work.</summary>
    /// <returns>A task representing the asynchronous test operation.</returns>
    [Test]
    public async Task SchedulingAfterQueueShutdownThrows()
    {
        var harness = new DispatcherQueueHarness();
        DispatcherQueueSequencer sequencer = new(harness.DispatcherQueue);
        await harness.DisposeAsync();
        var executed = false;
        await Assert.That(() => sequencer.Schedule(new DelegateWorkItem(() => executed = true)))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => sequencer.Schedule(new DelegateWorkItem(() => executed = true)))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(executed).IsFalse();
    }

    /// <summary>Verifies immediate work is enqueued to and executed on the dispatcher-queue thread.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateScheduleExecutesOnQueueThread()
    {
        await using var harness = new DispatcherQueueHarness();
        var sequencer = new DispatcherQueueSequencer(harness.DispatcherQueue);
        var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        sequencer.Schedule(new DelegateWorkItem(() => completion.TrySetResult(harness.DispatcherQueue.HasThreadAccess)));

        var ranOnQueueThread = await completion.Task;
        await Assert.That(ranOnQueueThread).IsTrue();
    }

    /// <summary>Verifies due work executes on the dispatcher queue thread.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DueScheduleExecutesOnQueueThread()
    {
        await using var harness = new DispatcherQueueHarness();
        var sequencer = new DispatcherQueueSequencer(harness.DispatcherQueue);
        var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var due = sequencer.Timestamp;
        sequencer.Schedule(new DelegateWorkItem(() => completion.TrySetResult(harness.DispatcherQueue.HasThreadAccess)), due);

        var ranOnQueueThread = await completion.Task;
        await Assert.That(ranOnQueueThread).IsTrue();
    }

    /// <summary>Work item that invokes a delegate when executed.</summary>
    private sealed class DelegateWorkItem : IWorkItem
    {
        /// <summary>The action to run on execution.</summary>
        private readonly Action _action;

        /// <summary>Initializes a new instance of the <see cref="DelegateWorkItem"/> class.</summary>
        /// <param name="action">The action to run on execution.</param>
        public DelegateWorkItem(Action action) => _action = action;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute() => _action();
    }

    /// <summary>Hosts a WinUI <see cref="DispatcherQueue"/> on a dedicated thread and shuts the queue down on disposal.</summary>
    private sealed class DispatcherQueueHarness : IAsyncDisposable
    {
        /// <summary>The controller owning the dedicated dispatcher-queue thread.</summary>
        private readonly DispatcherQueueController _controller;

        /// <summary>Initializes a new instance of the <see cref="DispatcherQueueHarness"/> class.</summary>
        public DispatcherQueueHarness()
        {
            _controller = DispatcherQueueController.CreateOnDedicatedThread();
            DispatcherQueue = _controller.DispatcherQueue;
        }

        /// <summary>Gets the hosted dispatcher queue.</summary>
        public DispatcherQueue DispatcherQueue { get; }

        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async ValueTask DisposeAsync() => await _controller.ShutdownQueueAsync().AsTask();
    }
}
