// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using ReactiveUI.Primitives.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Reactive.Tests;

/// <summary>Tests <see cref="SequencerImmediacyExtensions"/> immediacy detection against System.Reactive schedulers.</summary>
public class SequencerImmediacyExtensionsTests
{
    /// <summary>The immediate scheduler instance reports itself as immediate.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSchedulerIsImmediateInstance_ThenIsImmediateIsTrue() =>
        await Assert.That(ImmediateScheduler.Instance.IsImmediate).IsTrue();

    /// <summary>The scheduler exposed as the immediate scheduler reports itself as immediate.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSchedulerIsSchedulerImmediate_ThenIsImmediateIsTrue() =>
        await Assert.That(System.Reactive.Concurrency.Scheduler.Immediate.IsImmediate).IsTrue();

    /// <summary>The library's immediate sequencer reports itself as immediate.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSchedulerIsSequencerImmediate_ThenIsImmediateIsTrue() =>
        await Assert.That(Sequencer.Immediate.IsImmediate).IsTrue();

    /// <summary>The current-thread scheduler is not immediate.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSchedulerIsCurrentThread_ThenIsImmediateIsFalse() =>
        await Assert.That(System.Reactive.Concurrency.Scheduler.CurrentThread.IsImmediate).IsFalse();

    /// <summary>The default scheduler is not immediate.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSchedulerIsDefault_ThenIsImmediateIsFalse() =>
        await Assert.That(System.Reactive.Concurrency.Scheduler.Default.IsImmediate).IsFalse();

    /// <summary>The thread-pool sequencer is not immediate.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSchedulerIsThreadPool_ThenIsImmediateIsFalse() =>
        await Assert.That(ThreadPoolSequencer.Instance.IsImmediate).IsFalse();

    /// <summary>A virtual-time scheduler is not immediate.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSchedulerIsVirtualTime_ThenIsImmediateIsFalse()
    {
        HistoricalScheduler scheduler = new();

        await Assert.That(scheduler.IsImmediate).IsFalse();
    }

    /// <summary>A null scheduler is not immediate.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSchedulerIsNull_ThenIsImmediateIsFalse()
    {
        IScheduler? scheduler = null;

        await Assert.That(scheduler.IsImmediate).IsFalse();
    }
}
