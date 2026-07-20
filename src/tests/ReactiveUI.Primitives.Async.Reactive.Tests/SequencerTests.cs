// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Async.Reactive.Tests;

/// <summary>Verifies the async leaf's sequencers map onto the matching System.Reactive scheduler singletons.</summary>
public class SequencerTests
{
    /// <summary>A negative interval, which normalization clamps to zero.</summary>
    private static readonly TimeSpan NegativeInterval = TimeSpan.FromSeconds(-5);

    /// <summary>A positive interval, which normalization leaves untouched.</summary>
    private static readonly TimeSpan PositiveInterval = TimeSpan.FromSeconds(5);

    /// <summary>Verifies the current-thread sequencer is System.Reactive's current-thread scheduler.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCurrentThreadRead_ThenItIsTheCurrentThreadScheduler() =>
        await Assert.That(Sequencer.CurrentThread)
            .IsSameReferenceAs(System.Reactive.Concurrency.CurrentThreadScheduler.Instance);

    /// <summary>Verifies the immediate sequencer is System.Reactive's immediate scheduler.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenImmediateRead_ThenItIsTheImmediateScheduler() =>
        await Assert.That(Sequencer.Immediate)
            .IsSameReferenceAs(System.Reactive.Concurrency.ImmediateScheduler.Instance);

    /// <summary>Verifies the default sequencer is System.Reactive's task-pool scheduler.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDefaultRead_ThenItIsTheTaskPoolScheduler() =>
        await Assert.That(Sequencer.Default)
            .IsSameReferenceAs(System.Reactive.Concurrency.TaskPoolScheduler.Default);

    /// <summary>Verifies a negative interval normalizes to zero.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenNegativeIntervalNormalized_ThenItBecomesZero() =>
        await Assert.That(Sequencer.Normalize(NegativeInterval)).IsEqualTo(TimeSpan.Zero);

    /// <summary>Verifies a positive interval survives normalization unchanged.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPositiveIntervalNormalized_ThenItIsUnchanged() =>
        await Assert.That(Sequencer.Normalize(PositiveInterval)).IsEqualTo(PositiveInterval);
}
