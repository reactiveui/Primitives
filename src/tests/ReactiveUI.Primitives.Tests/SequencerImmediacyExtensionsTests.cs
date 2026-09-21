// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests <see cref="SequencerImmediacyExtensions"/> immediacy detection.</summary>
public class SequencerImmediacyExtensionsTests
{
    /// <summary>The immediate sequencer reports itself as immediate.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task IsImmediateIsTrueForTheImmediateSequencer() =>
        await Assert.That(Sequencer.Immediate.IsImmediate).IsTrue();

    /// <summary>The immediate sequencer reports itself as immediate when held as the sequencer interface.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task IsImmediateIsTrueForTheImmediateSequencerBehindTheInterface()
    {
        ISequencer sequencer = ImmediateSequencer.Instance;

        await Assert.That(sequencer.IsImmediate).IsTrue();
    }

    /// <summary>The current-thread sequencer is not immediate.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task IsImmediateIsFalseForTheCurrentThreadSequencer() =>
        await Assert.That(Sequencer.CurrentThread.IsImmediate).IsFalse();

    /// <summary>The thread-pool sequencer is not immediate.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task IsImmediateIsFalseForTheThreadPoolSequencer() =>
        await Assert.That(ThreadPoolSequencer.Instance.IsImmediate).IsFalse();

    /// <summary>The task-pool sequencer is not immediate.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task IsImmediateIsFalseForTheTaskPoolSequencer() =>
        await Assert.That(TaskPoolSequencer.Instance.IsImmediate).IsFalse();

    /// <summary>The default sequencer is not immediate.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task IsImmediateIsFalseForTheDefaultSequencer() =>
        await Assert.That(Sequencer.Default.IsImmediate).IsFalse();

    /// <summary>A virtual clock is not immediate.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task IsImmediateIsFalseForAVirtualClock()
    {
        VirtualClock clock = new();

        await Assert.That(clock.IsImmediate).IsFalse();
    }

    /// <summary>A null sequencer is not immediate.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task IsImmediateIsFalseForNull()
    {
        ISequencer? sequencer = null;

        await Assert.That(sequencer.IsImmediate).IsFalse();
    }
}
