// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Tests.Internal;

/// <summary>Tests for <see cref="ScheduledDrainState{T}"/>.</summary>
public sealed class ScheduledDrainStateTests
{
    /// <summary>Verifies disposing before an upstream subscription is attached marks the sink done without anything to release.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_BeforeAttach_MarksDone()
    {
        var state = CreateState();

        state.Dispose();
        state.Dispose();

        await Assert.That(state.Done).IsTrue();
    }

    /// <summary>Verifies disposing after attach releases the upstream subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_AfterAttach_ReleasesUpstream()
    {
        var state = CreateState();
        CountingDisposable upstream = new();
        state.Attach(upstream);

        state.Dispose();

        await Assert.That(upstream.DisposeCount).IsEqualTo(1);
        await Assert.That(state.Done).IsTrue();
    }

    /// <summary>Verifies a second disposal request after the sink has finished hands back nothing to release.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task BeginDispose_AfterDone_ReturnsNull()
    {
        var state = CreateState();
        state.Attach(new CountingDisposable());

        var first = state.BeginDispose();
        var second = state.BeginDispose();

        await Assert.That(first).IsNotNull();
        await Assert.That(second).IsNull();
    }

    /// <summary>Creates a drain state on the immediate sequencer with a no-op drain target.</summary>
    /// <returns>The drain state.</returns>
    private static ScheduledDrainState<int> CreateState() => new(ImmediateSequencer.Instance, new NoOpDrainTarget(), new());

    /// <summary>A drain target that does nothing.</summary>
    private sealed class NoOpDrainTarget : IDrainTarget
    {
        /// <inheritdoc/>
        public void Drain()
        {
        }
    }

    /// <summary>Counts how many times it is disposed.</summary>
    private sealed class CountingDisposable : IDisposable
    {
        /// <summary>Gets the number of dispose calls.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => DisposeCount++;
    }
}
