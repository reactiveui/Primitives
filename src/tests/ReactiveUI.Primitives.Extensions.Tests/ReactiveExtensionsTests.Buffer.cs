// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for ReactiveExtensionsTests.</summary>
public partial class ReactiveExtensionsTests
{
    /// <summary>Tests BufferUntil with character delimiters.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task BufferUntil_WithStartAndEndChars_BuffersCorrectly()
    {
        using Subject<char> subject = new();
        List<string> results = [];
        using var sub = subject.BufferUntil('<', '>').Subscribe(results.Add);
        subject.OnNext('a');
        subject.OnNext('<');
        subject.OnNext('t');
        subject.OnNext('e');
        subject.OnNext('s');
        subject.OnNext('t');
        subject.OnNext('>');
        subject.OnNext('b');
        subject.OnNext('<');
        subject.OnNext('d');
        subject.OnNext('a');
        subject.OnNext('t');
        subject.OnNext('a');
        subject.OnNext('>');
        subject.OnCompleted();
        using (Assert.Multiple())
        {
            await Assert.That(results).Count().IsEqualTo(SampleValue2);
            await Assert.That(results[0]).IsEqualTo("<test>");
            await Assert.That(results[1]).IsEqualTo("<data>");
        }
    }

    /// <summary>Exercises <c>BufferUntilObservable</c>'s <c>OnError</c> forwarding —
    /// the observer hands the source's error straight to the downstream subscriber.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBufferUntilSourceErrors_ThenForwardsError()
    {
        using Subject<char> subject = new();
        Exception? caught = null;
        InvalidOperationException expected = new("buffer-until-error");
        using var sub = subject.BufferUntil('<', '>').Subscribe(
            static _ => { },
            ex => caught = ex);
        subject.OnNext('a');
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Tests BufferUntil emits remaining buffered content when the source completes before the end delimiter.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task BufferUntil_WhenSourceCompletesWithPartialBuffer_EmitsRemainingContent()
    {
        using Subject<char> subject = new();
        List<string> results = [];
        var completed = false;
        using var sub = subject.BufferUntil('<', '>').Subscribe(results.Add, () => completed = true);
        subject.OnNext('x');
        subject.OnNext('<');
        subject.OnNext('a');
        subject.OnNext('b');
        subject.OnCompleted();
        using (Assert.Multiple())
        {
            await Assert.That(results).Count().IsEqualTo(1);
            await Assert.That(results[0]).IsEqualTo("<ab");
            await Assert.That(completed).IsTrue();
        }
    }

    /// <summary>Tests Conflate with minimum update period.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Conflate_WithMinimumPeriod_DelaysUpdates()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        using var sub = subject.Conflate(TimeSpan.FromTicks(100), scheduler).Subscribe(results.Add);
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        subject.OnNext(SampleValue3);
        scheduler.AdvanceBy(SchedulerWindowTicks + 1);
        using (Assert.Multiple())
        {
            await Assert.That(results).Count().IsEqualTo(1);
            await Assert.That(results[0]).IsEqualTo(SampleValue3);
        }
    }

    /// <summary>Tests Conflate completes after a pending delayed update has been emitted.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Conflate_WithPendingDelayedUpdate_CompletesAfterFlush()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        var completed = false;
        using var sub = subject.Conflate(TimeSpan.FromTicks(100), scheduler)
            .Subscribe(results.Add, () => completed = true);
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        subject.OnCompleted();
        await Assert.That(completed).IsFalse();
        scheduler.AdvanceBy(SchedulerWindowTicks + 1);
        using (Assert.Multiple())
        {
            await Assert.That(results).IsCollectionEqualTo([SampleValue2]);
            await Assert.That(completed).IsTrue();
        }
    }

    /// <summary>Tests BufferUntilIdle buffers values until idle period.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task BufferUntilIdle_BuffersUntilIdle()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<IList<int>> results = [];
        _ = subject.BufferUntilIdle(TimeSpan.FromMilliseconds(100), scheduler).Subscribe(results.Add);
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        scheduler.AdvanceBy(TimeSpan.FromMilliseconds(50).Ticks);
        subject.OnNext(SampleValue3);
        scheduler.AdvanceBy(TimeSpan.FromMilliseconds(150).Ticks); // Wait for idle period
        await Assert.That(results).Count().IsEqualTo(1);
        await Assert.That(results[0]).IsCollectionEqualTo([1, SampleValue2, SampleValue3]);
    }

    /// <summary>Tests BufferUntilIdle with scheduler forwards source error after flushing buffered items.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBufferUntilIdleWithSchedulerSourceErrors_ThenFlushesAndForwardsError()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<IList<int>> results = [];
        Exception? observedError = null;
        _ = subject.BufferUntilIdle(TimeSpan.FromTicks(100), scheduler).Subscribe(results.Add, ex => observedError = ex);
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        subject.OnError(new InvalidOperationException("test error"));
        using (Assert.Multiple())
        {
            await Assert.That(results).Count().IsEqualTo(1);
            await Assert.That(results[0]).IsCollectionEqualTo([1, SampleValue2]);
            await Assert.That(observedError).IsNotNull();
        }
    }

    /// <summary>Tests BufferUntilIdle with scheduler flushes buffered items on source completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBufferUntilIdleWithSchedulerSourceCompletes_ThenFlushesAndCompletes()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<IList<int>> results = [];
        var completed = false;
        _ = subject.BufferUntilIdle(TimeSpan.FromTicks(100), scheduler).Subscribe(results.Add, () => completed = true);
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        subject.OnCompleted();
        using (Assert.Multiple())
        {
            await Assert.That(results).Count().IsEqualTo(1);
            await Assert.That(results[0]).IsCollectionEqualTo([1, SampleValue2]);
            await Assert.That(completed).IsTrue();
        }
    }

    /// <summary>Tests BufferUntilIdle without scheduler uses the Publish+Buffer+Throttle path.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBufferUntilIdleCalledWithoutScheduler_ThenBuffersUntilIdle()
    {
        Subject<int> subject = new();
        List<IList<int>> results = [];
        _ = subject.BufferUntilIdle(TimeSpan.FromMilliseconds(100)).Subscribe(results.Add);
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        subject.OnNext(SampleValue3);
        subject.OnCompleted();
        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
    }

    /// <summary>Tests BufferUntilInactive buffers items and flushes after inactivity period.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBufferUntilInactive_ThenBuffersAndFlushesOnInactivity()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<IList<int>> results = [];
        _ = subject.BufferUntilInactive(TimeSpan.FromTicks(100), scheduler).Subscribe(results.Add);
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        scheduler.AdvanceBy(SchedulerHalfWindowTicks);
        subject.OnNext(SampleValue3);
        scheduler.AdvanceBy(SchedulerAdvancePastWindowTicks);
        await Assert.That(results).Count().IsEqualTo(1);
        await Assert.That(results[0]).IsCollectionEqualTo([1, SampleValue2, SampleValue3]);
    }

    /// <summary>Tests BufferUntilInactive flushes remaining items on error.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBufferUntilInactiveSourceErrors_ThenFlushesAndForwardsError()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<IList<int>> results = [];
        Exception? observedError = null;
        _ = subject.BufferUntilInactive(TimeSpan.FromTicks(100), scheduler)
            .Subscribe(results.Add, ex => observedError = ex);
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        subject.OnError(new InvalidOperationException("test"));
        using (Assert.Multiple())
        {
            await Assert.That(results).Count().IsEqualTo(1);
            await Assert.That(results[0]).IsCollectionEqualTo([1, SampleValue2]);
            await Assert.That(observedError).IsNotNull();
        }
    }

    /// <summary>Tests BufferUntilInactive flushes remaining items on completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBufferUntilInactiveSourceCompletes_ThenFlushesAndCompletes()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<IList<int>> results = [];
        var completed = false;
        _ = subject.BufferUntilInactive(TimeSpan.FromTicks(100), scheduler).Subscribe(results.Add, () => completed = true);
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        subject.OnCompleted();
        using (Assert.Multiple())
        {
            await Assert.That(results).Count().IsEqualTo(1);
            await Assert.That(results[0]).IsCollectionEqualTo([1, SampleValue2]);
            await Assert.That(completed).IsTrue();
        }
    }

    /// <summary>Tests Conflate non-throttled path emits immediately when update period has passed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConflateNonThrottledPath_ThenEmitsImmediately()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        var completed = false;
        _ = subject.Conflate(TimeSpan.FromTicks(100), scheduler).Subscribe(
            results.Add,
            _ => { },
            () => completed = true);

        // Emit first value - goes through non-throttled path (line 353)
        subject.OnNext(1);
        scheduler.AdvanceBy(1);

        // Advance past the minimum update period
        scheduler.AdvanceBy(SettleDelayMilliseconds);

        // Emit second value - also non-throttled since enough time passed
        subject.OnNext(SampleValue2);
        scheduler.AdvanceBy(1);

        // Complete with no scheduled update pending (line 372)
        subject.OnCompleted();
        scheduler.AdvanceBy(1);
        await Assert.That(results).IsCollectionEqualTo([1, SampleValue2]);
        await Assert.That(completed).IsTrue();
    }
}
