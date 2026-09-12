// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests throttle delivery after replacement, termination, and disposal.</summary>
public class ThrottleObservableTests
{
    /// <summary>Tick window for advancing past the throttle in settle assertions.</summary>
    private const int SettleTicks = 100;

    /// <summary>Tick window for the throttle itself.</summary>
    private const int ThrottleTicks = 10;

    /// <summary>Verifies stale, duplicate, and disposed throttle callbacks cannot emit.</summary>
    /// <returns>A task representing the asynchronous test operation.</returns>
    [Test]
    public async Task ThrottleCallbacksEmitOnlyTheCurrentValueOnce()
    {
        const int First = 1;
        const int Latest = 2;
        const int Disposed = 3;
        List<int> values = [];
        VirtualClock scheduler = new();
        ThrottleObservable<int>.ThrottleSink sink = new(
            Observer.Create<int>(values.Add),
            TimeSpan.FromTicks(1),
            scheduler);
        sink.OnNext(First);
        sink.OnNext(Latest);
        sink.Emit(First);
        await Assert.That(values).IsEmpty();
        sink.Emit(Latest);
        sink.Emit(Latest);
        sink.OnNext(Disposed);
        sink.Dispose();
        sink.Emit(Disposed);
        await Assert.That(values).IsCollectionEqualTo([Latest]);
    }

    /// <summary>Verifies that an <c>OnNext</c> arriving after the source has already completed is silently dropped by the throttle sink.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnNextAfterCompleted_ThenDropped()
    {
        VirtualClock scheduler = new();
        SyncDirectSource<int> source = new();
        List<int> values = [];
        var completed = false;
        using var sub = source.ThrottleOnScheduler(TimeSpan.FromTicks(ThrottleTicks), scheduler)
            .Subscribe(values.Add, () => completed = true);
        source.Observer.OnCompleted();
        scheduler.AdvanceBy(SettleTicks);
        source.Observer.OnNext(1);
        scheduler.AdvanceBy(SettleTicks);
        await Assert.That(completed).IsTrue();
        await Assert.That(values).IsEmpty();
    }

    /// <summary>Verifies that an <c>OnError</c> arriving after the source has already completed is silently dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorAfterCompleted_ThenDropped()
    {
        VirtualClock scheduler = new();
        SyncDirectSource<int> source = new();
        Exception? caught = null;
        var completed = false;
        using var sub = source.ThrottleOnScheduler(TimeSpan.FromTicks(ThrottleTicks), scheduler).Subscribe(
            static _ => { },
            ex => caught = ex,
            () => completed = true);
        source.Observer.OnCompleted();
        source.Observer.OnError(new InvalidOperationException("late"));
        await Assert.That(completed).IsTrue();
        await Assert.That(caught).IsNull();
    }

    /// <summary>Verifies that an <c>OnCompleted</c> arriving after a prior <c>OnError</c> is silently dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnCompletedAfterError_ThenDropped()
    {
        VirtualClock scheduler = new();
        SyncDirectSource<int> source = new();
        Exception? caught = null;
        var completed = false;
        InvalidOperationException expected = new("first");
        using var sub = source.ThrottleOnScheduler(TimeSpan.FromTicks(ThrottleTicks), scheduler).Subscribe(
            static _ => { },
            ex => caught = ex,
            () => completed = true);
        source.Observer.OnError(expected);
        source.Observer.OnCompleted();
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(completed).IsFalse();
    }
}
