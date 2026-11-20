// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;
using CoreKeepSignal = ReactiveUI.Primitives.Signals.KeepSignal<int>;
using CoreMapSignal = ReactiveUI.Primitives.Signals.MapSignal<int, int>;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <c>MapSignal</c>, <c>KeepSignal</c>, and signal disposal idempotency.</summary>
public sealed class MapSignalTests
{
    /// <summary>Second element constant.</summary>
    private const int Two = 2;

    /// <summary>Range length constant.</summary>
    private const int Three = 3;

    /// <summary>Projection scale factor.</summary>
    private const int Ten = 10;

    /// <summary>Projected first item.</summary>
    private const int TenFirst = 10;

    /// <summary>Projected second item.</summary>
    private const int Twenty = 20;

    /// <summary>Verifies <c>MapSignal</c> validates arguments and supports both range and live-source subscriptions.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_NullRangeAndLiveSource_DeliversProjectedValues()
    {
        CoreMapSignal rangeMap = new(Signal.Sequence(1, Two), static x => x * Ten);
        await Assert.That(() => rangeMap.Subscribe(null!)).Throws<ArgumentNullException>();

        RecordingWitness<int> rangeObserver = new();
        var rangeSub = rangeMap.Subscribe(rangeObserver);
        await Assert.That(rangeSub).IsSameReferenceAs(EmptyDisposable.Instance);
        await Assert.That(rangeObserver.Values.SequenceEqual([TenFirst, Twenty])).IsTrue();
        await Assert.That(rangeObserver.Completed).IsEqualTo(1);

        using Signal<int> live = new();
        CoreMapSignal liveMap = new(live, static x => x * Ten);
        RecordingWitness<int> liveObserver = new();
        using (liveMap.Subscribe(liveObserver))
        {
            live.OnNext(Two);
            live.OnCompleted();
        }

        await Assert.That(liveObserver.Values.SequenceEqual([Twenty])).IsTrue();
        await Assert.That(liveObserver.Completed).IsEqualTo(1);

        CoreKeepSignal rangeKeep = new(Signal.Sequence(1, Three), static x => x >= Two);
        RecordingWitness<int> keepObserver = new();
        var keepSub = rangeKeep.Subscribe(keepObserver);
        await Assert.That(keepSub).IsSameReferenceAs(EmptyDisposable.Instance);
        await Assert.That(keepObserver.Values.SequenceEqual([Two, Three])).IsTrue();

        await Assert.That(MapWithSignal<int, int, int>.Create(Signal.Empty<int>(), 1, static (s, x) => s + x))
            .IsSameReferenceAs(Signal.Empty<int>());
        await Assert.That(MapWithSignal<int, int, int>.Create(Signal.Silent<int>(), 1, static (s, x) => s + x))
            .IsSameReferenceAs(Signal.Silent<int>());
    }

    /// <summary>Verifies inner lock-guarded disposal and duplicate handler disposal are idempotent across core signal types.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DisposeCore_AfterDispose_IsIdempotentAcrossSignals()
    {
        Signal<int> signal = new();
        signal.Dispose();
        signal.DisposeCore();
        await Assert.That(signal.IsDisposed).IsTrue();

        AsyncSignal<int> asyncSignal = new();
        asyncSignal.Dispose();
        asyncSignal.DisposeCore();
        await Assert.That(asyncSignal.IsDisposed).IsTrue();

        using StateSignal<int> upstream = new(1);
        var projected = ProjectedReadOnlyState<int, int>.Create(upstream, static x => x);
        projected.Dispose();
        projected.DisposeCore();
        await Assert.That(() => projected.Value).Throws<ObjectDisposedException>();

        ReplaySignal<int> replay = new();
        replay.Dispose();
        replay.DisposeCore();
        await Assert.That(replay.IsDisposed).IsTrue();

        using BehaviorSignal<int> behavior = new(1);
        var sub = behavior.Subscribe(static _ => { });
        sub.Dispose();
        sub.Dispose();
    }
}
