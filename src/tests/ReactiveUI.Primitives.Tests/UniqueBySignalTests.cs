// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="UniqueBySignal{T, TKey}"/> and <see cref="UniqueByWitness{T, TKey}"/>.</summary>
public sealed class UniqueBySignalTests
{
    /// <summary>Second element value.</summary>
    private const int Two = 2;

    /// <summary>Third element value.</summary>
    private const int Three = 3;

    /// <summary>Fourth element value.</summary>
    private const int Four = 4;

    /// <summary>A throwing key selector on a single-element Emit source is invoked and routed to OnError.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_SingleItemEmitWithThrowingKeySelector_ForwardsToOnError()
    {
        InvalidOperationException expected = new("key-fault");
        RecordingWitness<int> observer = new();
        RecordingWitness<int> rxObserver = new();

        _ = Signal.Emit(1).UniqueBy<int, int>(_ => throw expected).Subscribe(observer);
        _ = Signal.Emit(1).DistinctUntilChangedBy<int, int>(_ => throw expected).Subscribe(rxObserver);

        await Assert.That(observer.Errors.SequenceEqual([expected])).IsTrue();
        await Assert.That(rxObserver.Errors.SequenceEqual([expected])).IsTrue();
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(rxObserver.Values).IsEmpty();
    }

    /// <summary>Live witness catches non-fatal key selector and comparer exceptions, disposes upstream, and suppresses later events.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task LiveWitness_CallbackThrows_RoutesToOnErrorDisposesUpstreamAndStops()
    {
        var keyDisposals = 0;
        IObserver<int>? keyUpstream = null;
        var keySource = Signal.Create<int>(obs =>
        {
            keyUpstream = obs;
            return new ActionDisposable(() => keyDisposals++);
        });

        InvalidOperationException keyBoom = new("key-boom");
        RecordingWitness<int> keyRecorder = new();
        _ = keySource.DistinctUntilChangedBy(x => x == Two ? throw keyBoom : x).Subscribe(keyRecorder);

        keyUpstream!.OnNext(1);
        keyUpstream.OnNext(Two);
        keyUpstream.OnNext(Three);
        keyUpstream.OnCompleted();

        await Assert.That(keyDisposals).IsEqualTo(1);
        await Assert.That(keyRecorder.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(keyRecorder.Errors.SequenceEqual([keyBoom])).IsTrue();
        await Assert.That(keyRecorder.Completed).IsEqualTo(0);

        var cmpDisposals = 0;
        IObserver<int>? cmpUpstream = null;
        var cmpSource = Signal.Create<int>(obs =>
        {
            cmpUpstream = obs;
            return new ActionDisposable(() => cmpDisposals++);
        });

        InvalidOperationException cmpBoom = new("cmp-boom");
        RecordingWitness<int> cmpRecorder = new();
        _ = cmpSource.UniqueBy(static x => x, new ThrowingIntComparer(cmpBoom)).Subscribe(cmpRecorder);

        cmpUpstream!.OnNext(1);
        cmpUpstream.OnNext(1);
        cmpUpstream.OnError(new InvalidOperationException("late"));

        await Assert.That(cmpDisposals).IsEqualTo(1);
        await Assert.That(cmpRecorder.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(cmpRecorder.Errors.SequenceEqual([cmpBoom])).IsTrue();

        UniqueByWitness<int, int> stoppedSink = new(cmpRecorder, static x => x, EqualityComparer<int>.Default);
        stoppedSink.Dispose();
        stoppedSink.OnNext(Two);
        await Assert.That(cmpRecorder.Values.SequenceEqual([1])).IsTrue();
    }

    /// <summary>Downstream observer exceptions and fatal callback exceptions propagate to the producer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task LiveWitness_DownstreamObserverAndFatalExceptions_PropagateToProducer()
    {
        var disposals = 0;
        IObserver<int>? upstream = null;
        var source = Signal.Create<int>(obs =>
        {
            upstream = obs;
            return new ActionDisposable(() => disposals++);
        });

        InvalidOperationException observerBoom = new("observer-boom");
        _ = source.DistinctUntilChangedBy(static x => x).Subscribe(_ => throw observerBoom);

        await Assert.That(() => upstream!.OnNext(1)).Throws<InvalidOperationException>();
        await Assert.That(disposals).IsEqualTo(1);

        using Signal<int> fatalLive = new();
        RecordingWitness<int> fatalRecorder = new();
        using var sub = fatalLive.UniqueBy<int, int>(static _ => throw new BadImageFormatException()).Subscribe(fatalRecorder);
        await Assert.That(() => fatalLive.OnNext(1)).Throws<BadImageFormatException>();
    }

    /// <summary>Range and FromEnumerable fast paths filter duplicates and route non-fatal callback faults to OnError.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task RangeAndListFastPaths_FilterDuplicatesAndRouteCallbackFaults()
    {
        RecordingWitness<int> rangeOk = new();
        _ = Signal.Sequence(0, Four).UniqueBy(static x => x / Two).Subscribe(rangeOk);
        await Assert.That(rangeOk.Values.SequenceEqual([0, Two])).IsTrue();
        await Assert.That(rangeOk.Completed).IsEqualTo(1);

        RecordingWitness<int> listOk = new();
        _ = Signal.FromEnumerable([1, 1, Two, Three]).DistinctUntilChangedBy(static x => x).Subscribe(listOk);
        await Assert.That(listOk.Values.SequenceEqual([1, Two, Three])).IsTrue();
        await Assert.That(listOk.Completed).IsEqualTo(1);

        InvalidOperationException boom = new("fast-boom");
        RecordingWitness<int> rangeErr = new();
        _ = Signal.Sequence(1, Three).UniqueBy(x => x == Two ? throw boom : x).Subscribe(rangeErr);
        await Assert.That(rangeErr.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(rangeErr.Errors.SequenceEqual([boom])).IsTrue();
        await Assert.That(rangeErr.Completed).IsEqualTo(0);

        RecordingWitness<int> listErr = new();
        _ = Signal.FromEnumerable([1, Two]).UniqueBy(static x => x, new ThrowingIntComparer(boom)).Subscribe(listErr);
        await Assert.That(listErr.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(listErr.Errors.SequenceEqual([boom])).IsTrue();
        await Assert.That(listErr.Completed).IsEqualTo(0);

        await Assert.That(() =>
            Signal.FromEnumerable([1]).UniqueBy<int, int>(static _ => throw new BadImageFormatException()).Subscribe(listErr))
            .Throws<BadImageFormatException>();
    }

    /// <summary>Equality comparer that throws the configured exception when comparing values.</summary>
    /// <param name="error">The exception to throw.</param>
    private sealed class ThrowingIntComparer(Exception error) : IEqualityComparer<int>
    {
        /// <inheritdoc/>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public bool Equals(int x, int y) => throw error;

        /// <inheritdoc/>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(int obj) => obj;
    }
}
