// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="FoldWitness{TSource, TAccumulate}"/> and <see cref="FoldSignal{TSource, TAccumulate}"/>.</summary>
public sealed class FoldWitnessTests
{
    /// <summary>Second element value.</summary>
    private const int Two = 2;

    /// <summary>Third element value.</summary>
    private const int Three = 3;

    /// <summary>Live Scan/Fold routes non-fatal accumulator exceptions to OnError, disposes upstream, and stops.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Scan_LiveAccumulatorThrows_RoutesToOnErrorDisposesUpstreamAndStops()
    {
        var disposals = 0;
        IObserver<int>? upstream = null;
        var source = Signal.Create<int>(obs =>
        {
            upstream = obs;
            return new ActionDisposable(() => disposals++);
        });

        InvalidOperationException boom = new("scan-boom");
        RecordingWitness<int> recorder = new();
        _ = source.Scan(0, (acc, x) => x == Two ? throw boom : acc + x).Subscribe(recorder);

        upstream!.OnNext(1);
        upstream.OnNext(Two);
        upstream.OnNext(Three);
        upstream.OnCompleted();
        upstream.OnError(new InvalidOperationException("late"));

        await Assert.That(disposals).IsEqualTo(1);
        await Assert.That(recorder.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(recorder.Errors.SequenceEqual([boom])).IsTrue();
        await Assert.That(recorder.Completed).IsEqualTo(0);

        FoldWitness<int, int> stoppedSink = new(recorder, 0, static (a, b) => a + b);
        stoppedSink.Dispose();
        stoppedSink.OnNext(1);
        await Assert.That(recorder.Values.SequenceEqual([1])).IsTrue();
    }

    /// <summary>Downstream observer exceptions and fatal accumulator exceptions propagate to the producer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Scan_DownstreamObserverAndFatalExceptions_PropagateToProducer()
    {
        var disposals = 0;
        IObserver<int>? upstream = null;
        var source = Signal.Create<int>(obs =>
        {
            upstream = obs;
            return new ActionDisposable(() => disposals++);
        });

        InvalidOperationException observerBoom = new("observer-boom");
        _ = source.Scan(0, static (acc, x) => acc + x).Subscribe(_ => throw observerBoom);

        await Assert.That(() => upstream!.OnNext(1)).Throws<InvalidOperationException>();
        await Assert.That(disposals).IsEqualTo(1);

        using Signal<int> fatalSource = new();
        RecordingWitness<int> recorder = new();
        using var sub = fatalSource.Fold(0, static (_, _) => throw new BadImageFormatException()).Subscribe(recorder);
        await Assert.That(() => fatalSource.OnNext(1)).Throws<BadImageFormatException>();
    }

    /// <summary>Synchronous sequence and enumerable fast paths route accumulator exceptions to OnError.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Scan_SequenceAndEnumerableFastPaths_RouteCallbackFaultsToOnError()
    {
        InvalidOperationException boom = new("fast-scan");
        RecordingWitness<int> rangeRecorder = new();
        _ = Signal.Sequence(1, Three).Scan(0, (acc, x) => x == Two ? throw boom : acc + x).Subscribe(rangeRecorder);

        await Assert.That(rangeRecorder.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(rangeRecorder.Errors.SequenceEqual([boom])).IsTrue();
        await Assert.That(rangeRecorder.Completed).IsEqualTo(0);

        RecordingWitness<int> listRecorder = new();
        _ = Signal.FromEnumerable([1, Two]).Fold(0, (acc, x) => x == Two ? throw boom : acc + x).Subscribe(listRecorder);

        await Assert.That(listRecorder.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(listRecorder.Errors.SequenceEqual([boom])).IsTrue();
        await Assert.That(listRecorder.Completed).IsEqualTo(0);
    }
}
