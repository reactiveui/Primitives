// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="UniqueSignal{T}"/> and <see cref="UniqueWitness{T}"/>.</summary>
public sealed class UniqueSignalTests
{
    /// <summary>Second element value.</summary>
    private const int Two = 2;

    /// <summary>Third element value.</summary>
    private const int Three = 3;

    /// <summary>Live DistinctUntilChanged/Unique routes non-fatal comparer exceptions to OnError, disposes upstream, and stops.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DistinctUntilChanged_LiveComparerThrows_RoutesToOnErrorDisposesUpstreamAndStops()
    {
        var disposals = 0;
        IObserver<int>? upstream = null;
        var source = Signal.Create<int>(obs =>
        {
            upstream = obs;
            return new ActionDisposable(() => disposals++);
        });

        InvalidOperationException boom = new("comparer-boom");
        RecordingWitness<int> recorder = new();
        _ = source.DistinctUntilChanged(new ThrowingComparer(boom)).Subscribe(recorder);

        upstream!.OnNext(1);
        upstream.OnNext(Two);
        upstream.OnNext(Three);
        upstream.OnCompleted();
        upstream.OnError(new InvalidOperationException("late"));

        await Assert.That(disposals).IsEqualTo(1);
        await Assert.That(recorder.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(recorder.Errors.SequenceEqual([boom])).IsTrue();
        await Assert.That(recorder.Completed).IsEqualTo(0);

        UniqueWitness<int> stoppedSink = new(recorder, EqualityComparer<int>.Default);
        stoppedSink.Dispose();
        stoppedSink.OnNext(Two);
        await Assert.That(recorder.Values.SequenceEqual([1])).IsTrue();
    }

    /// <summary>Downstream observer exceptions, fatal comparer exceptions, and FromEnumerable comparer faults behave per contract.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Unique_DownstreamFatalAndListFastPath_FollowObserverAndErrorContract()
    {
        var disposals = 0;
        IObserver<int>? upstream = null;
        var source = Signal.Create<int>(obs =>
        {
            upstream = obs;
            return new ActionDisposable(() => disposals++);
        });

        InvalidOperationException observerBoom = new("observer-boom");
        _ = source.Unique(EqualityComparer<int>.Default).Subscribe(_ => throw observerBoom);
        await Assert.That(() => upstream!.OnNext(1)).Throws<InvalidOperationException>();
        await Assert.That(disposals).IsEqualTo(1);

        using Signal<int> fatalLive = new();
        RecordingWitness<int> fatalRecorder = new();
        using var sub = fatalLive.Unique(new ThrowingComparer(new BadImageFormatException())).Subscribe(fatalRecorder);
        fatalLive.OnNext(1);
        await Assert.That(() => fatalLive.OnNext(Two)).Throws<BadImageFormatException>();

        RecordingWitness<int> listOk = new();
        _ = Signal.FromEnumerable([1, 1, Two, Two, Three]).Unique(EqualityComparer<int>.Default).Subscribe(listOk);
        await Assert.That(listOk.Values.SequenceEqual([1, Two, Three])).IsTrue();
        await Assert.That(listOk.Completed).IsEqualTo(1);

        InvalidOperationException listBoom = new("list-comparer");
        RecordingWitness<int> listErr = new();
        _ = Signal.FromEnumerable([1, Two, Three]).DistinctUntilChanged(new ThrowingComparer(listBoom)).Subscribe(listErr);
        await Assert.That(listErr.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(listErr.Errors.SequenceEqual([listBoom])).IsTrue();
        await Assert.That(listErr.Completed).IsEqualTo(0);

        await Assert.That(() =>
            Signal.FromEnumerable([1, Two])
                .Unique(new ThrowingComparer(new BadImageFormatException()))
                .Subscribe(listErr))
            .Throws<BadImageFormatException>();
    }

    /// <summary>Equality comparer that throws the configured exception when comparing values.</summary>
    /// <param name="error">The exception to throw.</param>
    private sealed class ThrowingComparer(Exception error) : IEqualityComparer<int>
    {
        /// <inheritdoc/>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public bool Equals(int x, int y) => throw error;

        /// <inheritdoc/>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(int obj) => obj;
    }
}
