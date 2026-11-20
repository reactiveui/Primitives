// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="TapSignal{T}"/> and <see cref="TapWitness{T}"/>.</summary>
public sealed class TapSignalTests
{
    /// <summary>Second element value.</summary>
    private const int Two = 2;

    /// <summary>Third element value.</summary>
    private const int Three = 3;

    /// <summary>Live Do/Tap routes throwing value callbacks to OnError, disposes upstream, and suppresses later items.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Do_LiveOnNextThrows_RoutesToOnErrorDisposesUpstreamAndStops()
    {
        var nextDisposals = 0;
        IObserver<int>? nextUpstream = null;
        var nextSource = Signal.Create<int>(obs =>
        {
            nextUpstream = obs;
            return new ActionDisposable(() => nextDisposals++);
        });

        InvalidOperationException nextBoom = new("tap-next");
        RecordingWitness<int> nextRecorder = new();
        _ = nextSource.Do(x =>
        {
            if (x == Two)
            {
                throw nextBoom;
            }
        }).Subscribe(nextRecorder);

        nextUpstream!.OnNext(1);
        nextUpstream.OnNext(Two);
        nextUpstream.OnNext(Three);
        nextUpstream.OnCompleted();

        await Assert.That(nextDisposals).IsEqualTo(1);
        await Assert.That(nextRecorder.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(nextRecorder.Errors.SequenceEqual([nextBoom])).IsTrue();
        await Assert.That(nextRecorder.Completed).IsEqualTo(0);
    }

    /// <summary>Live Do/Tap routes throwing error and completion callbacks to OnError and disposes upstream.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Do_LiveOnErrorAndOnCompletedThrow_RouteToOnErrorAndStop()
    {
        var errDisposals = 0;
        IObserver<int>? errUpstream = null;
        var errSource = Signal.Create<int>(obs =>
        {
            errUpstream = obs;
            return new ActionDisposable(() => errDisposals++);
        });

        InvalidOperationException errBoom = new("tap-error");
        RecordingWitness<int> errRecorder = new();
        _ = new TapSignal<int>(errSource, static _ => { }, _ => throw errBoom).Subscribe(errRecorder);
        errUpstream!.OnError(new InvalidOperationException("original"));
        errUpstream.OnCompleted();

        await Assert.That(errDisposals).IsEqualTo(1);
        await Assert.That(errRecorder.Errors.SequenceEqual([errBoom])).IsTrue();

        var doneDisposals = 0;
        IObserver<int>? doneUpstream = null;
        var doneSource = Signal.Create<int>(obs =>
        {
            doneUpstream = obs;
            return new ActionDisposable(() => doneDisposals++);
        });

        InvalidOperationException doneBoom = new("tap-completed");
        RecordingWitness<int> doneRecorder = new();
        _ = doneSource.Do(static _ => { }, () => throw doneBoom).Subscribe(doneRecorder);
        doneUpstream!.OnCompleted();
        doneUpstream.OnNext(1);

        await Assert.That(doneDisposals).IsEqualTo(1);
        await Assert.That(doneRecorder.Errors.SequenceEqual([doneBoom])).IsTrue();
        await Assert.That(doneRecorder.Completed).IsEqualTo(0);
    }

    /// <summary>Downstream observer exceptions propagate to the producer while Range and FromEnumerable fast paths guard callbacks.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Do_DownstreamObserverPropagatesAndFastPathsGuardCallbacks()
    {
        var disposals = 0;
        IObserver<int>? upstream = null;
        var source = Signal.Create<int>(obs =>
        {
            upstream = obs;
            return new ActionDisposable(() => disposals++);
        });

        InvalidOperationException observerBoom = new("observer-boom");
        _ = source.Do(static _ => { }).Subscribe(_ => throw observerBoom);
        await Assert.That(() => upstream!.OnNext(1)).Throws<InvalidOperationException>();
        await Assert.That(disposals).IsEqualTo(1);

        InvalidOperationException fastBoom = new("fast-tap");
        RecordingWitness<int> rangeNextErr = new();
        _ = Signal.Sequence(1, Three).Do(x =>
        {
            if (x == Two)
            {
                throw fastBoom;
            }
        }).Subscribe(rangeNextErr);
        await Assert.That(rangeNextErr.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(rangeNextErr.Errors.SequenceEqual([fastBoom])).IsTrue();

        RecordingWitness<int> rangeDoneErr = new();
        _ = new TapSignal<int>(Signal.Sequence(1, Two), static _ => { }, () => throw fastBoom).Subscribe(rangeDoneErr);
        await Assert.That(rangeDoneErr.Values.SequenceEqual([1, Two])).IsTrue();
        await Assert.That(rangeDoneErr.Errors.SequenceEqual([fastBoom])).IsTrue();
        await Assert.That(rangeDoneErr.Completed).IsEqualTo(0);

        RecordingWitness<int> listDoneErr = new();
        _ = Signal.FromEnumerable([1, Two]).Do(static _ => { }, () => throw fastBoom).Subscribe(listDoneErr);
        await Assert.That(listDoneErr.Values.SequenceEqual([1, Two])).IsTrue();
        await Assert.That(listDoneErr.Errors.SequenceEqual([fastBoom])).IsTrue();
        await Assert.That(listDoneErr.Completed).IsEqualTo(0);
    }
}
