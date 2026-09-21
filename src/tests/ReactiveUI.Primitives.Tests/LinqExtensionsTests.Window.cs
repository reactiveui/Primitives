// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the <c>Window</c> overloads.</summary>
public partial class LinqExtensionsTests
{
    /// <summary>Windowing by count hands out consecutive windows.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Window_Count_HandsOutConsecutiveWindows() =>
        await Assert.That(SliceLog(static s => s.Window(WindowSize), static s => PushThenComplete(s, "a", "b", "c"))).IsEqualTo(CountWindowsLog);

    /// <summary>Windowing by count and skip drops the values in the gap.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Window_CountAndSkip_DropsTheValuesInTheGap() =>
        await Assert.That(SliceLog(static s => s.Window(WindowSize, GapSkip), static s => PushThenComplete(s, "a", "b", "c", "d", "e"))).IsEqualTo(GapWindowsLog);

    /// <summary>Windowing by time on the default scheduler builds a time signal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Window_TimeSpan_BuildsATimeSignal() =>
        await Assert.That(Signal.None<string>().Window(TimeSpan.FromSeconds(DefaultSequencerSpanSeconds))).IsTypeOf<SliceTimeSignal<string>>();

    /// <summary>Windowing by time on a scheduler ends each window after the duration.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Window_TimeSpanAndScheduler_EndsEachWindowAfterTheDuration() =>
        await Assert.That(TimedLog(static (s, clock) => s.Window(Ms(SpanMilliseconds), clock))).IsEqualTo(TwoWindowsLog);

    /// <summary>Windowing by time and shift on the default scheduler builds a time signal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Window_TimeSpanAndShift_BuildsATimeSignal() =>
        await Assert.That(Signal.None<string>().Window(TimeSpan.FromSeconds(DefaultSequencerSpanSeconds), TimeSpan.FromSeconds(DefaultSequencerShiftSeconds)))
            .IsTypeOf<SliceTimeSignal<string>>();

    /// <summary>Windowing by time and shift on a scheduler opens a window every shift.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Window_TimeSpanShiftAndScheduler_OpensAWindowEveryShift() =>
        await Assert.That(TimedLog(static (s, clock) => s.Window(Ms(SpanMilliseconds), Ms(SpanMilliseconds), clock))).IsEqualTo(TwoWindowsLog);

    /// <summary>Windowing by time and count on the default scheduler builds a time and count signal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Window_TimeSpanAndCount_BuildsATimeCountSignal() =>
        await Assert.That(Signal.None<string>().Window(TimeSpan.FromSeconds(DefaultSequencerSpanSeconds), WindowSize)).IsTypeOf<SliceTimeCountSignal<string>>();

    /// <summary>Windowing by time and count on a scheduler ends a window at whichever limit comes first.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Window_TimeSpanCountAndScheduler_EndsAWindowAtWhicheverLimitComesFirst() =>
        await Assert.That(TimedLog(static (s, clock) => s.Window(Ms(SpanMilliseconds), WindowSize, clock))).IsEqualTo(TwoWindowsLog);

    /// <summary>Windowing by a boundary signal starts the next window on each emission.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Window_Boundary_StartsTheNextWindowOnEachEmission()
    {
        Signal<int> boundary = new();

        var log = SliceLog(s => s.Window(boundary), s =>
        {
            s.OnNext("a");
            boundary.OnNext(0);
            s.OnNext("b");
            s.OnCompleted();
        });

        await Assert.That(log).IsEqualTo(TwoWindowsLog);
    }

    /// <summary>Windowing with a closing selector ends each window with its closing signal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Window_ClosingSelector_EndsEachWindowWithItsClosingSignal()
    {
        List<Signal<int>> closers = [];

        var log = SliceLog(
            s => s.Window(() => NewCloser(closers)),
            s =>
            {
                s.OnNext("a");
                closers[0].OnNext(0);
                s.OnNext("b");
                s.OnCompleted();
            });

        await Assert.That(log).IsEqualTo(TwoWindowsLog);
    }

    /// <summary>Windowing with openings and a closing selector opens windows on demand.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Window_OpeningsAndClosingSelector_OpensWindowsOnDemand()
    {
        Signal<int> openings = new();
        Signal<int> closer = new();

        var log = SliceLog(
            s => s.Window(openings, _ => closer),
            s =>
            {
                openings.OnNext(0);
                s.OnNext("a");
                closer.OnNext(0);
                s.OnNext("b");
            });

        await Assert.That(log).IsEqualTo("open w0 w0:a w0:done");
    }

    /// <summary>A null source, argument or non-positive size is rejected by every <c>Window</c> overload.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Window_InvalidArguments_Throw()
    {
        var source = Signal.None<string>();
        VirtualClock clock = new();
        var span = Ms(SpanMilliseconds);

        await Assert.That(static () => ((IObservable<string>)null!).Window(WindowSize)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => source.Window(0)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => source.Window(WindowSize, 0)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => source.Window(TimeSpan.Zero)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => source.Window(span, (ISequencer)null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => source.Window(span, TimeSpan.Zero)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => source.Window(span, span, (ISequencer)null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => source.Window(span, 0)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => source.Window(span, WindowSize, (ISequencer)null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => source.Window(span, 0, clock)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => source.Window((IObservable<string>)null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => source.Window((Func<IObservable<string>>)null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => source.Window(source, (Func<string, IObservable<string>>)null!)).ThrowsExactly<ArgumentNullException>();
    }
}
