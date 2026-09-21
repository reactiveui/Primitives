// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the <c>Slice</c> overloads.</summary>
public partial class LinqExtensionsTests
{
    /// <summary>The duration of each window, in milliseconds.</summary>
    private const int SpanMilliseconds = 10;

    /// <summary>The number of values in each count window.</summary>
    private const int WindowSize = 2;

    /// <summary>The number of values between the starts of count windows that leave a gap.</summary>
    private const int GapSkip = 3;

    /// <summary>The duration of each window that the default-sequencer overloads are given, in seconds.</summary>
    private const int DefaultSequencerSpanSeconds = 1;

    /// <summary>The time between window starts that the default-sequencer overloads are given, in seconds.</summary>
    private const int DefaultSequencerShiftSeconds = 2;

    /// <summary>The log of two consecutive windows that hold a value each and end with the source.</summary>
    private const string TwoWindowsLog = "open w0 w0:a w0:done open w1 w1:b w1:done outer:done";

    /// <summary>The log of consecutive windows of <see cref="WindowSize"/> values over "a", "b" and "c".</summary>
    private const string CountWindowsLog = "open w0 w0:a w0:b w0:done open w1 w1:c w1:done outer:done";

    /// <summary>The log of windows of <see cref="WindowSize"/> values with a skip of <see cref="GapSkip"/> over "a" to "e".</summary>
    private const string GapWindowsLog = "open w0 w0:a w0:b w0:done open w1 w1:d w1:e w1:done outer:done";

    /// <summary>Slicing by count hands out consecutive windows.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Slice_Count_HandsOutConsecutiveWindows() =>
        await Assert.That(SliceLog(static s => s.Slice(WindowSize), static s => PushThenComplete(s, "a", "b", "c"))).IsEqualTo(CountWindowsLog);

    /// <summary>Slicing by count and skip drops the values in the gap.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Slice_CountAndSkip_DropsTheValuesInTheGap() =>
        await Assert.That(SliceLog(static s => s.Slice(WindowSize, GapSkip), static s => PushThenComplete(s, "a", "b", "c", "d", "e"))).IsEqualTo(GapWindowsLog);

    /// <summary>Slicing by time on the default sequencer builds a time signal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Slice_TimeSpan_BuildsATimeSignal() =>
        await Assert.That(Signal.None<string>().Slice(TimeSpan.FromSeconds(DefaultSequencerSpanSeconds))).IsTypeOf<SliceTimeSignal<string>>();

    /// <summary>Slicing by time on a sequencer ends each window after the duration.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Slice_TimeSpanAndSequencer_EndsEachWindowAfterTheDuration() =>
        await Assert.That(TimedLog(static (s, clock) => s.Slice(Ms(SpanMilliseconds), clock))).IsEqualTo(TwoWindowsLog);

    /// <summary>Slicing by time and shift on the default sequencer builds a time signal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Slice_TimeSpanAndShift_BuildsATimeSignal() =>
        await Assert.That(Signal.None<string>().Slice(TimeSpan.FromSeconds(DefaultSequencerSpanSeconds), TimeSpan.FromSeconds(DefaultSequencerShiftSeconds)))
            .IsTypeOf<SliceTimeSignal<string>>();

    /// <summary>Slicing by time and shift on a sequencer opens a window every shift.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Slice_TimeSpanShiftAndSequencer_OpensAWindowEveryShift() =>
        await Assert.That(TimedLog(static (s, clock) => s.Slice(Ms(SpanMilliseconds), Ms(SpanMilliseconds), clock))).IsEqualTo(TwoWindowsLog);

    /// <summary>Slicing by time and count on the default sequencer builds a time and count signal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Slice_TimeSpanAndCount_BuildsATimeCountSignal() =>
        await Assert.That(Signal.None<string>().Slice(TimeSpan.FromSeconds(DefaultSequencerSpanSeconds), WindowSize)).IsTypeOf<SliceTimeCountSignal<string>>();

    /// <summary>Slicing by time and count on a sequencer ends a window at whichever limit comes first.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Slice_TimeSpanCountAndSequencer_EndsAWindowAtWhicheverLimitComesFirst() =>
        await Assert.That(TimedLog(static (s, clock) => s.Slice(Ms(SpanMilliseconds), WindowSize, clock))).IsEqualTo(TwoWindowsLog);

    /// <summary>Slicing by a boundary signal starts the next window on each emission.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Slice_Boundary_StartsTheNextWindowOnEachEmission()
    {
        Signal<int> boundary = new();

        var log = SliceLog(s => s.Slice(boundary), s =>
        {
            s.OnNext("a");
            boundary.OnNext(0);
            s.OnNext("b");
            s.OnCompleted();
        });

        await Assert.That(log).IsEqualTo(TwoWindowsLog);
    }

    /// <summary>Slicing with a closing selector ends each window with its closing signal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Slice_ClosingSelector_EndsEachWindowWithItsClosingSignal()
    {
        List<Signal<int>> closers = [];

        var log = SliceLog(
            s => s.Slice(() => NewCloser(closers)),
            s =>
            {
                s.OnNext("a");
                closers[0].OnNext(0);
                s.OnNext("b");
                s.OnCompleted();
            });

        await Assert.That(log).IsEqualTo(TwoWindowsLog);
    }

    /// <summary>Slicing with openings and a closing selector opens windows on demand.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Slice_OpeningsAndClosingSelector_OpensWindowsOnDemand()
    {
        Signal<int> openings = new();
        Signal<int> closer = new();

        var log = SliceLog(
            s => s.Slice(openings, _ => closer),
            s =>
            {
                openings.OnNext(0);
                s.OnNext("a");
                closer.OnNext(0);
                s.OnNext("b");
            });

        await Assert.That(log).IsEqualTo("open w0 w0:a w0:done");
    }

    /// <summary>A null source, argument or non-positive size is rejected by every <c>Slice</c> overload.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Slice_InvalidArguments_Throw()
    {
        var source = Signal.None<string>();
        VirtualClock clock = new();
        var span = Ms(SpanMilliseconds);

        await Assert.That(static () => ((IObservable<string>)null!).Slice(WindowSize)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => source.Slice(0)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => source.Slice(WindowSize, 0)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => source.Slice(TimeSpan.Zero)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => source.Slice(span, (ISequencer)null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => source.Slice(span, TimeSpan.Zero)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => source.Slice(span, span, (ISequencer)null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => source.Slice(span, 0)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => source.Slice(span, WindowSize, (ISequencer)null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => source.Slice(span, 0, clock)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => source.Slice((IObservable<string>)null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => source.Slice((Func<IObservable<string>>)null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => source.Slice(source, (Func<string, IObservable<string>>)null!)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Creates a duration in milliseconds.</summary>
    /// <param name="milliseconds">The number of milliseconds.</param>
    /// <returns>The duration.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static TimeSpan Ms(int milliseconds) => TimeSpan.FromMilliseconds(milliseconds);

    /// <summary>Creates a closing signal and records it.</summary>
    /// <param name="closers">The recorded closing signals.</param>
    /// <returns>The closing signal.</returns>
    private static Signal<int> NewCloser(List<Signal<int>> closers)
    {
        Signal<int> closer = new();
        closers.Add(closer);
        return closer;
    }

    /// <summary>Pushes values into a source and completes it.</summary>
    /// <param name="source">The source.</param>
    /// <param name="values">The values to push.</param>
    private static void PushThenComplete(Signal<string> source, params string[] values)
    {
        foreach (var value in values)
        {
            source.OnNext(value);
        }

        source.OnCompleted();
    }

    /// <summary>Subscribes a window recorder to an operator over a subject, drives the subject and returns the log.</summary>
    /// <param name="build">Applies the operator under test.</param>
    /// <param name="drive">Drives the subject.</param>
    /// <returns>The recorded log.</returns>
    private static string SliceLog(Func<Signal<string>, IObservable<IObservable<string>>> build, Action<Signal<string>> drive)
    {
        Signal<string> source = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = build(source).Subscribe(observer);
        drive(source);
        return observer.Text;
    }

    /// <summary>Runs a time-based operator over a subject: "a", one window duration, "b", then completion.</summary>
    /// <param name="build">Applies the operator under test with a virtual clock.</param>
    /// <returns>The recorded log.</returns>
    private static string TimedLog(Func<Signal<string>, VirtualClock, IObservable<IObservable<string>>> build)
    {
        VirtualClock clock = new();
        return SliceLog(
            s => build(s, clock),
            s =>
            {
                s.OnNext("a");
                clock.AdvanceBy(Ms(SpanMilliseconds));
                s.OnNext("b");
                s.OnCompleted();
            });
    }
}
