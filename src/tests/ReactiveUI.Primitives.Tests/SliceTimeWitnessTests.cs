// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the sink that hands out windows of a fixed duration.</summary>
public class SliceTimeWitnessTests
{
    /// <summary>The duration of each window, in milliseconds.</summary>
    private const int SpanMilliseconds = 10;

    /// <summary>A shift equal to the duration hands out consecutive windows.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Tick_ShiftEqualToSpan_HandsOutConsecutiveWindows()
    {
        const int halfSpanMilliseconds = 5;
        Signal<string> source = new();
        VirtualClock clock = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceTimeSignal<string>(source, Ms(SpanMilliseconds), Ms(SpanMilliseconds), clock).Subscribe(observer);

        source.OnNext("a");
        clock.AdvanceBy(Ms(halfSpanMilliseconds));
        source.OnNext("b");
        clock.AdvanceBy(Ms(halfSpanMilliseconds));
        source.OnNext("c");
        clock.AdvanceBy(Ms(halfSpanMilliseconds));
        source.OnNext("d");
        clock.AdvanceBy(Ms(halfSpanMilliseconds));
        source.OnNext("e");
        clock.AdvanceBy(Ms(halfSpanMilliseconds));
        source.OnCompleted();

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:a w0:b w0:done open w1 w1:c w1:d w1:done open w2 w2:e w2:done outer:done");
    }

    /// <summary>A shift shorter than the duration hands out overlapping windows.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Tick_ShiftShorterThanSpan_OverlapsWindows()
    {
        const int shiftMilliseconds = 5;
        Signal<string> source = new();
        VirtualClock clock = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceTimeSignal<string>(source, Ms(SpanMilliseconds), Ms(shiftMilliseconds), clock).Subscribe(observer);

        source.OnNext("a");
        clock.AdvanceBy(Ms(shiftMilliseconds));
        source.OnNext("b");
        clock.AdvanceBy(Ms(shiftMilliseconds));
        source.OnNext("c");
        clock.AdvanceBy(Ms(shiftMilliseconds));
        source.OnNext("d");
        clock.AdvanceBy(Ms(shiftMilliseconds));
        source.OnCompleted();

        await Assert.That(observer.Text).IsEqualTo(
            "open w0 w0:a open w1 w0:b w1:b w0:done open w2 w1:c w2:c w1:done open w3 w2:d w3:d w2:done open w4 w3:done w4:done outer:done");
    }

    /// <summary>A shift longer than the duration leaves a gap in which values reach no window.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Tick_ShiftLongerThanSpan_LeavesAGap()
    {
        const int shiftMilliseconds = 15;
        Signal<string> source = new();
        VirtualClock clock = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceTimeSignal<string>(source, Ms(SpanMilliseconds), Ms(shiftMilliseconds), clock).Subscribe(observer);

        source.OnNext("a");
        clock.AdvanceBy(Ms(SpanMilliseconds));
        source.OnNext("dropped");
        clock.AdvanceBy(Ms(shiftMilliseconds - SpanMilliseconds));
        source.OnNext("b");
        clock.AdvanceBy(Ms(SpanMilliseconds));
        source.OnNext("dropped");
        source.OnCompleted();

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:a w0:done open w1 w1:b w1:done outer:done");
    }

    /// <summary>Source completion cancels the timer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnCompleted_CancelsTheTimer()
    {
        Signal<string> source = new();
        VirtualClock clock = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceTimeSignal<string>(source, Ms(SpanMilliseconds), Ms(SpanMilliseconds), clock).Subscribe(observer);
        source.OnCompleted();

        clock.AdvanceBy(Ms(SpanMilliseconds + SpanMilliseconds));

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:done outer:done");
    }

    /// <summary>A source error faults every open window and the outer sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnError_FaultsOpenWindowsThenTheOuterSequence()
    {
        const int shiftMilliseconds = 5;
        Signal<string> source = new();
        VirtualClock clock = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceTimeSignal<string>(source, Ms(SpanMilliseconds), Ms(shiftMilliseconds), clock).Subscribe(observer);
        clock.AdvanceBy(Ms(shiftMilliseconds));

        source.OnError(new InvalidOperationException("boom"));

        await Assert.That(observer.Text).IsEqualTo("open w0 open w1 w0:error boom w1:error boom outer:error boom");
    }

    /// <summary>Disposing the outer subscription keeps the source and the timer alive while a window is subscribed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_OuterWhileWindowSubscribed_KeepsSourceUntilWindowIsDisposed()
    {
        Signal<string> source = new();
        VirtualClock clock = new();
        WindowRecordingWitness<string> observer = new();
        var outer = new SliceTimeSignal<string>(source, Ms(SpanMilliseconds), Ms(SpanMilliseconds), clock).Subscribe(observer);

        outer.Dispose();
        var aliveAfterOuter = source.HasObservers;
        clock.AdvanceBy(Ms(SpanMilliseconds));
        observer.Subscriptions[0].Dispose();
        clock.AdvanceBy(Ms(SpanMilliseconds));

        await Assert.That(aliveAfterOuter).IsTrue();
        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(observer.Text).IsEqualTo("open w0 w0:done");
    }

    /// <summary>A downstream observer failure tears the sink down.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Tick_OuterObserverThrows_DisposesTheSinkAndRethrows()
    {
        Signal<string> source = new();
        VirtualClock clock = new();
        using var subscription = new SliceTimeSignal<string>(source, Ms(SpanMilliseconds), Ms(SpanMilliseconds), clock).Subscribe(new ThrowAfterFirstWindow());

        await Assert.That(() => clock.AdvanceBy(Ms(SpanMilliseconds))).ThrowsExactly<InvalidOperationException>();
        await Assert.That(source.HasObservers).IsFalse();
    }

    /// <summary>Notifications after the sink is disposed are ignored.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_AfterDispose_IsIgnored()
    {
        VirtualClock clock = new();
        RecordingWitness<IObservable<string>> observer = new();
        SliceTimeWitness<string> witness = new(observer, Ms(SpanMilliseconds), Ms(SpanMilliseconds), clock);
        witness.Start();
        witness.Dispose();

        witness.OnNext("a");
        witness.OnCompleted();
        clock.AdvanceBy(Ms(SpanMilliseconds + SpanMilliseconds));

        await Assert.That(observer.Values.Count).IsEqualTo(1);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>A null argument or a non-positive duration is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_InvalidArguments_Throw()
    {
        VirtualClock clock = new();
        RecordingWitness<IObservable<string>> observer = new();
        var negative = TimeSpan.FromTicks(-1);

        await Assert.That(() => new SliceTimeWitness<string>(null!, Ms(SpanMilliseconds), Ms(SpanMilliseconds), clock)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SliceTimeWitness<string>(observer, Ms(SpanMilliseconds), Ms(SpanMilliseconds), null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SliceTimeWitness<string>(observer, TimeSpan.Zero, Ms(SpanMilliseconds), clock)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => new SliceTimeWitness<string>(observer, Ms(SpanMilliseconds), negative, clock)).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Creates a duration in milliseconds.</summary>
    /// <param name="milliseconds">The number of milliseconds.</param>
    /// <returns>The duration.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static TimeSpan Ms(int milliseconds) => TimeSpan.FromMilliseconds(milliseconds);

    /// <summary>Accepts the first window and throws on the next.</summary>
    private sealed class ThrowAfterFirstWindow : IObserver<IObservable<string>>
    {
        /// <summary>The number of windows received.</summary>
        private int _windows;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted()
        {
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        public void OnNext(IObservable<string> value)
        {
            _windows++;
            if (_windows > 1)
            {
                throw new InvalidOperationException("observer");
            }
        }
    }
}
