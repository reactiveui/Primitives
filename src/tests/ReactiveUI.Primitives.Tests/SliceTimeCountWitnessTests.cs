// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the sink that hands out windows that end after a duration or a number of values.</summary>
public class SliceTimeCountWitnessTests
{
    /// <summary>The maximum duration of each window, in milliseconds.</summary>
    private const int SpanMilliseconds = 10;

    /// <summary>A full window ends at once and each new window restarts the duration.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_CountReached_EndsTheWindowAndOpensTheNext()
    {
        const int windowSize = 2;
        Signal<string> source = new();
        VirtualClock clock = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceTimeCountSignal<string>(source, Ms(SpanMilliseconds), windowSize, clock).Subscribe(observer);

        source.OnNext("a");
        source.OnNext("b");
        var afterCount = observer.Text;
        clock.AdvanceBy(Ms(SpanMilliseconds));
        source.OnNext("c");
        source.OnCompleted();

        await Assert.That(afterCount).IsEqualTo("open w0 w0:a w0:b w0:done open w1");
        await Assert.That(observer.Text).IsEqualTo("open w0 w0:a w0:b w0:done open w1 w1:done open w2 w2:c w2:done outer:done");
    }

    /// <summary>The duration ends a window that has not reached its count.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Tick_DurationElapses_EndsTheWindowAndStartsTheNext()
    {
        const int windowSize = 5;
        Signal<string> source = new();
        VirtualClock clock = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceTimeCountSignal<string>(source, Ms(SpanMilliseconds), windowSize, clock).Subscribe(observer);

        source.OnNext("a");
        clock.AdvanceBy(Ms(SpanMilliseconds));
        source.OnNext("b");
        clock.AdvanceBy(Ms(SpanMilliseconds));

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:a w0:done open w1 w1:b w1:done open w2");
    }

    /// <summary>The timer of a window that filled up is cancelled, so it cannot end the window after it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_CountReached_CancelsTheEarlierTimer()
    {
        const int windowSize = 2;
        const int halfSpanMilliseconds = 5;
        Signal<string> source = new();
        VirtualClock clock = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceTimeCountSignal<string>(source, Ms(SpanMilliseconds), windowSize, clock).Subscribe(observer);
        clock.AdvanceBy(Ms(halfSpanMilliseconds));
        source.OnNext("a");
        source.OnNext("b");

        clock.AdvanceBy(Ms(halfSpanMilliseconds));
        var atOriginalDeadline = observer.Text;
        clock.AdvanceBy(Ms(halfSpanMilliseconds));

        await Assert.That(atOriginalDeadline).IsEqualTo("open w0 w0:a w0:b w0:done open w1");
        await Assert.That(observer.Text).IsEqualTo("open w0 w0:a w0:b w0:done open w1 w1:done open w2");
    }

    /// <summary>A source error faults the window and the outer sequence and cancels the timer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnError_FaultsTheWindowAndTheOuterSequence()
    {
        const int windowSize = 2;
        Signal<string> source = new();
        VirtualClock clock = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceTimeCountSignal<string>(source, Ms(SpanMilliseconds), windowSize, clock).Subscribe(observer);

        source.OnError(new InvalidOperationException("boom"));
        clock.AdvanceBy(Ms(SpanMilliseconds + SpanMilliseconds));

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:error boom outer:error boom");
    }

    /// <summary>Disposing the outer subscription keeps the source alive while a window is subscribed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_OuterWhileWindowSubscribed_KeepsSourceUntilWindowIsDisposed()
    {
        const int windowSize = 2;
        Signal<string> source = new();
        VirtualClock clock = new();
        WindowRecordingWitness<string> observer = new();
        var outer = new SliceTimeCountSignal<string>(source, Ms(SpanMilliseconds), windowSize, clock).Subscribe(observer);

        outer.Dispose();
        var aliveAfterOuter = source.HasObservers;
        source.OnNext("a");
        observer.Subscriptions[0].Dispose();

        await Assert.That(aliveAfterOuter).IsTrue();
        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(observer.Text).IsEqualTo("open w0 w0:a");
    }

    /// <summary>A downstream observer failure tears the sink down.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_OuterObserverThrows_DisposesTheSinkAndRethrows()
    {
        Signal<string> source = new();
        VirtualClock clock = new();
        var windows = 0;
        using var subscription = new SliceTimeCountSignal<string>(source, Ms(SpanMilliseconds), 1, clock).Subscribe(new CallbackWindowObserver(() =>
        {
            windows++;
            if (windows > 1)
            {
                throw new InvalidOperationException("observer");
            }
        }));

        await Assert.That(() => source.OnNext("a")).ThrowsExactly<InvalidOperationException>();
        await Assert.That(source.HasObservers).IsFalse();
    }

    /// <summary>Notifications after the sink is disposed are ignored.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_AfterDispose_IsIgnored()
    {
        const int windowSize = 2;
        VirtualClock clock = new();
        RecordingWitness<IObservable<string>> observer = new();
        SliceTimeCountWitness<string> witness = new(observer, Ms(SpanMilliseconds), windowSize, clock);
        witness.Start();
        witness.Dispose();

        witness.OnNext("a");
        witness.OnCompleted();
        clock.AdvanceBy(Ms(SpanMilliseconds + SpanMilliseconds));

        await Assert.That(observer.Values.Count).IsEqualTo(1);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>A null argument or a non-positive duration or count is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_InvalidArguments_Throw()
    {
        const int windowSize = 2;
        VirtualClock clock = new();
        RecordingWitness<IObservable<string>> observer = new();

        await Assert.That(() => new SliceTimeCountWitness<string>(null!, Ms(SpanMilliseconds), windowSize, clock)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SliceTimeCountWitness<string>(observer, Ms(SpanMilliseconds), windowSize, null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SliceTimeCountWitness<string>(observer, TimeSpan.Zero, windowSize, clock)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => new SliceTimeCountWitness<string>(observer, Ms(SpanMilliseconds), 0, clock)).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Creates a duration in milliseconds.</summary>
    /// <param name="milliseconds">The number of milliseconds.</param>
    /// <returns>The duration.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static TimeSpan Ms(int milliseconds) => TimeSpan.FromMilliseconds(milliseconds);

    /// <summary>Runs a callback for each window.</summary>
    /// <param name="onWindow">The callback.</param>
    private sealed class CallbackWindowObserver(Action onWindow) : IObserver<IObservable<string>>
    {
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(IObservable<string> value) => onWindow();
    }
}
