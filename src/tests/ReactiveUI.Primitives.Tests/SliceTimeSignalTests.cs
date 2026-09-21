// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the cold signal that splits a source into windows of a fixed duration.</summary>
public class SliceTimeSignalTests
{
    /// <summary>The duration of each window, in milliseconds.</summary>
    private const int SpanMilliseconds = 10;

    /// <summary>Each subscription runs its own timer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_TwiceRunsEachTimerOnItsOwn()
    {
        const int laterSubscriptionOffsetMilliseconds = 5;
        Signal<string> source = new();
        VirtualClock clock = new();
        var span = TimeSpan.FromMilliseconds(SpanMilliseconds);
        SliceTimeSignal<string> signal = new(source, span, span, clock);
        WindowRecordingWitness<string> first = new();
        WindowRecordingWitness<string> second = new();

        using var firstSubscription = signal.Subscribe(first);
        clock.AdvanceBy(TimeSpan.FromMilliseconds(laterSubscriptionOffsetMilliseconds));
        using var secondSubscription = signal.Subscribe(second);
        clock.AdvanceBy(TimeSpan.FromMilliseconds(laterSubscriptionOffsetMilliseconds));

        await Assert.That(first.Text).IsEqualTo("open w0 w0:done open w1");
        await Assert.That(second.Text).IsEqualTo("open w0");
    }

    /// <summary>A null argument or a non-positive duration is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_InvalidArguments_Throw()
    {
        var source = Signal.None<string>();
        VirtualClock clock = new();
        var span = TimeSpan.FromMilliseconds(SpanMilliseconds);

        await Assert.That(() => new SliceTimeSignal<string>(null!, span, span, clock)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SliceTimeSignal<string>(source, span, span, null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SliceTimeSignal<string>(source, TimeSpan.Zero, span, clock)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => new SliceTimeSignal<string>(source, span, TimeSpan.Zero, clock)).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>A null observer is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_NullObserver_ThrowsArgumentNull()
    {
        var span = TimeSpan.FromMilliseconds(SpanMilliseconds);
        SliceTimeSignal<string> signal = new(Signal.None<string>(), span, span, new VirtualClock());

        await Assert.That(() => signal.Subscribe(null!)).ThrowsExactly<ArgumentNullException>();
    }
}
