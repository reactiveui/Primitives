// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the sink that hands out windows of a fixed number of values.</summary>
public class SliceCountWitnessTests
{
    /// <summary>Windows with a skip equal to the count are consecutive and do not overlap.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_SkipEqualToCount_HandsOutConsecutiveWindows()
    {
        const int windowSize = 2;

        var text = Run(windowSize, windowSize, "a", "b", "c", "d", "e");

        await Assert.That(text).IsEqualTo("open w0 w0:a w0:b w0:done open w1 w1:c w1:d w1:done open w2 w2:e w2:done outer:done");
    }

    /// <summary>A skip below the count opens overlapping windows, each receiving every value while it is open.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_SkipBelowCount_OverlapsWindows()
    {
        const int windowSize = 3;
        const int skip = 2;

        var text = Run(windowSize, skip, "a", "b", "c", "d", "e", "f", "g");

        await Assert.That(text).IsEqualTo(
            "open w0 w0:a w0:b open w1 w0:c w1:c w0:done w1:d open w2 w1:e w2:e w1:done w2:f open w3 w2:g w3:g w2:done w3:done outer:done");
    }

    /// <summary>A skip above the count leaves a gap of values that no window receives.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_SkipAboveCount_DropsTheValuesInTheGap()
    {
        const int windowSize = 2;
        const int skip = 3;

        var text = Run(windowSize, skip, "a", "b", "c", "d", "e", "f", "g");

        await Assert.That(text).IsEqualTo("open w0 w0:a w0:b w0:done open w1 w1:d w1:e w1:done open w2 w2:g w2:done outer:done");
    }

    /// <summary>An empty source completes the first window and the outer sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnCompleted_WithoutValues_CompletesTheFirstWindow()
    {
        const int windowSize = 2;

        var text = Run(windowSize, windowSize);

        await Assert.That(text).IsEqualTo("open w0 w0:done outer:done");
    }

    /// <summary>A source error faults the open windows in order and then the outer sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnError_FaultsOpenWindowsThenTheOuterSequence()
    {
        const int windowSize = 3;
        Signal<string> source = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceCountSignal<string>(source, windowSize, 1).Subscribe(observer);
        source.OnNext("a");
        source.OnNext("b");

        source.OnError(new InvalidOperationException("boom"));

        await Assert.That(observer.Text).IsEqualTo(
            "open w0 w0:a open w1 w0:b w1:b open w2 w0:error boom w1:error boom w2:error boom outer:error boom");
        await Assert.That(source.HasObservers).IsFalse();
    }

    /// <summary>Disposing the outer subscription keeps the source alive while a window is subscribed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_OuterWhileWindowSubscribed_KeepsSourceUntilWindowIsDisposed()
    {
        const int windowSize = 3;
        Signal<string> source = new();
        WindowRecordingWitness<string> observer = new();
        var outer = new SliceCountSignal<string>(source, windowSize, windowSize).Subscribe(observer);
        source.OnNext("a");

        outer.Dispose();
        var aliveAfterOuter = source.HasObservers;
        source.OnNext("b");
        observer.Subscriptions[0].Dispose();

        await Assert.That(aliveAfterOuter).IsTrue();
        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(observer.Text).IsEqualTo("open w0 w0:a w0:b");
    }

    /// <summary>Disposing the outer subscription with no subscribed window releases the source.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_OuterWithoutSubscribedWindow_ReleasesTheSource()
    {
        const int windowSize = 3;
        Signal<string> source = new();
        RecordingWitness<IObservable<string>> observer = new();
        var outer = new SliceCountSignal<string>(source, windowSize, windowSize).Subscribe(observer);

        outer.Dispose();

        await Assert.That(source.HasObservers).IsFalse();
    }

    /// <summary>A downstream observer failure tears the sink down and reaches the caller.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Start_OuterObserverThrows_DisposesTheSinkAndRethrows()
    {
        const int windowSize = 2;
        Signal<string> source = new();
        ThrowingWitness<IObservable<string>> observer = new(throwOnNext: true);

        await Assert.That(() => new SliceCountSignal<string>(source, windowSize, windowSize).Subscribe(observer)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(source.HasObservers).IsFalse();
    }

    /// <summary>Notifications after the sink is disposed are ignored.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_AfterDispose_IsIgnored()
    {
        const int windowSize = 2;
        RecordingWitness<IObservable<string>> observer = new();
        SliceCountWitness<string> witness = new(observer, windowSize, windowSize);
        witness.Dispose();

        witness.OnNext("a");
        witness.OnCompleted();

        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>A second terminal notification is ignored.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnCompleted_Repeated_ForwardsOnlyTheFirst()
    {
        const int windowSize = 2;
        RecordingWitness<IObservable<string>> observer = new();
        SliceCountWitness<string> witness = new(observer, windowSize, windowSize);
        witness.Start();

        witness.OnCompleted();
        witness.OnError(new InvalidOperationException("late"));

        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
    }

    /// <summary>A null observer or a non-positive size is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_InvalidArguments_Throw()
    {
        const int windowSize = 2;
        RecordingWitness<IObservable<string>> observer = new();

        await Assert.That(static () => new SliceCountWitness<string>(null!, windowSize, windowSize)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SliceCountWitness<string>(observer, 0, windowSize)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => new SliceCountWitness<string>(observer, windowSize, 0)).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Feeds values through a count window and returns the recorded log.</summary>
    /// <param name="count">The window size.</param>
    /// <param name="skip">The number of values between window starts.</param>
    /// <param name="values">The values the source emits before it completes.</param>
    /// <returns>The recorded log.</returns>
    private static string Run(int count, int skip, params string[] values)
    {
        Signal<string> source = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceCountSignal<string>(source, count, skip).Subscribe(observer);
        foreach (var value in values)
        {
            source.OnNext(value);
        }

        source.OnCompleted();
        return observer.Text;
    }
}
