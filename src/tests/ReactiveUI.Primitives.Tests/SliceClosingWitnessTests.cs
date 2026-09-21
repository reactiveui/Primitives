// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the sink that hands out windows ended by their own closing signal.</summary>
public class SliceClosingWitnessTests
{
    /// <summary>A closing signal that emits ends the window, and a new closing signal is requested for the next window.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ClosingEmitsOrCompletes_EndsTheWindowAndOpensTheNext()
    {
        Signal<string> source = new();
        List<Signal<int>> closers = [];
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceClosingSignal<string, int>(source, () => NewCloser(closers)).Subscribe(observer);
        var closersAtStart = closers.Count;

        source.OnNext("a");
        closers[0].OnNext(0);
        var closersAfterFirst = closers.Count;
        source.OnNext("b");
        closers[1].OnCompleted();
        var closersAfterSecond = closers.Count;
        source.OnNext("c");
        source.OnCompleted();

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:a w0:done open w1 w1:b w1:done open w2 w2:c w2:done outer:done");
        await Assert.That(closersAtStart).IsEqualTo(1);
        await Assert.That(closersAfterFirst).IsEqualTo(closersAtStart + 1);
        await Assert.That(closersAfterSecond).IsEqualTo(closersAfterFirst + 1);
    }

    /// <summary>A closing signal that fires twice ends the window once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ClosingEmitsThenCompletes_EndsTheWindowOnce()
    {
        var requests = 0;
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceClosingSignal<string, int>(
            Signal.Silent<string>(),
            () =>
            {
                requests++;
                return requests == 1
                    ? new ScriptedObservable<int>(static o =>
                    {
                        o.OnNext(0);
                        o.OnCompleted();
                    })
                    : Signal.Silent<int>();
            }).Subscribe(observer);

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:done open w1");
        await Assert.That(requests).IsGreaterThan(1);
    }

    /// <summary>A closing error faults the window and the outer sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ClosingFaults_FaultsTheWindowAndTheOuterSequence()
    {
        Signal<string> source = new();
        List<Signal<int>> closers = [];
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceClosingSignal<string, int>(source, () => NewCloser(closers)).Subscribe(observer);

        closers[0].OnError(new InvalidOperationException("closing"));

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:error closing outer:error closing");
        await Assert.That(source.HasObservers).IsFalse();
    }

    /// <summary>A selector failure on the first window faults the window and the outer sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SelectorThrows_OnFirstWindow_FaultsTheWindowAndTheOuterSequence()
    {
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceClosingSignal<string, int>(
            Signal.Silent<string>(),
            static IObservable<int> () => throw new InvalidOperationException("selector")).Subscribe(observer);

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:error selector outer:error selector");
    }

    /// <summary>A selector failure on a later window faults that window and the outer sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SelectorThrows_OnLaterWindow_FaultsThatWindowAndTheOuterSequence()
    {
        var requests = 0;
        Signal<int> closer = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceClosingSignal<string, int>(
            Signal.Silent<string>(),
            () =>
            {
                requests++;
                return requests == 1 ? closer : throw new InvalidOperationException("selector");
            }).Subscribe(observer);

        closer.OnNext(0);

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:done open w1 w1:error selector outer:error selector");
    }

    /// <summary>Source completion ends the window and the outer sequence and releases the closing subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SourceCompletes_EndsTheWindowAndReleasesTheClosingSubscription()
    {
        Signal<string> source = new();
        List<Signal<int>> closers = [];
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceClosingSignal<string, int>(source, () => NewCloser(closers)).Subscribe(observer);

        source.OnCompleted();

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:done outer:done");
        await Assert.That(closers[0].HasObservers).IsFalse();
    }

    /// <summary>A source error faults the window and the outer sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SourceFaults_FaultsTheWindowAndTheOuterSequence()
    {
        Signal<string> source = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceClosingSignal<string, int>(source, static () => Signal.Silent<int>()).Subscribe(observer);

        source.OnError(new InvalidOperationException("source"));

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:error source outer:error source");
    }

    /// <summary>Disposing the outer subscription keeps the source alive while a window is subscribed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_OuterWhileWindowSubscribed_KeepsSourceUntilWindowIsDisposed()
    {
        Signal<string> source = new();
        WindowRecordingWitness<string> observer = new();
        var outer = new SliceClosingSignal<string, int>(source, static () => Signal.Silent<int>()).Subscribe(observer);

        outer.Dispose();
        var aliveAfterOuter = source.HasObservers;
        source.OnNext("a");
        observer.Subscriptions[0].Dispose();

        await Assert.That(aliveAfterOuter).IsTrue();
        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(observer.Text).IsEqualTo("open w0 w0:a");
    }

    /// <summary>A window subscriber that throws tears the sink down and the exception reaches the source.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_WindowObserverThrows_DisposesTheSinkAndRethrows()
    {
        Signal<string> source = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceClosingSignal<string, int>(source, static () => Signal.Silent<int>()).Subscribe(observer);
        using var thrower = observer.Windows[0].Subscribe(new ThrowingWitness<string>(throwOnNext: true));

        await Assert.That(() => source.OnNext("a")).ThrowsExactly<InvalidOperationException>();
        await Assert.That(source.HasObservers).IsFalse();
    }

    /// <summary>Notifications after the sink is disposed are ignored.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_AfterDispose_IsIgnored()
    {
        RecordingWitness<IObservable<string>> observer = new();
        SliceClosingWitness<string, int> witness = new(observer, static () => Signal.Silent<int>());
        witness.Start();
        witness.Dispose();

        witness.OnNext("a");
        witness.OnCompleted();

        await Assert.That(observer.Values.Count).IsEqualTo(1);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>A null observer or selector is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullArguments_ThrowArgumentNull()
    {
        RecordingWitness<IObservable<string>> observer = new();

        await Assert.That(static () => new SliceClosingWitness<string, int>(null!, static () => Signal.Silent<int>())).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SliceClosingWitness<string, int>(observer, null!)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Creates a closing signal and records it.</summary>
    /// <param name="closers">The recorded closing signals.</param>
    /// <returns>The closing signal.</returns>
    private static Signal<int> NewCloser(List<Signal<int>> closers)
    {
        Signal<int> closer = new();
        closers.Add(closer);
        return closer;
    }
}
