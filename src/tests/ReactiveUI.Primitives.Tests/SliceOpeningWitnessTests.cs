// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the sink that opens a window per opening signal emission and ends it with a closing signal.</summary>
public class SliceOpeningWitnessTests
{
    /// <summary>Windows overlap, close independently, and the outer sequence completes with the opening signal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OpeningsAndClosings_DriveOverlappingWindows()
    {
        Signal<string> source = new();
        Signal<int> openings = new();
        List<Signal<int>> closers = [];
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceOpeningSignal<string, int, int>(source, openings, _ => NewCloser(closers)).Subscribe(observer);

        source.OnNext("a");
        openings.OnNext(0);
        source.OnNext("b");
        openings.OnNext(0);
        source.OnNext("c");
        closers[0].OnNext(0);
        source.OnNext("d");
        closers[1].OnCompleted();
        source.OnNext("e");
        openings.OnNext(0);
        source.OnNext("f");
        openings.OnCompleted();
        source.OnNext("g");
        source.OnCompleted();

        await Assert.That(observer.Text).IsEqualTo(
            "open w0 w0:b open w1 w0:c w1:c w0:done w1:d w1:done open w2 w2:f outer:done w2:g w2:done");
    }

    /// <summary>The closing selector receives the opening value.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ClosingSelector_ReceivesTheOpeningValue()
    {
        const string opening = "opening";
        Signal<string> openings = new();
        List<string> seen = [];
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceOpeningSignal<string, string, int>(
            Signal.Silent<string>(),
            openings,
            value =>
            {
                seen.Add(value);
                return Signal.Silent<int>();
            }).Subscribe(observer);

        openings.OnNext(opening);

        await Assert.That(seen.SequenceEqual([opening])).IsTrue();
    }

    /// <summary>A closing signal that fires twice ends the window once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ClosingEmitsThenCompletes_EndsTheWindowOnce()
    {
        Signal<int> openings = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceOpeningSignal<string, int, int>(
            Signal.Silent<string>(),
            openings,
            static _ => new ScriptedObservable<int>(static o =>
            {
                o.OnNext(0);
                o.OnCompleted();
            })).Subscribe(observer);

        openings.OnNext(0);

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:done");
    }

    /// <summary>A closing error faults every open window and the outer sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ClosingFaults_FaultsEveryWindowAndTheOuterSequence()
    {
        Signal<string> source = new();
        Signal<int> openings = new();
        List<Signal<int>> closers = [];
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceOpeningSignal<string, int, int>(source, openings, _ => NewCloser(closers)).Subscribe(observer);
        openings.OnNext(0);
        openings.OnNext(0);

        closers[1].OnError(new InvalidOperationException("closing"));

        await Assert.That(observer.Text).IsEqualTo("open w0 open w1 w0:error closing w1:error closing outer:error closing");
        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(openings.HasObservers).IsFalse();
    }

    /// <summary>An opening error faults every open window and the outer sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OpeningsFault_FaultsEveryWindowAndTheOuterSequence()
    {
        Signal<int> openings = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceOpeningSignal<string, int, int>(Signal.Silent<string>(), openings, static _ => Signal.Silent<int>()).Subscribe(observer);
        openings.OnNext(0);

        openings.OnError(new InvalidOperationException("openings"));

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:error openings outer:error openings");
    }

    /// <summary>A closing selector failure faults every open window and the outer sequence without opening a window.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ClosingSelectorThrows_FaultsEveryWindowAndTheOuterSequence()
    {
        Signal<int> openings = new();
        var requests = 0;
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceOpeningSignal<string, int, int>(
            Signal.Silent<string>(),
            openings,
            _ =>
            {
                requests++;
                return requests == 1 ? Signal.Silent<int>() : throw new InvalidOperationException("selector");
            }).Subscribe(observer);
        openings.OnNext(0);

        openings.OnNext(0);

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:error selector outer:error selector");
    }

    /// <summary>Source completion completes every open window and the outer sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SourceCompletes_CompletesEveryWindowAndTheOuterSequence()
    {
        Signal<string> source = new();
        Signal<int> openings = new();
        List<Signal<int>> closers = [];
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceOpeningSignal<string, int, int>(source, openings, _ => NewCloser(closers)).Subscribe(observer);
        openings.OnNext(0);
        source.OnNext("a");

        source.OnCompleted();

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:a w0:done outer:done");
        await Assert.That(closers[0].HasObservers).IsFalse();
        await Assert.That(openings.HasObservers).IsFalse();
    }

    /// <summary>Source failure faults every open window and the outer sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SourceFaults_FaultsEveryWindowAndTheOuterSequence()
    {
        Signal<string> source = new();
        Signal<int> openings = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceOpeningSignal<string, int, int>(source, openings, static _ => Signal.Silent<int>()).Subscribe(observer);
        openings.OnNext(0);

        source.OnError(new InvalidOperationException("source"));

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:error source outer:error source");
    }

    /// <summary>Source failure after the opening signal completed faults only the open windows.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SourceFaults_AfterOpeningsCompleted_FaultsOnlyTheOpenWindows()
    {
        Signal<string> source = new();
        Signal<int> openings = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceOpeningSignal<string, int, int>(source, openings, static _ => Signal.Silent<int>()).Subscribe(observer);
        openings.OnNext(0);
        openings.OnCompleted();

        source.OnError(new InvalidOperationException("source"));

        await Assert.That(observer.Text).IsEqualTo("open w0 outer:done w0:error source");
    }

    /// <summary>A second completion of the opening signal is ignored.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OpeningsComplete_Repeated_CompletesTheOuterSequenceOnce()
    {
        RecordingWitness<IObservable<string>> observer = new();
        SliceOpeningWitness<string, int, int> witness = new(observer, static _ => Signal.Silent<int>());

        witness.Openings.OnCompleted();
        witness.Openings.OnCompleted();

        await Assert.That(observer.Completed).IsEqualTo(1);
    }

    /// <summary>Disposing the outer subscription keeps everything alive while a window is subscribed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_OuterWhileWindowSubscribed_KeepsSourceUntilWindowIsDisposed()
    {
        Signal<string> source = new();
        Signal<int> openings = new();
        List<Signal<int>> closers = [];
        WindowRecordingWitness<string> observer = new();
        var outer = new SliceOpeningSignal<string, int, int>(source, openings, _ => NewCloser(closers)).Subscribe(observer);
        openings.OnNext(0);

        outer.Dispose();
        var aliveAfterOuter = source.HasObservers && closers[0].HasObservers;
        source.OnNext("a");
        observer.Subscriptions[0].Dispose();

        await Assert.That(aliveAfterOuter).IsTrue();
        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(closers[0].HasObservers).IsFalse();
        await Assert.That(observer.Text).IsEqualTo("open w0 w0:a");
    }

    /// <summary>A downstream observer failure tears the sink down and reaches the caller.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Open_OuterObserverThrows_DisposesTheSinkAndRethrows()
    {
        Signal<string> source = new();
        Signal<int> openings = new();
        ThrowingWitness<IObservable<string>> observer = new(throwOnNext: true);
        using var subscription = new SliceOpeningSignal<string, int, int>(source, openings, static _ => Signal.Silent<int>()).Subscribe(observer);

        await Assert.That(() => openings.OnNext(0)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(source.HasObservers).IsFalse();
    }

    /// <summary>Notifications after the sink is disposed are ignored.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_AfterDispose_IsIgnored()
    {
        RecordingWitness<IObservable<string>> observer = new();
        SliceOpeningWitness<string, int, int> witness = new(observer, static _ => Signal.Silent<int>());
        witness.Dispose();

        witness.OnNext("a");
        witness.Openings.OnNext(0);
        witness.Openings.OnCompleted();
        witness.OnCompleted();

        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>A null observer or selector is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullArguments_ThrowArgumentNull()
    {
        RecordingWitness<IObservable<string>> observer = new();

        await Assert.That(static () => new SliceOpeningWitness<string, int, int>(null!, static _ => Signal.Silent<int>())).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SliceOpeningWitness<string, int, int>(observer, null!)).ThrowsExactly<ArgumentNullException>();
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
