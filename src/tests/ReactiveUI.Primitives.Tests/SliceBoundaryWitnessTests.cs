// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the sink that hands out windows delimited by a boundary signal.</summary>
public class SliceBoundaryWitnessTests
{
    /// <summary>Each boundary emission ends the window and starts the next.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task BoundaryEmits_EndsTheWindowAndStartsTheNext()
    {
        Signal<string> source = new();
        Signal<int> boundary = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceBoundarySignal<string, int>(source, boundary).Subscribe(observer);

        source.OnNext("a");
        boundary.OnNext(0);
        source.OnNext("b");
        boundary.OnNext(0);
        source.OnNext("c");
        source.OnCompleted();

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:a w0:done open w1 w1:b w1:done open w2 w2:c w2:done outer:done");
    }

    /// <summary>A completed boundary signal ends the window and the outer sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task BoundaryCompletes_EndsTheWindowAndTheOuterSequence()
    {
        Signal<string> source = new();
        Signal<int> boundary = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceBoundarySignal<string, int>(source, boundary).Subscribe(observer);
        source.OnNext("a");

        boundary.OnCompleted();
        source.OnNext("b");

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:a w0:done outer:done");
        await Assert.That(source.HasObservers).IsFalse();
    }

    /// <summary>A boundary error faults the window and the outer sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task BoundaryFaults_FaultsTheWindowAndTheOuterSequence()
    {
        Signal<string> source = new();
        Signal<int> boundary = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceBoundarySignal<string, int>(source, boundary).Subscribe(observer);

        boundary.OnError(new InvalidOperationException("boundary"));

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:error boundary outer:error boundary");
        await Assert.That(source.HasObservers).IsFalse();
    }

    /// <summary>A source error faults the window and the outer sequence and releases the boundary subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SourceFaults_FaultsTheWindowAndTheOuterSequence()
    {
        Signal<string> source = new();
        Signal<int> boundary = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceBoundarySignal<string, int>(source, boundary).Subscribe(observer);

        source.OnError(new InvalidOperationException("source"));

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:error source outer:error source");
        await Assert.That(boundary.HasObservers).IsFalse();
    }

    /// <summary>An empty source completes the first window and the outer sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SourceCompletes_WithoutValues_CompletesTheFirstWindow()
    {
        Signal<string> source = new();
        WindowRecordingWitness<string> observer = new();
        using var subscription = new SliceBoundarySignal<string, int>(source, Signal.Silent<int>()).Subscribe(observer);

        source.OnCompleted();

        await Assert.That(observer.Text).IsEqualTo("open w0 w0:done outer:done");
    }

    /// <summary>Disposing the outer subscription keeps the source and boundary alive while a window is subscribed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_OuterWhileWindowSubscribed_KeepsSourceUntilWindowIsDisposed()
    {
        Signal<string> source = new();
        Signal<int> boundary = new();
        WindowRecordingWitness<string> observer = new();
        var outer = new SliceBoundarySignal<string, int>(source, boundary).Subscribe(observer);

        outer.Dispose();
        var aliveAfterOuter = source.HasObservers && boundary.HasObservers;
        source.OnNext("a");
        observer.Subscriptions[0].Dispose();

        await Assert.That(aliveAfterOuter).IsTrue();
        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(boundary.HasObservers).IsFalse();
        await Assert.That(observer.Text).IsEqualTo("open w0 w0:a");
    }

    /// <summary>A downstream observer failure tears the sink down.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Advance_OuterObserverThrows_DisposesTheSinkAndRethrows()
    {
        Signal<string> source = new();
        Signal<int> boundary = new();
        using var subscription = new SliceBoundarySignal<string, int>(source, boundary).Subscribe(new ThrowAfterFirstWindow());

        await Assert.That(() => boundary.OnNext(0)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(source.HasObservers).IsFalse();
    }

    /// <summary>Notifications after the sink is disposed are ignored.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_AfterDispose_IsIgnored()
    {
        RecordingWitness<IObservable<string>> observer = new();
        SliceBoundaryWitness<string, int> witness = new(observer);
        witness.Start();
        witness.Dispose();

        witness.OnNext("a");
        witness.Boundaries.OnNext(0);
        witness.OnCompleted();

        await Assert.That(observer.Values.Count).IsEqualTo(1);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>A null observer is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullObserver_ThrowsArgumentNull() =>
        await Assert.That(static () => new SliceBoundaryWitness<string, int>(null!)).ThrowsExactly<ArgumentNullException>();

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
