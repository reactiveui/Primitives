// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Tests for the quiet-period coordinator behind <c>Calm</c> and its <c>Throttle</c> alias, whose completion
/// has to deliver the value still waiting inside the quiet window instead of discarding it.
/// </summary>
public sealed class CalmCoordinatorTests
{
    /// <summary>The integer constant one.</summary>
    private const int One = 1;

    /// <summary>The integer constant two.</summary>
    private const int Two = 2;

    /// <summary>The number of values a flushed sequence delivers.</summary>
    private const int FlushedValueCount = 1;

    /// <summary>The marker for a completion that has not been observed yet.</summary>
    private const int NoCompletionObserved = -1;

    /// <summary>The quiet period used by these tests; its timer only fires when the test runs it.</summary>
    private static readonly TimeSpan QuietPeriod = TimeSpan.FromMilliseconds(50);

    /// <summary>Verifies completion inside the quiet window emits the buffered value before completing.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompletionInsideTheQuietWindowEmitsTheBufferedValueFirst()
    {
        ManualSequencer sequencer = new();
        IObserver<int>? source = null;
        List<int> values = [];
        var valuesAtCompletion = NoCompletionObserved;
        using var subscription = new ScriptedObservable<int>(observer => source = observer)
            .Calm(QuietPeriod, sequencer)
            .Subscribe(values.Add, static _ => { }, () => valuesAtCompletion = values.Count);

        source!.OnNext(One);
        source.OnCompleted();

        await Assert.That(values.SequenceEqual([One])).IsTrue();
        await Assert.That(valuesAtCompletion).IsEqualTo(FlushedValueCount);
    }

    /// <summary>Verifies completion flushes only the newest value buffered by the quiet window.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompletionFlushesOnlyTheNewestBufferedValue()
    {
        ManualSequencer sequencer = new();
        IObserver<int>? source = null;
        RecordingWitness<int> witness = new();
        using var subscription = new ScriptedObservable<int>(observer => source = observer)
            .Calm(QuietPeriod, sequencer)
            .Subscribe(witness);

        source!.OnNext(One);
        source.OnNext(Two);
        source.OnCompleted();

        await Assert.That(witness.Values.SequenceEqual([Two])).IsTrue();
        await Assert.That(witness.Completed).IsEqualTo(1);
    }

    /// <summary>Verifies completion does not repeat a value the quiet-period timer has already delivered.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompletionDoesNotRepeatAValueTheTimerAlreadyDelivered()
    {
        ManualSequencer sequencer = new();
        IObserver<int>? source = null;
        RecordingWitness<int> witness = new();
        using var subscription = new ScriptedObservable<int>(observer => source = observer)
            .Calm(QuietPeriod, sequencer)
            .Subscribe(witness);

        source!.OnNext(One);
        sequencer.Advance(QuietPeriod);
        sequencer.RunPending();
        source.OnCompleted();

        await Assert.That(witness.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(witness.Completed).IsEqualTo(1);
    }

    /// <summary>Verifies a failing source discards the buffered value and forwards only the error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FailureInsideTheQuietWindowDiscardsTheBufferedValue()
    {
        ManualSequencer sequencer = new();
        IObserver<int>? source = null;
        RecordingWitness<int> witness = new();
        using var subscription = new ScriptedObservable<int>(observer => source = observer)
            .Calm(QuietPeriod, sequencer)
            .Subscribe(witness);

        source!.OnNext(One);
        source.OnError(new InvalidOperationException("calm-error"));

        await Assert.That(witness.Values.Count).IsEqualTo(0);
        await Assert.That(witness.Errors.Count).IsEqualTo(1);
        await Assert.That(witness.Completed).IsEqualTo(0);
    }
}
