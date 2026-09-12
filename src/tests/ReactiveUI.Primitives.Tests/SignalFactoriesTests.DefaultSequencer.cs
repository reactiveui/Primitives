// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies factory defaults, cancellation, and scheduled notifications.</summary>
public partial class SignalFactoriesTests
{
    /// <summary>The timeout used by the expiry factory test.</summary>
    private static readonly TimeSpan ShortExpiry = TimeSpan.FromMilliseconds(20);

    /// <summary>Verifies an empty range completes immediately without emitting.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AnEmptySequenceCompletesWithoutEmitting()
    {
        List<int> values = [];
        var completions = 0;
        using var subscription = Signal.Sequence(One, 0)
            .Subscribe(values.Add, static _ => { }, () => completions++);
        await Assert.That(values.Count).IsEqualTo(0);
        await Assert.That(completions).IsEqualTo(1);
    }

    /// <summary>The enumerable factory stops for a cancelled token and emits its whole sequence for an uncancellable one.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromEnumerableHonorsACancellableTokenAndIgnoresAnUncancellableOne()
    {
        using CancellationTokenSource cancelled = new();
        await cancelled.CancelAsync();
        List<int> cancelledValues = [];
        List<Exception> cancelledErrors = [];
        var cancelledCompletions = 0;
        using var cancelledSubscription = Signal.FromEnumerable([One, Two, Three], cancelled.Token)
            .Subscribe(cancelledValues.Add, cancelledErrors.Add, () => cancelledCompletions++);
        await Assert.That(cancelledValues.Count).IsEqualTo(0);
        await Assert.That(cancelledErrors.Count).IsEqualTo(0);
        await Assert.That(cancelledCompletions).IsEqualTo(0);
        List<int> plainValues = [];
        using var plainSubscription = Signal.FromEnumerable([One, Two, Three], CancellationToken.None)
            .Subscribe(plainValues.Add);
        await Assert.That(plainValues.SequenceEqual([One, Two, Three])).IsTrue();
    }

    /// <summary>Running the scheduled expiry fails a silent source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ExpireFailsASilentSequenceWhenTheDelayRuns()
{
        ManualSequencer sequencer = new();
        RecordingWitness<int> witness = new();
        using var subscription = Signal.Expire(Signal.Silent<int>(), ShortExpiry, sequencer).Subscribe(witness);
        await Assert.That(witness.Errors.Count).IsEqualTo(0);
        sequencer.RunPending();
        await Assert.That(witness.Errors[0]).IsTypeOf<TimeoutException>();
        await Assert.That(witness.Errors.Count).IsEqualTo(1);
    }

    /// <summary>Verifies the sequencer-free <c>Start</c> factories run their work and emit its outcome.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StartWithoutASequencerRunsTheWorkOnTheDefaultSequencer()
    {
        AwaitableWitness<int> functionWitness = new();
        using var functionSubscription = Signal.Start(static () => Two).Subscribe(functionWitness);
        await functionWitness.ValueCountReaching(1);
        await Assert.That(functionWitness.Values.SequenceEqual([Two])).IsTrue();
        var actionRuns = 0;
        AwaitableWitness<RxVoid> actionWitness = new();

        // A void method group selects Start(Action); a lambda over 'actionRuns++' would bind to Start<T>.
        void RunAction() => actionRuns++;

        using var actionSubscription = Signal.Start(RunAction).Subscribe(actionWitness);
        await actionWitness.ValueCountReaching(1);
        await Assert.That(actionRuns).IsEqualTo(1);
        await Assert.That(actionWitness.Values[0]).IsEqualTo(RxVoid.Default);
    }

    /// <summary>Each scheduled callback emits the next tick.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EveryTicksWhenTheSequencerRuns()
{
        ManualSequencer sequencer = new();
        RecordingWitness<long> witness = new();
        using var subscription = Signal.Every(ShortExpiry, sequencer).Subscribe(witness);
        sequencer.RunPending();
        sequencer.RunPending();
        await Assert.That(witness.Values.SequenceEqual([0L, 1L])).IsTrue();
    }
}
