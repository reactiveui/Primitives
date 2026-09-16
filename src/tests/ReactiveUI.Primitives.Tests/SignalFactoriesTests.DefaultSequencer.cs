// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
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

    /// <summary>The sequencer-free Start factories select the default sequencer without invoking their work.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StartWithoutASequencerSelectsTheDefaultSequencer()
    {
        var functionRuns = 0;
        var function = await Assert.That(Signal.Start(() => ++functionRuns)).IsTypeOf<StartSignal<int>>().And.IsNotNull();
        var actionRuns = 0;
        void RunAction() => actionRuns++;
        var action = await Assert.That(Signal.Start(RunAction)).IsTypeOf<StartSignal>().And.IsNotNull();

        await Assert.That(function.Scheduler).IsSameReferenceAs(Sequencer.Default);
        await Assert.That(action.Scheduler).IsSameReferenceAs(Sequencer.Default);
        await Assert.That(functionRuns).IsEqualTo(0);
        await Assert.That(actionRuns).IsEqualTo(0);
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

    /// <summary>The recurring factory without a scheduler creates a lazy thread-pool signal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Every_WithoutScheduler_CreatesLazyRecurringSignal()
    {
        var signal = await Assert.That(Signal.Every(ShortExpiry)).IsTypeOf<EverySignal>().And.IsNotNull();

        await Assert.That(signal.IsRequiredSubscribeOnCurrentThread()).IsFalse();
    }

    /// <summary>The expiry factory without a scheduler validates its source and preserves lazy subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Expire_WithoutScheduler_CreatesLazyTimeoutSignal()
    {
        using Signal<int> source = new();
        var signal = await Assert.That(Signal.Expire(source, ShortExpiry)).IsTypeOf<ExpireSignal<int>>().And.IsNotNull();

        await Assert.That(signal.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(static () => Signal.Expire<int>(null!, ShortExpiry)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>The expiry factory falls back to the thread-pool sequencer when given a null scheduler.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Expire_NullScheduler_CreatesTimeoutSignal()
    {
        using Signal<int> source = new();

        await Assert.That(Signal.Expire(source, ShortExpiry, null)).IsTypeOf<ExpireSignal<int>>();
    }

    /// <summary>The empty and failing factories complete immediately on the immediate sequencer and schedule otherwise.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task NoneAndFail_ImmediateAndScheduledSequencers_Terminate()
    {
        InvalidOperationException error = new("fail");
        RecordingWitness<int> noneImmediate = new();
        RecordingWitness<int> failImmediate = new();

        using var noneSubscription = Signal.None(Sequencer.Immediate, 0).Subscribe(noneImmediate);
        using var failSubscription = Signal.Fail(error, Sequencer.Immediate, 0).Subscribe(failImmediate);

        await Assert.That(noneImmediate.Completed).IsEqualTo(1);
        await Assert.That(failImmediate.Errors).HasSingleItem();
        await Assert.That(Signal.None(ThreadPoolSequencer.Instance, 0)).IsNotNull();
        await Assert.That(Signal.Fail(error, ThreadPoolSequencer.Instance, 0)).IsNotNull();
    }

    /// <summary>An empty range completes without emitting a value.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Range_ZeroCount_CompletesWithoutValues()
    {
        RecordingWitness<int> observer = new();

        using var subscription = Signal.Range(1, 0).Subscribe(observer);

        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(1);
    }
}
