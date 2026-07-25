// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Verifies how the <see cref="Signal"/> run-once, sequence, loop, and timer factories dispatch through an
/// explicit sequencer: the work is deferred until the sequencer runs it, a current-thread sequencer routes the
/// subscription through the trampoline, and an external cancellation token is only wired up when it can be cancelled.
/// </summary>
public partial class SignalFactoriesTests
{
    /// <summary>The number of values taken from the infinite loop signal before it is torn down.</summary>
    private const int LoopTakeCount = 3;

    /// <summary>The tick used as both the due time and the period of the virtual timers.</summary>
    private static readonly TimeSpan SingleTick = TimeSpan.FromTicks(1);

    /// <summary>The action factory defers its work to the sequencer and emits a single unit on completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StartActionDefersToTheSequencerThenCompletesWithASingleUnit()
    {
        VirtualClock clock = new();
        RecordingWitness<RxVoid> witness = new();
        var runCount = 0;
        void Run() => runCount++;

        using var subscription = Signal.Start(Run, clock).Subscribe(witness);

        await Assert.That(runCount).IsEqualTo(0);
        await Assert.That(witness.Values.Count).IsEqualTo(0);

        clock.Start();

        await Assert.That(runCount).IsEqualTo(1);
        await Assert.That(witness.Values.Count).IsEqualTo(1);
        await Assert.That(witness.Completed).IsEqualTo(1);
        await Assert.That(witness.Errors.Count).IsEqualTo(0);
    }

    /// <summary>An action that throws is reported as an error rather than a completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StartActionForwardsAThrownErrorInsteadOfCompleting()
    {
        VirtualClock clock = new();
        RecordingWitness<RxVoid> witness = new();
        InvalidOperationException expected = new("start-action");
        void Fail() => throw expected;

        using var subscription = Signal.Start(Fail, clock).Subscribe(witness);
        clock.Start();

        await Assert.That(witness.Errors.Count).IsEqualTo(1);
        await Assert.That(witness.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(witness.Values.Count).IsEqualTo(0);
        await Assert.That(witness.Completed).IsEqualTo(0);
    }

    /// <summary>The action factory declares the current-thread requirement and runs through the trampoline.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StartActionOnTheCurrentThreadSequencerRunsThroughTheTrampoline()
    {
        var signal = Signal.Start(static () => { }, Sequencer.CurrentThread);
        await Assert.That(((IRequireCurrentThread<RxVoid>)signal).IsRequiredSubscribeOnCurrentThread()).IsTrue();
        await Assert.That(((IRequireCurrentThread<RxVoid>)Signal.Start(static () => { }, new VirtualClock()))
            .IsRequiredSubscribeOnCurrentThread()).IsFalse();

        RecordingWitness<RxVoid> witness = new();
        var runCount = 0;
        void Run() => runCount++;

        using var subscription = Signal.Start(Run, Sequencer.CurrentThread).Subscribe(witness);

        await Assert.That(runCount).IsEqualTo(1);
        await Assert.That(witness.Values.Count).IsEqualTo(1);
        await Assert.That(witness.Completed).IsEqualTo(1);
    }

    /// <summary>The function factory defers to the sequencer and then emits the function result.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StartFunctionDefersToTheSequencerThenEmitsItsResult()
    {
        VirtualClock clock = new();
        RecordingWitness<int> witness = new();

        using var subscription = Signal.Start(static () => Seven, clock).Subscribe(witness);

        await Assert.That(witness.Values.Count).IsEqualTo(0);

        clock.Start();

        await Assert.That(witness.Values.SequenceEqual(ExpectedSingleSeven)).IsTrue();
        await Assert.That(witness.Completed).IsEqualTo(1);
    }

    /// <summary>A sequence bound to the current-thread sequencer emits its whole range through the trampoline.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SequenceOnTheCurrentThreadSequencerEmitsTheRangeThroughTheTrampoline()
    {
        SequenceSignal signal = new(Three, Three, Sequencer.CurrentThread);
        await Assert.That(signal.IsRequiredSubscribeOnCurrentThread()).IsTrue();

        RecordingWitness<int> witness = new();
        using var subscription = signal.Subscribe(witness);

        await Assert.That(witness.Values.SequenceEqual(ExpectedThreeToFive)).IsTrue();
        await Assert.That(witness.Completed).IsEqualTo(1);
        _ = Assert.Throws<ArgumentNullException>(() => signal.Subscribe(null!));
    }

    /// <summary>A one-shot timer on the current-thread sequencer ticks once and completes through the trampoline.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AfterOnTheCurrentThreadSequencerTicksOnceThroughTheTrampoline()
    {
        RecordingWitness<long> witness = new();

        using var subscription = Signal.After(TimeSpan.Zero, Sequencer.CurrentThread).Subscribe(witness);

        await Assert.That(witness.Values.SequenceEqual(ExpectedSingleZeroTick)).IsTrue();
        await Assert.That(witness.Completed).IsEqualTo(1);
    }

    /// <summary>A periodic timer stops rescheduling once the subscription is torn down by the downstream.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PeriodicAfterStopsReschedulingWhenTheSubscriptionIsDisposed()
    {
        VirtualClock clock = new();
        RecordingWitness<long> witness = new();

        using var subscription = Signal.After(SingleTick, SingleTick, clock).Take(1).Subscribe(witness);
        clock.AdvanceBy(SingleTick);

        await Assert.That(witness.Values.SequenceEqual(ExpectedSingleZeroTick)).IsTrue();
        await Assert.That(witness.Completed).IsEqualTo(1);

        // The downstream tore the timer down on its first tick, so no further tick may be scheduled.
        clock.AdvanceBy(SingleTick);
        clock.AdvanceBy(SingleTick);

        await Assert.That(witness.Values.SequenceEqual(ExpectedSingleZeroTick)).IsTrue();
    }

    /// <summary>The infinite loop signal repeats its value and stops as soon as the downstream tears it down.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LoopRepeatsItsValueUntilTheDownstreamTearsItDown()
    {
        await Assert.That(((IRequireCurrentThread<int>)Signal.Loop(Seven)).IsRequiredSubscribeOnCurrentThread())
            .IsTrue();

        RecordingWitness<int> witness = new();

        // Loop drives itself through the current-thread trampoline, so the subscription has to be taken while the
        // trampoline is already running; otherwise the handle that stops the recursion is never handed back.
        _ = Sequencer.CurrentThread.Schedule(() =>
            Signal.Loop(Seven).Take(LoopTakeCount).Subscribe(witness));

        await Assert.That(witness.Values.SequenceEqual(ExpectedSevenSevenSeven)).IsTrue();
        await Assert.That(witness.Completed).IsEqualTo(1);
    }

    /// <summary>A loop subscription that is already torn down never emits.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LoopEmitsNothingWhenTheSubscriptionIsDisposedBeforeItRuns()
    {
        RecordingWitness<int> witness = new();

        _ = Sequencer.CurrentThread.Schedule(() => Signal.Loop(Seven).Subscribe(witness).Dispose());

        await Assert.That(witness.Values.Count).IsEqualTo(0);
        await Assert.That(witness.Completed).IsEqualTo(0);
    }

    /// <summary>The unit and witness-typed terminal factories return the shared allocation-free signals.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UnitEmitAndWitnessFailFactoriesProduceTheExpectedTerminals()
    {
        RecordingWitness<RxVoid> unitWitness = new();
        using var unitSubscription = Signal.Emit(RxVoid.Default).Subscribe(unitWitness);

        await Assert.That(unitWitness.Values.Count).IsEqualTo(1);
        await Assert.That(unitWitness.Completed).IsEqualTo(1);

        RecordingWitness<int> failWitness = new();
        InvalidOperationException expected = new("witness-fail");
        using var failSubscription = Signal.Fail(expected, One).Subscribe(failWitness);

        await Assert.That(failWitness.Errors.Count).IsEqualTo(1);
        await Assert.That(failWitness.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(failWitness.Values.Count).IsEqualTo(0);
        await Assert.That(failWitness.Completed).IsEqualTo(0);
    }

    /// <summary>A scheduled signal routes values to its default observer until a real subscriber takes over.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduledSignalSendsToTheDefaultObserverWhileNoSubscriberIsPresent()
    {
        RecordingWitness<int> fallback = new();
        var signal = Signal.Scheduled(Sequencer.Immediate, fallback);

        signal.OnNext(One);

        await Assert.That(fallback.Values.SequenceEqual(SingleFirstExpected)).IsTrue();

        RecordingWitness<int> subscriber = new();
        using var subscription = signal.Subscribe(subscriber);
        signal.OnNext(Two);

        await Assert.That(subscriber.Values.Count).IsEqualTo(1);
        await Assert.That(subscriber.Values[0]).IsEqualTo(Two);
        await Assert.That(fallback.Values.SequenceEqual(SingleFirstExpected)).IsTrue();
    }

    /// <summary>The external-cancellation async factory runs to completion when its token can never be cancelled.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromAsyncWithAnUncancellableTokenStillCompletesWithItsResult()
    {
        RecordingWitness<int> witness = new();

        using var subscription = Signal
            .FromAsync(static _ => Task.FromResult(Seven), CancellationToken.None)
            .Subscribe(witness);

        await TestPolling.SpinUntil(() => witness.Completed == 1, TimeSpan.FromSeconds(TimeoutSeconds));

        await Assert.That(witness.Values.SequenceEqual(ExpectedSingleSeven)).IsTrue();
        await Assert.That(witness.Completed).IsEqualTo(1);
        await Assert.That(witness.Errors.Count).IsEqualTo(0);
    }

    /// <summary>A scheduled return signal emits inline when it is built on the immediate sequencer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReturnSignalOnTheImmediateSequencerEmitsInline()
    {
        ReturnSignal<int> signal = new(Seven, Sequencer.Immediate);
        await Assert.That(signal.IsRequiredSubscribeOnCurrentThread()).IsFalse();

        RecordingWitness<int> witness = new();
        using var subscription = signal.Subscribe(witness);

        await Assert.That(witness.Values.SequenceEqual(ExpectedSingleSeven)).IsTrue();
        await Assert.That(witness.Completed).IsEqualTo(1);
    }
}
