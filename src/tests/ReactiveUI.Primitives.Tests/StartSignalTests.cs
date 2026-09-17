// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests scheduler selection and work results for start signals.</summary>
public sealed class StartSignalTests
{
    /// <summary>The value produced by the start function.</summary>
    private const int ProducedValue = 5;

    /// <summary>Verifies a start signal scheduled on the current-thread sequencer requires the current thread.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StartSignalOnTheCurrentThreadSequencerRequiresTheCurrentThread()
    {
        StartSignal<int> signal = new(static () => ProducedValue, Sequencer.CurrentThread);

        await Assert.That(signal.IsRequiredSubscribeOnCurrentThread()).IsTrue();
    }

    /// <summary>Verifies a start signal scheduled off the current-thread sequencer does not require the current thread.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StartSignalOffTheCurrentThreadSequencerDoesNotRequireTheCurrentThread()
    {
        StartSignal<int> signal = new(static () => ProducedValue, Sequencer.Immediate);

        await Assert.That(signal.IsRequiredSubscribeOnCurrentThread()).IsFalse();
    }

    /// <summary>A default start function runs once, emits its result, and completes.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Run_DefaultFunction_EmitsResultAndCompletes()
    {
        var runs = 0;
        var signal = await Assert.That(Signal.Start(() =>
        {
            runs++;
            return ProducedValue;
        })).IsTypeOf<StartSignal<int>>().And.IsNotNull();
        RecordingWitness<int> observer = new();

        signal.Run(observer);

        await Assert.That(runs).IsEqualTo(1);
        await Assert.That(observer.Values).HasSingleItem();
        await Assert.That(observer.Values[0]).IsEqualTo(ProducedValue);
        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(observer.Errors).IsEmpty();
    }

    /// <summary>A default start action runs once, emits its unit value, and completes.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Run_DefaultAction_EmitsUnitAndCompletes()
    {
        var runs = 0;
        void RunAction() => runs++;
        var signal = await Assert.That(Signal.Start(RunAction)).IsTypeOf<StartSignal>().And.IsNotNull();
        RecordingWitness<RxVoid> observer = new();

        signal.Run(observer);

        await Assert.That(runs).IsEqualTo(1);
        await Assert.That(observer.Values).HasSingleItem();
        await Assert.That(observer.Values[0]).IsEqualTo(RxVoid.Default);
        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(observer.Errors).IsEmpty();
    }

    /// <summary>A failing start function forwards its exception without a value or completion.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Run_FailingFunction_ForwardsError()
    {
        InvalidOperationException fault = new("function failed");
        var signal = await Assert.That(Signal.Start<int>(() => throw fault)).IsTypeOf<StartSignal<int>>().And.IsNotNull();
        RecordingWitness<int> observer = new();

        signal.Run(observer);

        await Assert.That(observer.Errors).HasSingleItem();
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(fault);
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>A failing start action forwards its exception without a value or completion.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Run_FailingAction_ForwardsError()
    {
        InvalidOperationException fault = new("action failed");
        void RunAction() => throw fault;
        var signal = await Assert.That(Signal.Start(RunAction)).IsTypeOf<StartSignal>().And.IsNotNull();
        RecordingWitness<RxVoid> observer = new();

        signal.Run(observer);

        await Assert.That(observer.Errors).HasSingleItem();
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(fault);
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(0);
    }
}
