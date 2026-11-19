// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies coordinator and witness selector faults, post-terminal guards, and race-arm claims.</summary>
public partial class SignalOperatorParityMixinsTests
{
    /// <summary>Verifies selector exceptions in CombineLatest, ForkJoin, Latch, Pair, and SyncLatest route to downstream <c>OnError</c>.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CombinationCoordinators_WhenSelectorThrows_ForwardErrorAndStop()
    {
        InvalidOperationException boom = new("selector-boom");
        Func<int, int, int> throwingSelector = (_, _) => throw boom;

        using Signal<int> combineLeft = new();
        using Signal<int> combineRight = new();
        RecordingWitness<int> combineObserver = new();
        using (combineLeft.CombineLatest(combineRight, throwingSelector).Subscribe(combineObserver))
        {
            combineLeft.OnNext(One);
            combineRight.OnNext(Two);
        }

        await Assert.That(combineObserver.Errors.Single()).IsSameReferenceAs(boom);

        RecordingWitness<int> forkObserver = new();
        using var forkSub = Signal.Return(One).ForkJoin(Signal.Return(Two), throwingSelector).Subscribe(forkObserver);
        await Assert.That(forkObserver.Errors.Single()).IsSameReferenceAs(boom);

        ScriptedObservable<int> latchLeft = new(static o =>
        {
            o.OnNext(One);
            o.OnNext(Two);
        });
        RecordingWitness<int> latchObserver = new();
        using var latchSub = latchLeft.WithLatestFrom(Signal.Return(One), throwingSelector).Subscribe(latchObserver);
        await Assert.That(latchObserver.Errors.Single()).IsSameReferenceAs(boom);

        RecordingWitness<int> pairObserver = new();
        using var pairSub = Signal.Return(One).Pair(Signal.Return(Two), throwingSelector).Subscribe(pairObserver);
        await Assert.That(pairObserver.Errors.Single()).IsSameReferenceAs(boom);

        RecordingWitness<int> syncObserver = new();
        using var syncSub = new SyncLatestSignal<int, int, int>(Signal.Return(One), Signal.Return(Two), throwingSelector).Subscribe(syncObserver);
        await Assert.That(syncObserver.Errors.Single()).IsSameReferenceAs(boom);
    }

    /// <summary>Verifies Blend, Chain, FromAsyncExternalCancellation, MaxConcurrentBlend, and Merge coordinator guards.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task BlendChainAndMergeCoordinators_PostTerminalAndDisposalGuards_RemainSafe()
    {
        InvalidOperationException boom = new("boom");

        ScriptedObservable<int> dupCompleteInner = new(static o =>
        {
            o.OnCompleted();
            o.OnCompleted();
        });
        ScriptedObservable<IObservable<int>> dupOuter = new(o =>
        {
            o.OnNext(dupCompleteInner);
            o.OnCompleted();
            o.OnCompleted();
        });
        RecordingWitness<int> blendDoneObserver = new();
        using var blendDoneSub = new BlendWitness<int>(blendDoneObserver).Run(dupOuter);
        await Assert.That(blendDoneObserver.Completed).IsEqualTo(One);

        ScriptedObservable<IObservable<int>> errThenSource = new(o =>
        {
            o.OnError(boom);
            o.OnNext(Signal.Return(One));
        });
        RecordingWitness<int> blendErrObserver = new();
        using var blendErrSub = new BlendWitness<int>(blendErrObserver).Run(errThenSource);
        await Assert.That(blendErrObserver.Errors.Single()).IsSameReferenceAs(boom);

        ScriptedObservable<int> throwingSubscribe = new(_ => throw boom);
        RecordingWitness<int> chainThrowObserver = new();
        using var chainThrowSub = new ChainWitness<int>(chainThrowObserver).Run([throwingSubscribe]);
        await Assert.That(chainThrowObserver.Errors.Single()).IsSameReferenceAs(boom);

        var innerDisposed = 0;
        ChainWitness<int>? selfDisposingChain = null;
        var disposeDuringSub = Signal.Create<int>(_ =>
        {
            selfDisposingChain!.Dispose();
            return new ActionDisposable(() => innerDisposed++);
        });
        selfDisposingChain = new(new RecordingWitness<int>());
        _ = selfDisposingChain.Run([disposeDuringSub]);
        await Assert.That(innerDisposed).IsEqualTo(One);

        using AsyncSubscriptionLifetime lifetime = new();
        using FromAsyncExternalCancellation<int> extCancel = new(new RecordingWitness<int>(), lifetime, CancellationToken.None);
        await Assert.That(extCancel.Start()).IsTrue();

        RecordingWitness<int> maxBlendObserver = new();
        using var maxBlendSub = new MaxConcurrentBlendCoordinator<int>(maxBlendObserver).Run([dupCompleteInner], Two);
        await Assert.That(maxBlendObserver.Completed).IsEqualTo(One);

        RecordingWitness<int> mergeErrObserver = new();
        using MergeCoordinator<int> mergeCoordinator = new(mergeErrObserver);
        _ = mergeCoordinator.Run([Signal.Throw<int>(boom), Signal.Return(One)]);
        mergeCoordinator.OnOuterCompleted();
        await Assert.That(mergeErrObserver.Errors.Single()).IsSameReferenceAs(boom);
    }

    /// <summary>Verifies Probe, RaceArms, Recover, Resume, and SelectMany coordinator guards.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ProbeRaceRecoverResumeAndSelectManyCoordinators_PostTerminalGuards_RemainSafe()
    {
        InvalidOperationException boom = new("boom");

        ProbeCoordinator<int> probeCoordinator = new(Signal.Silent<int>(), TimeSpan.FromSeconds(One), Sequencer.Immediate, new RecordingWitness<int>());
        probeCoordinator.Dispose();
        probeCoordinator.ScheduleNext();

        RecordingWitness<int> raceObserver = new();
        using RaceArms<int> arms = new(raceObserver);
        await Assert.That(arms.ClaimWinner(0)).IsTrue();
        await Assert.That(arms.ClaimWinner(One)).IsFalse();
        arms.OnError(One, boom);
        await Assert.That(raceObserver.Errors).IsEmpty();

        ScriptedObservable<int> noisyComplete = new(o =>
        {
            o.OnCompleted();
            o.OnNext(One);
            o.OnError(boom);
            o.OnCompleted();
        });
        RecordingWitness<int> recoverObserver = new();
        using var recoverSub = noisyComplete.Catch<int, InvalidOperationException>(static _ => Signal.Empty<int>()).Subscribe(recoverObserver);
        await Assert.That(recoverObserver.Completed).IsEqualTo(One);

        RecordingWitness<int> resumeObserver = new();
        using var resumeSub = new ResumeSignal<int>(noisyComplete, Signal.Empty<int>()).Subscribe(resumeObserver);
        await Assert.That(resumeObserver.Completed).IsEqualTo(One);

        SelectManyCoordinator<int, int>? selectManyCoordinator = null;
        selectManyCoordinator = new(
            new RecordingWitness<int>(),
            _ =>
            {
                selectManyCoordinator!.Dispose();
                return Signal.Return(One);
            });
        using var selectManySub = selectManyCoordinator.Run(Signal.Return(One));

        SelectManyResultCoordinator<int, int, int>? selectManyResultCoordinator = null;
        selectManyResultCoordinator = new(
            new RecordingWitness<int>(),
            _ =>
            {
                selectManyResultCoordinator!.Dispose();
                return Signal.Return(One);
            },
            static (a, b) => a + b);
        using var selectManyResultSub = selectManyResultCoordinator.Run(Signal.Return(One));
    }

    /// <summary>Verifies Slice, Switch, TaskTerminalWitnessHelper, and WitnessOnSignal post-disposal and fault guards.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SliceSwitchTerminalAndWitnessOnGuards_RemainSafe()
    {
        InvalidOperationException boom = new("boom");

        SliceClosingWitness<int, int> sliceClosing = new(new RecordingWitness<IObservable<int>>(), static () => Signal.Silent<int>());
        sliceClosing.Dispose();
        sliceClosing.Advance(EmptyDisposable.Instance);

        SliceOpeningWitness<int, int, int> sliceOpening = new(new RecordingWitness<IObservable<int>>(), static _ => Signal.Silent<int>());
        sliceOpening.Dispose();
        sliceOpening.Close(null!, EmptyDisposable.Instance);

        SliceTimeCountWitness<int> sliceTimeCount = new(new RecordingWitness<IObservable<int>>(), TimeSpan.FromSeconds(One), Two, Sequencer.Immediate);
        sliceTimeCount.Dispose();
        sliceTimeCount.Tick(0);

        SliceTimeWitness<int> sliceTime = new(new RecordingWitness<IObservable<int>>(), TimeSpan.FromSeconds(One), TimeSpan.FromSeconds(One), Sequencer.Immediate);
        sliceTime.Dispose();
        sliceTime.ScheduleTick();
        sliceTime.Tick(isSpan: true, isShift: true);

        ScriptedObservable<IObservable<int>> nullThenSource = new(static o =>
        {
            o.OnNext(null!);
            o.OnNext(Signal.Return(One));
        });
        RecordingWitness<int> switchNullObserver = new();
        using var switchNullSub = new SwitchWitness<int>(switchNullObserver).Run(nullThenSource);
        await Assert.That(switchNullObserver.Errors.Single()).IsTypeOf<InvalidOperationException>();

        IDisposable? slot = new ActionDisposable(static () => { });
        var stopped = 0;
        var secondDisposed = 0;
        TaskTerminalWitnessHelper.SetSubscription(ref slot, ref stopped, new ActionDisposable(() => secondDisposed++));
        await Assert.That(secondDisposed).IsEqualTo(One);

        using Signal<int> witnessOnSource = new();
        using var witnessOnSub = new WitnessOnSignal<int>(witnessOnSource, Sequencer.Immediate)
            .Subscribe(Witness.Create<int>(_ => throw boom));
        await Assert.That(() => witnessOnSource.OnNext(One)).ThrowsExactly<InvalidOperationException>();
    }
}
