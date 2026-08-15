// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="Witness"/> routing and safe-termination contracts.</summary>
public class WitnessTests
{
    /// <summary>A reusable value for one.</summary>
    private const int One = 1;

    /// <summary>A reusable value for two.</summary>
    private const int Two = 2;

    /// <summary>A reusable value for three.</summary>
    private const int Three = 3;

    /// <summary>A reusable value for four.</summary>
    private const int Four = 4;

    /// <summary>A reusable value for ten.</summary>
    private const int Ten = 10;

    /// <summary>A reusable value for twelve.</summary>
    private const int Twelve = 12;

    /// <summary>A reusable value for thirteen.</summary>
    private const int Thirteen = 13;

    /// <summary>A reusable value for fourteen.</summary>
    private const int Fourteen = 14;

    /// <summary>Timeout used when waiting for thread-pool scheduled observer callbacks.</summary>
    private const int TimeoutSeconds = 2;

    /// <summary>Shared state value.</summary>
    private const string State = "state";

    /// <summary>Expected two-only value sequence.</summary>
    private static readonly int[] ExpectedTwoOnly = [Two];

    /// <summary>Expected handled error sequence.</summary>
    private static readonly string[] ExpectedHandledErrors = ["handled"];

    /// <summary>Expected safe witness event sequence.</summary>
    private static readonly string[] ExpectedSafeEvents = ["next:3", "completed"];

    /// <summary>Expected values from thread-pool observer dispatch.</summary>
    private static readonly int[] WitnessOnExpected = [One];

    /// <summary>Verifies delegate witnesses route next, error, and completion callbacks.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WitnessCreateRoutesCallbacks()
    {
        const int ObservedValue = 7;
        List<string> calls = [];
        InvalidOperationException error = new("boom");
        var witness = Witness.Create<int>(
            value => calls.Add($"N{value}"),
            ex => calls.Add($"E{ex.Message}"),
            () => calls.Add("C"));
        witness.OnNext(ObservedValue);
        witness.OnError(error);
        witness.OnCompleted();
        string[] expected = [$"N{ObservedValue}", "Eboom", "C"];
        await Assert.That(calls.SequenceEqual(expected)).IsTrue();
    }

    /// <summary>Verifies safe witnesses ignore notifications after termination and dispose once.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SafeWitnessIgnoresSignalsAfterTerminalAndDisposesOnce()
    {
        const int FirstValue = 1;
        const int LateValue = 2;
        List<string> calls = [];
        var disposed = 0;
        var witness = Witness.Safe(
            Witness.Create<int>(
                value => calls.Add($"N{value}"),
                ex => calls.Add($"E{ex.Message}"),
                () => calls.Add("C")),
            new ActionDisposable(() => disposed++));
        witness.OnNext(FirstValue);
        witness.OnCompleted();
        witness.OnNext(LateValue);
        witness.OnError(new InvalidOperationException("late"));
        witness.OnCompleted();
        string[] expected = [$"N{FirstValue}", "C"];
        await Assert.That(calls.SequenceEqual(expected)).IsTrue();
        await Assert.That(disposed).IsEqualTo(1);
    }

    /// <summary>Covers internal witness implementations and safe observer terminal behavior.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WitnessesCoverDisposedThrowEmptyAndSafeBranches()
    {
        _ = Assert.Throws<ObjectDisposedException>(static () => DisposedWitness<int>.Instance.OnNext(One));
        _ = Assert.Throws<ObjectDisposedException>(DisposedWitness<int>.Instance.OnCompleted);
        _ = Assert.Throws<ObjectDisposedException>(static () =>
            DisposedWitness<int>.Instance.OnError(new InvalidOperationException("disposed")));
        ThrowWitness<int>.Instance.OnNext(One);
        ThrowWitness<int>.Instance.OnCompleted();
        _ = Assert.Throws<InvalidOperationException>(static () =>
            ThrowWitness<int>.Instance.OnError(new InvalidOperationException("throw")));
        List<int> values = [];
        List<string> errors = [];
        var completed = 0;
        EmptyWitness<int>.Instance.OnNext(One);
        new EmptyWitness<int>(values.Add).OnNext(Two);
        new EmptyWitness<int>(values.Add, ex => errors.Add(ex.Message))
            .OnError(new InvalidOperationException("handled"));
        new EmptyWitness<int>(values.Add, () => completed++).OnCompleted();
        new EmptyWitness<int>(values.Add, ex => errors.Add(ex.Message), () => completed++).OnCompleted();
        _ = Assert.Throws<InvalidOperationException>(() =>
            new EmptyWitness<int>(values.Add).OnError(new InvalidOperationException("rethrown")));
        await Assert.That(values.SequenceEqual(ExpectedTwoOnly)).IsTrue();
        await Assert.That(errors.SequenceEqual(ExpectedHandledErrors)).IsTrue();
        await Assert.That(completed).IsEqualTo(Two);
        _ = Assert.Throws<ArgumentNullException>(static () => Witness.Create<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Witness.Create<int>(static _ => { }, (Action<Exception>)null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Witness.Create<int>(static _ => { }, (Action)null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Witness.Create<int>(static _ => { }, static _ => { }, null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Witness.Safe<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Witness.Safe(Witness.Create<int>(static _ => { }), null!));
        List<string> events = [];
        var cancelDisposed = 0;
        var safe = Witness.Safe(
            Witness.Create<int>(
                value => events.Add($"next:{value}"),
                ex => events.Add($"error:{ex.Message}"),
                () => events.Add("completed")),
            new ActionDisposable(() => cancelDisposed++));
        safe.OnNext(Three);
        safe.OnCompleted();
        safe.OnCompleted();
        safe.OnNext(Four);
        safe.OnError(new InvalidOperationException("late"));
        await Assert.That(events.SequenceEqual(ExpectedSafeEvents)).IsTrue();
        await Assert.That(cancelDisposed).IsEqualTo(1);
        var throwingCancel = 0;
        var throwing = Witness.Safe(
            Witness.Create<int>(static _ => throw new InvalidOperationException("next-failed")),
            new ActionDisposable(() => throwingCancel++));
        _ = Assert.Throws<InvalidOperationException>(() => throwing.OnNext(One));
        throwing.OnNext(Two);
        await Assert.That(throwingCancel).IsEqualTo(1);
        _ = Assert.Throws<ArgumentNullException>(() => safe.OnError(null!));
    }

    /// <summary>Covers the thread-pool-specialized witness dispatch implementation.</summary>
    /// <returns>A task representing asynchronous observer dispatch.</returns>
    [Test]
    public async Task WitnessOnThreadPoolDispatchesNextCompletedAndErrorSignals()
    {
        List<int> values = [];
        TaskCompletionSource completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using (Signal.FromEnumerable(WitnessOnExpected)
                   .WitnessOn(ThreadPoolSequencer.Instance)
                   .Subscribe(values.Add, completion.SetException, completion.SetResult))
        {
            await WaitForAsync(completion.Task);
        }

        await Assert.That(values.Count <= WitnessOnExpected.Length).IsTrue();
        InvalidOperationException error = new("thread-pool");
        TaskCompletionSource<Exception> observed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using (Signal.Fail<int>(error).WitnessOn(ThreadPoolSequencer.Instance)
                   .Subscribe(static _ => { }, observed.SetResult, static () => { }))
        {
            await Assert.That(await WaitForAsync(observed.Task)).IsSameReferenceAs(error);
        }
    }

    /// <summary>Covers callback, forwarding, and stateful witness contracts.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WitnessImplementationsForwardNotificationsAndFallbackErrors()
    {
        AssertWitnessConstructorsRejectMissingCallbacks();
        await AssertCallbackWitnessForwardsEachNotification();
        await AssertForwardingWitnessForwardsEachNotification();
        await AssertStatefulWitnessForwardsEachNotificationWithItsState();
        await AssertSafeWitnessIgnoresNotificationsAfterTheTerminal();
    }

    /// <summary>Covers witness factory and safe-wrapper null-callback validation.</summary>
    [Test]
    public void WitnessFactoriesValidateNullCallbacks()
    {
        _ = Assert.Throws<ArgumentNullException>(static () => Witness.Create<int>(null!, static _ => { }, static () => { }));
        _ = Assert.Throws<ArgumentNullException>(static () => Witness.Create<int>(static _ => { }, null!, static () => { }));
        _ = Assert.Throws<ArgumentNullException>(static () => Witness.Create<int>(static _ => { }, static _ => { }, null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Witness.Safe<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Witness.Safe(Witness.Create<int>(static _ => { }), null!));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            Witness.Safe(new Recorder<int>(), new RecordingDisposable()).OnError(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Witness.Safe(new Recorder<int>()).OnError(null!));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            Witness.Create<int>(static _ => { }, static _ => { }, static () => { }).OnError(null!));
    }

    /// <summary>Covers safe-witness error forwarding and post-terminal suppression branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SafeWitnessForwardsErrorAndIgnoresLateSignals()
    {
        var cancelDisposed = false;
        Witness.SafeWitness<int> safe = new(
            new ThrowingWitness<int>(throwOnError: true),
            new ActionDisposable(() => cancelDisposed = true));
        _ = Assert.Throws<InvalidOperationException>(() => safe.OnError(new InvalidOperationException("safe")));
        await Assert.That(cancelDisposed).IsTrue();
        safe.OnError(new InvalidOperationException("ignored"));
    }

    /// <summary>Verifies task terminal witnesses validate null callbacks and error arguments.</summary>
    [Test]
    public void TaskTerminalWitnessesValidateNullPredicatesAndErrors()
    {
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            TaskAnyWitness<int> invalid = new(null!, CancellationToken.None);
            GC.KeepAlive(invalid);
        });
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            TaskCountWitness<int> invalid = new(null!, CancellationToken.None);
            GC.KeepAlive(invalid);
        });
        _ = Assert.Throws<ArgumentNullException>(static () =>
            new TaskAnyWitness<int>(CancellationToken.None).OnError(null!));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            new TaskCountWitness<int>(CancellationToken.None).OnError(null!));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            new TaskAnyWitness<int>(CancellationToken.None).SetSubscription(null!));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            new TaskCountWitness<int>(CancellationToken.None).SetSubscription(null!));
    }

    /// <summary>Verifies any-task witnesses complete true, false, fault, and ignore late notifications.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskAnyWitnessCompletesTrueFalseErrorsAndIgnoresLateSignals()
    {
        TaskAnyWitness<int> any = new(CancellationToken.None);
        RecordingDisposable anySubscription = new();
        any.SetSubscription(anySubscription);
        any.OnNext(One);
        any.OnNext(Two);
        any.OnCompleted();
        await Assert.That(await WaitForAsync(any.Task)).IsTrue();
        await Assert.That(anySubscription.DisposeCount).IsEqualTo(One);

        TaskAnyWitness<int> unmatched = new(static value => value > Four, CancellationToken.None);
        unmatched.OnNext(One);
        unmatched.OnCompleted();
        unmatched.OnNext(Four);
        await Assert.That(await WaitForAsync(unmatched.Task)).IsFalse();

        InvalidOperationException predicateError = new("any-predicate");
        TaskAnyWitness<int> predicateFault = new(_ => throw predicateError, CancellationToken.None);
        predicateFault.OnNext(One);
        var observedPredicateError = await Assert.That(() => WaitForAsync(predicateFault.Task))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(observedPredicateError).IsSameReferenceAs(predicateError);

        InvalidOperationException sourceError = new("any-source");
        TaskAnyWitness<int> sourceFault = new(CancellationToken.None);
        sourceFault.OnError(sourceError);
        sourceFault.OnCompleted();
        var observedSourceError = await Assert.That(() => WaitForAsync(sourceFault.Task))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(observedSourceError).IsSameReferenceAs(sourceError);
    }

    /// <summary>Verifies count-task witnesses count matches, fault, and ignore late notifications.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskCountWitnessCountsMatchesErrorsAndIgnoresLateSignals()
    {
        TaskCountWitness<int> all = new(CancellationToken.None);
        all.OnNext(One);
        all.OnNext(Two);
        all.OnCompleted();
        all.OnNext(Three);
        await Assert.That(await WaitForAsync(all.Task)).IsEqualTo(Two);

        TaskCountWitness<int> even = new(static value => value % Two == 0, CancellationToken.None);
        even.OnNext(One);
        even.OnNext(Two);
        even.OnNext(Three);
        even.OnNext(Four);
        even.OnCompleted();
        even.OnError(new InvalidOperationException("late"));
        await Assert.That(await WaitForAsync(even.Task)).IsEqualTo(Two);

        InvalidOperationException predicateError = new("count-predicate");
        TaskCountWitness<int> predicateFault = new(_ => throw predicateError, CancellationToken.None);
        predicateFault.OnNext(One);
        var observedPredicateError = await Assert.That(() => WaitForAsync(predicateFault.Task))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(observedPredicateError).IsSameReferenceAs(predicateError);

        InvalidOperationException sourceError = new("count-source");
        TaskCountWitness<int> sourceFault = new(CancellationToken.None);
        sourceFault.OnError(sourceError);
        sourceFault.OnNext(Two);
        var observedSourceError = await Assert.That(() => WaitForAsync(sourceFault.Task))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(observedSourceError).IsSameReferenceAs(sourceError);
    }

    /// <summary>Verifies task terminal witnesses dispose subscriptions on terminal, cancel, and explicit disposal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskTerminalWitnessesDisposeSubscriptionsOnTerminalCancelAndDispose()
    {
        TaskAnyWitness<int> completed = new(CancellationToken.None);
        RecordingDisposable completedSubscription = new();
        completed.SetSubscription(completedSubscription);
        completed.OnCompleted();
        completed.Dispose();
        await Assert.That(await WaitForAsync(completed.Task)).IsFalse();
        await Assert.That(completedSubscription.DisposeCount).IsEqualTo(One);

        TaskAnyWitness<int> alreadyStopped = new(CancellationToken.None);
        alreadyStopped.OnCompleted();
        RecordingDisposable lateSubscription = new();
        alreadyStopped.SetSubscription(lateSubscription);
        await Assert.That(lateSubscription.DisposeCount).IsEqualTo(One);

        using CancellationTokenSource cancellation = new();
        TaskAnyWitness<int> canceled = new(cancellation.Token);
        RecordingDisposable canceledSubscription = new();
        canceled.RegisterCancellation();
        canceled.SetSubscription(canceledSubscription);
        await cancellation.CancelAsync();
        await Assert.That(canceled.Task.IsCanceled).IsTrue();
        await Assert.That(canceledSubscription.DisposeCount).IsEqualTo(One);
        canceled.Dispose();
        await Assert.That(canceledSubscription.DisposeCount).IsEqualTo(One);

        TaskCountWitness<int> disposed = new(CancellationToken.None);
        RecordingDisposable disposedSubscription = new();
        disposed.SetSubscription(disposedSubscription);
        disposed.Dispose();
        disposed.Dispose();
        await Assert.That(disposedSubscription.DisposeCount).IsEqualTo(One);
    }

    /// <summary>Verifies direct empty-state witnesses emit terminal state and dispose upstream subscriptions.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task IsEmptyWitnessEmitsTerminalStateAndIgnoresLateSignals()
    {
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            IsEmptyWitness<int> invalid = new(null!);
            GC.KeepAlive(invalid);
        });

        RecordingWitness<bool> emptyObserver = new();
        RecordingDisposable emptySubscription = new();
        IsEmptyWitness<int> empty = new(emptyObserver);
        empty.SetSubscription(emptySubscription);
        empty.OnCompleted();
        empty.OnCompleted();
        empty.OnNext(One);
        await Assert.That(emptyObserver.Values.SequenceEqual([true])).IsTrue();
        await Assert.That(emptyObserver.Completed).IsEqualTo(One);
        await Assert.That(emptySubscription.DisposeCount).IsEqualTo(One);

        RecordingWitness<bool> valueObserver = new();
        RecordingDisposable valueSubscription = new();
        IsEmptyWitness<int> value = new(valueObserver);
        value.SetSubscription(valueSubscription);
        value.OnNext(One);
        value.OnError(new InvalidOperationException("late"));
        await Assert.That(valueObserver.Values.SequenceEqual([false])).IsTrue();
        await Assert.That(valueObserver.Completed).IsEqualTo(One);
        await Assert.That(valueSubscription.DisposeCount).IsEqualTo(One);

        InvalidOperationException expected = new("is-empty");
        RecordingWitness<bool> errorObserver = new();
        IsEmptyWitness<int> error = new(errorObserver);
        error.OnError(expected);
        error.OnCompleted();
        await Assert.That(errorObserver.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(errorObserver.Completed).IsEqualTo(0);
    }

    /// <summary>Verifies collect witnesses emit immediate, scheduled, final, and error batches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CollectWitnessEmitsImmediateScheduledFinalAndErrorBatches()
    {
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            CollectWitness<int> invalid = new(null!);
            GC.KeepAlive(invalid);
        });
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            CollectWitness<int> invalid = new(new RecordingWitness<IList<int>>(), TimeSpan.FromTicks(One), null!);
            GC.KeepAlive(invalid);
        });

        RecordingWitness<IList<int>> immediateObserver = new();
        CollectWitness<int> immediate = new(immediateObserver);
        immediate.OnNext(One);
        immediate.OnNext(Two);
        immediate.OnCompleted();
        immediate.OnNext(Three);
        await Assert.That(immediateObserver.Values.Select(static batch => batch.ToArray()).SelectMany(static batch => batch)
            .SequenceEqual([One, Two])).IsTrue();
        await Assert.That(immediateObserver.Completed).IsEqualTo(One);

        RecordingWitness<IList<int>> scheduledObserver = new();
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        CollectWitness<int> scheduled = new(scheduledObserver, TimeSpan.FromTicks(One), clock);
        scheduled.SetSubscription(new RecordingDisposable());
        scheduled.OnNext(One);
        scheduled.OnNext(Two);
        clock.AdvanceBy(TimeSpan.FromTicks(One));
        scheduled.OnNext(Three);
        scheduled.OnCompleted();
        scheduled.OnError(new InvalidOperationException("late"));
        await Assert.That(scheduledObserver.Values[0].SequenceEqual([One, Two])).IsTrue();
        await Assert.That(scheduledObserver.Values[1].SequenceEqual([Three])).IsTrue();
        await Assert.That(scheduledObserver.Completed).IsEqualTo(One);

        InvalidOperationException expected = new("collect");
        RecordingWitness<IList<int>> errorObserver = new();
        CollectWitness<int> error = new(errorObserver);
        error.OnError(expected);
        error.OnError(new InvalidOperationException("late"));
        await Assert.That(errorObserver.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(errorObserver.Errors.Count).IsEqualTo(One);
    }

    /// <summary>Verifies chain witnesses concatenate sources and forward null-source failures.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ChainWitnessConcatenatesSourcesAndForwardsNullSourceFailures()
    {
        RecordingWitness<int> fixedObserver = new();
        using (new ChainWitness<int>(fixedObserver).Run(Signal.Emit(One), Signal.Emit(Two)))
        {
            await Assert.That(fixedObserver.Values.SequenceEqual([One, Two])).IsTrue();
            await Assert.That(fixedObserver.Completed).IsEqualTo(One);
        }

        RecordingWitness<int> enumerableObserver = new();
        using (new ChainWitness<int>(enumerableObserver).Run([Signal.Emit(Three), Signal.Emit(Four)]))
        {
            await Assert.That(enumerableObserver.Values.SequenceEqual([Three, Four])).IsTrue();
            await Assert.That(enumerableObserver.Completed).IsEqualTo(One);
        }

        RecordingWitness<int> errorObserver = new();
        using (new ChainWitness<int>(errorObserver).Run([Signal.Emit(One), null!]))
        {
            await Assert.That(errorObserver.Values.SequenceEqual([One])).IsTrue();
            await Assert.That(errorObserver.Errors[0].Message).IsEqualTo("Chain source contained null.");
        }
    }

    /// <summary>Verifies blend witnesses merge active sources, complete once, and forward the first error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task BlendWitnessMergesSourcesCompletesOnceAndForwardsFirstError()
    {
        RecordingWitness<int> mergedObserver = new();
        Signal<int> first = new();
        Signal<int> second = new();
        using (new BlendWitness<int>(mergedObserver).Run([first, second]))
        {
            first.OnNext(One);
            second.OnNext(Two);
            first.OnCompleted();
            await Assert.That(mergedObserver.Completed).IsEqualTo(0);
            second.OnCompleted();
            await Assert.That(mergedObserver.Values.SequenceEqual([One, Two])).IsTrue();
            await Assert.That(mergedObserver.Completed).IsEqualTo(One);
        }

        InvalidOperationException expected = new("blend");
        RecordingWitness<int> errorObserver = new();
        Signal<int> failing = new();
        Signal<int> late = new();
        using (new BlendWitness<int>(errorObserver).Run([failing, late]))
        {
            failing.OnError(expected);
            late.OnNext(Three);
            late.OnError(new InvalidOperationException("late"));
            await Assert.That(errorObserver.Errors[0]).IsSameReferenceAs(expected);
            await Assert.That(errorObserver.Errors.Count).IsEqualTo(One);
            await Assert.That(errorObserver.Values.Count).IsEqualTo(0);
        }
    }

    /// <summary>Verifies SelectMany coordinators wait for active inners and forward selector failures.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SelectManyCoordinatorWaitsForInnerCompletionAndForwardsFailures()
    {
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            SelectManyCoordinator<int, int> invalid = new(null!, static _ => Signal.Emit(One));
            GC.KeepAlive(invalid);
        });
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            SelectManyCoordinator<int, int> invalid =
                new(new RecordingWitness<int>(), (Func<int, IObservable<int>>)null!);
            GC.KeepAlive(invalid);
        });
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            SelectManyCoordinator<int, int> invalid =
                new(new RecordingWitness<int>(), (IObservable<int>)null!);
            GC.KeepAlive(invalid);
        });

        RecordingWitness<int> observer = new();
        Signal<int> outer = new();
        Signal<int> inner = new();
        using (new SelectManyCoordinator<int, int>(observer, _ => inner).Run(outer))
        {
            outer.OnNext(One);
            inner.OnNext(Two);
            outer.OnCompleted();
            await Assert.That(observer.Completed).IsEqualTo(0);
            inner.OnCompleted();
            await Assert.That(observer.Values.SequenceEqual([Two])).IsTrue();
            await Assert.That(observer.Completed).IsEqualTo(One);
        }

        RecordingWitness<int> repeated = new();
        using (new SelectManyCoordinator<int, int>(repeated, Signal.Emit(Three))
                   .Run(Signal.FromEnumerable([One, Two])))
        {
            await Assert.That(repeated.Values.SequenceEqual([Three, Three])).IsTrue();
            await Assert.That(repeated.Completed).IsEqualTo(One);
        }

        InvalidOperationException expected = new("select-many");
        RecordingWitness<int> failed = new();
        SelectManyCoordinator<int, int> throwing = new(failed, _ => throw expected);
        throwing.OnNext(One);
        throwing.OnNext(Two);
        await Assert.That(failed.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(failed.Errors.Count).IsEqualTo(One);

        RecordingWitness<int> nullInner = new();
        SelectManyCoordinator<int, int> nullCoordinator = new(nullInner, static _ => null!);
        nullCoordinator.OnNext(One);
        await Assert.That(nullInner.Errors[0].Message).IsEqualTo("Blend source contained null.");
    }

    /// <summary>Verifies SelectMany result coordinators project values and forward inner selector failures.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SelectManyResultCoordinatorProjectsValuesAndForwardsFailures()
    {
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            SelectManyResultCoordinator<int, int, string> invalid =
                new(null!, static _ => Signal.Emit(One), static (_, _) => string.Empty);
            GC.KeepAlive(invalid);
        });
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            SelectManyResultCoordinator<int, int, string> invalid = new(
                new RecordingWitness<string>(),
                null!,
                static (_, _) => string.Empty);
            GC.KeepAlive(invalid);
        });
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            SelectManyResultCoordinator<int, int, string> invalid = new(
                new RecordingWitness<string>(),
                static _ => Signal.Emit(One),
                null!);
            GC.KeepAlive(invalid);
        });

        RecordingWitness<string> observer = new();
        Signal<int> outer = new();
        Signal<int> inner = new();
        using (new SelectManyResultCoordinator<int, int, string>(
                   observer,
                   _ => inner,
                   static (outerValue, innerValue) => $"{outerValue}:{innerValue}").Run(outer))
        {
            outer.OnNext(One);
            inner.OnNext(Two);
            outer.OnCompleted();
            await Assert.That(observer.Completed).IsEqualTo(0);
            inner.OnCompleted();
            await Assert.That(observer.Values.SequenceEqual([$"{One}:{Two}"])).IsTrue();
            await Assert.That(observer.Completed).IsEqualTo(One);
        }

        InvalidOperationException selectorError = new("collection");
        RecordingWitness<string> collectionFailed = new();
        SelectManyResultCoordinator<int, int, string> collection = new(
            collectionFailed,
            _ => throw selectorError,
            static (_, _) => string.Empty);
        collection.OnNext(One);
        await Assert.That(collectionFailed.Errors[0]).IsSameReferenceAs(selectorError);

        InvalidOperationException resultError = new("result");
        RecordingWitness<string> resultFailed = new();
        SelectManyResultCoordinator<int, int, string> result = new(
            resultFailed,
            static _ => Signal.Emit(Two),
            (_, _) => throw resultError);
        result.OnNext(One);
        await Assert.That(resultFailed.Errors[0]).IsSameReferenceAs(resultError);
    }

    /// <summary>Verifies merge coordinators wait for active sources and forward the first terminal error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task MergeCoordinatorWaitsForActiveSourcesAndForwardsFirstError()
    {
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            MergeCoordinator<int> invalid = new(null!);
            GC.KeepAlive(invalid);
        });

        RecordingWitness<int> empty = new();
        using (new MergeCoordinator<int>(empty).Run([]))
        {
            await Assert.That(empty.Completed).IsEqualTo(One);
            await Assert.That(empty.Values.Count).IsEqualTo(0);
        }

        RecordingWitness<int> synchronous = new();
        using (new MergeCoordinator<int>(synchronous).Run([Signal.Emit(One), Signal.Emit(Two), Signal.Emit(Three)]))
        {
            await Assert.That(synchronous.Values.SequenceEqual([One, Two, Three])).IsTrue();
            await Assert.That(synchronous.Completed).IsEqualTo(One);
        }

        RecordingWitness<int> observer = new();
        Signal<int> first = new();
        Signal<int> second = new();
        using (new MergeCoordinator<int>(observer).Run(first, second))
        {
            first.OnNext(One);
            second.OnNext(Two);
            first.OnCompleted();
            await Assert.That(observer.Completed).IsEqualTo(0);
            second.OnCompleted();
            await Assert.That(observer.Values.SequenceEqual([One, Two])).IsTrue();
            await Assert.That(observer.Completed).IsEqualTo(One);
        }

        RecordingWitness<int> nullSource = new();
        MergeCoordinator<int> nullCoordinator = new(nullSource);
        nullCoordinator.OnSource(null);
        nullCoordinator.OnSource(Signal.Emit(Three));
        await Assert.That(nullSource.Errors[0].Message).IsEqualTo("Blend source contained null.");
        await Assert.That(nullSource.Values.Count).IsEqualTo(0);

        InvalidOperationException expected = new("merge");
        RecordingWitness<int> error = new();
        MergeCoordinator<int> failing = new(error);
        failing.OnAnyError(expected);
        failing.OnAnyError(new InvalidOperationException("late"));
        await Assert.That(error.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(error.Errors.Count).IsEqualTo(One);
    }

    /// <summary>Verifies race witnesses forward only the winning source and ignore late candidates.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RaceWitnessForwardsOnlyWinningSourceAndIgnoresLateCandidates()
    {
        InvalidOperationException outerFailure = new("race-outer");
        RecordingWitness<int> outerError = new();
        Signal<IObservable<int>> outerFailureSource = new();
        using (new RaceWitness<int>(outerError).Run(outerFailureSource))
        {
            outerFailureSource.OnError(outerFailure);
            await Assert.That(outerError.Errors[0]).IsSameReferenceAs(outerFailure);
            await Assert.That(outerError.Completed).IsEqualTo(0);
        }

        RecordingWitness<int> outerCompleted = new();
        Signal<IObservable<int>> outer = new();
        Signal<int> completingWinner = new();
        using (new RaceWitness<int>(outerCompleted).Run(outer))
        {
            outer.OnNext(completingWinner);
            outer.OnCompleted();
            await Assert.That(outerCompleted.Completed).IsEqualTo(0);
            completingWinner.OnCompleted();
            await Assert.That(outerCompleted.Completed).IsEqualTo(One);
        }

        RecordingWitness<int> observer = new();
        Signal<int> first = new();
        Signal<int> second = new();
        Signal<int> third = new();
        using (new RaceWitness<int>(observer).Run([first, second, third]))
        {
            second.OnNext(Two);
            first.OnNext(One);
            first.OnCompleted();
            third.OnError(new InvalidOperationException("late"));
            second.OnNext(Three);
            second.OnCompleted();
            await Assert.That(observer.Values.SequenceEqual([Two, Three])).IsTrue();
            await Assert.That(observer.Completed).IsEqualTo(One);
            await Assert.That(observer.Errors.Count).IsEqualTo(0);
        }

        InvalidOperationException expected = new("race");
        RecordingWitness<int> error = new();
        Signal<int> winner = new();
        Signal<int> loser = new();
        using (new RaceWitness<int>(error).Run([winner, loser]))
        {
            winner.OnError(expected);
            loser.OnNext(One);
            await Assert.That(error.Errors[0]).IsSameReferenceAs(expected);
            await Assert.That(error.Values.Count).IsEqualTo(0);
        }

        RecordingWitness<int> nullSource = new();
        using (new RaceWitness<int>(nullSource).Run([null!]))
        {
            await Assert.That(nullSource.Errors[0].Message).IsEqualTo("Race source contained null.");
            await Assert.That(nullSource.Values.Count).IsEqualTo(0);
        }
    }

    /// <summary>Verifies race witnesses dispose losing source subscriptions once a winner emerges.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RaceWitnessDisposesLosingSubscriptionAfterWinnerEmerges()
    {
        RecordingDisposableObservable<int> winner = new();
        RecordingDisposableObservable<int> loser = new();
        RecordingWitness<int> observer = new();
        using (new RaceWitness<int>(observer).Run([winner, loser]))
        {
            winner.Observer!.OnNext(One);
            await Assert.That(loser.DisposeCount).IsEqualTo(1);
            await Assert.That(winner.DisposeCount).IsEqualTo(0);
            await Assert.That(observer.Values.SequenceEqual([One])).IsTrue();
            await Assert.That(observer.Errors.Count).IsEqualTo(0);
        }

        await Assert.That(winner.DisposeCount).IsEqualTo(1);
        await Assert.That(loser.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies create witnesses dispose cancellation resources on terminal and disposal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CreateWitnessDisposesCancellationOnTerminalAndDisposal()
    {
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            CreateWitness<int> invalid = new(null!);
            GC.KeepAlive(invalid);
        });

        RecordingWitness<int> completedObserver = new();
        RecordingDisposable completedCancel = new();
        CreateWitness<int> completed = new(completedObserver);
        completed.SetCancel(completedCancel);
        _ = Assert.Throws<ArgumentNullException>(() => completed.SetCancel(null!));
        completed.OnNext(One);
        completed.OnCompleted();
        completed.OnNext(Two);
        completed.OnError(new InvalidOperationException("late"));
        await Assert.That(completedObserver.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(completedObserver.Completed).IsEqualTo(One);
        await Assert.That(completedCancel.DisposeCount).IsEqualTo(One);

        InvalidOperationException expected = new("publish-selector");
        RecordingWitness<int> errorObserver = new();
        RecordingDisposable firstCancel = new();
        RecordingDisposable duplicateCancel = new();
        CreateWitness<int> error = new(errorObserver);
        error.SetCancel(firstCancel);
        error.SetCancel(duplicateCancel);
        error.OnError(expected);
        error.OnCompleted();
        await Assert.That(errorObserver.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(firstCancel.DisposeCount).IsEqualTo(One);
        await Assert.That(duplicateCancel.DisposeCount).IsEqualTo(One);

        RecordingDisposable completionThrowCancel = new();
        CreateWitness<int> completionThrow = new(new ThrowingWitness<int>(throwOnCompleted: true));
        completionThrow.SetCancel(completionThrowCancel);
        _ = Assert.Throws<InvalidOperationException>(completionThrow.OnCompleted);
        await Assert.That(completionThrowCancel.DisposeCount).IsEqualTo(One);

        RecordingDisposable errorThrowCancel = new();
        CreateWitness<int> errorThrow = new(new ThrowingWitness<int>(throwOnError: true));
        errorThrow.SetCancel(errorThrowCancel);
        _ = Assert.Throws<InvalidOperationException>(() => errorThrow.OnError(expected));
        await Assert.That(errorThrowCancel.DisposeCount).IsEqualTo(One);

        CreateWitness<int> disposed = new(new RecordingWitness<int>());
        RecordingDisposable lateCancel = new();
        disposed.Dispose();
        disposed.SetCancel(lateCancel);
        disposed.OnNext(Three);
        await Assert.That(lateCancel.DisposeCount).IsEqualTo(One);
    }

    /// <summary>Verifies buffer-each witnesses emit single-value lists and suppress late notifications.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task BufferEachWitnessEmitsSingleValueListsAndSuppressesLateNotifications()
    {
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            BufferEachWitness<int> invalid = new(null!);
            GC.KeepAlive(invalid);
        });

        RecordingWitness<IList<int>> observer = new();
        RecordingDisposable firstSubscription = new();
        RecordingDisposable secondSubscription = new();
        BufferEachWitness<int> buffer = new(observer);
        buffer.SetSubscription(firstSubscription);
        buffer.SetSubscription(secondSubscription);
        _ = Assert.Throws<ArgumentNullException>(() => buffer.SetSubscription(null!));
        buffer.OnNext(One);
        buffer.OnNext(Two);
        buffer.OnCompleted();
        buffer.OnNext(Three);
        await Assert.That(firstSubscription.DisposeCount).IsEqualTo(One);
        await Assert.That(observer.Values[0].SequenceEqual([One])).IsTrue();
        await Assert.That(observer.Values[1].SequenceEqual([Two])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(One);
        await Assert.That(secondSubscription.DisposeCount).IsEqualTo(One);

        InvalidOperationException expected = new("buffer");
        RecordingWitness<IList<int>> errorObserver = new();
        BufferEachWitness<int> error = new(errorObserver);
        error.OnError(expected);
        error.OnError(new InvalidOperationException("late"));
        await Assert.That(errorObserver.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(errorObserver.Errors.Count).IsEqualTo(One);

        BufferEachWitness<int> disposed = new(new RecordingWitness<IList<int>>());
        RecordingDisposable lateSubscription = new();
        disposed.Dispose();
        disposed.SetSubscription(lateSubscription);
        await Assert.That(lateSubscription.DisposeCount).IsEqualTo(One);
    }

    /// <summary>Verifies range-with-latest fast-path signals combine each left value with the final right value.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RangeWithLatestSignalCombinesEachLeftValueWithFinalRightValue()
    {
        RangeWithLatestSignal<int> signal = new(
            new(One, Three),
            new(Ten, Two),
            static (left, right) => left + right);
        RecordingWitness<int> observer = new();
        using (signal.Subscribe(observer))
        {
            await Assert.That(observer.Values.SequenceEqual([Twelve, Thirteen, Fourteen])).IsTrue();
            await Assert.That(observer.Completed).IsEqualTo(One);
        }

        var selectorCalls = 0;
        RangeWithLatestSignal<int> emptyLeft = new(
            new(One, 0),
            new(Ten, Two),
            (left, right) =>
            {
                selectorCalls++;
                return left + right;
            });
        RecordingWitness<int> emptyObserver = new();
        using (emptyLeft.Subscribe(emptyObserver))
        {
            await Assert.That(selectorCalls).IsEqualTo(0);
            await Assert.That(emptyObserver.Values.Count).IsEqualTo(0);
            await Assert.That(emptyObserver.Completed).IsEqualTo(One);
        }

        InvalidOperationException expected = new("range-with-latest");
        RangeWithLatestSignal<int> throwing = new(
            new(One, Three),
            new(Ten, Two),
            (left, right) => left == Two ? throw expected : left + right);
        RecordingWitness<int> throwingObserver = new();
        _ = Assert.Throws<InvalidOperationException>(() => throwing.Subscribe(throwingObserver));
        await Assert.That(throwingObserver.Values.SequenceEqual([Twelve])).IsTrue();
        await Assert.That(throwingObserver.Completed).IsEqualTo(0);

        _ = Assert.Throws<ArgumentNullException>(() => signal.Subscribe(null!));
    }

    /// <summary>
    /// Verifies removing an observer the list never held leaves the witness unchanged, so a stray unsubscribe
    /// cannot drop a live observer or allocate a new snapshot.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ListWitnessRemoveReturnsTheSameWitnessWhenTheObserverIsNotPresent()
    {
        RecordingWitness<int> present = new();
        RecordingWitness<int> absent = new();
        ListWitness<int> witness = new(CopyOnWriteList<IObserver<int>>.Empty.Add(present));

        var result = witness.Remove(absent);

        await Assert.That(result).IsSameReferenceAs(witness);
    }

    /// <summary>Asserts each witness rejects the callback or observer it cannot work without.</summary>
    private static void AssertWitnessConstructorsRejectMissingCallbacks()
    {
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            CallbackWitness<int> invalid = new(null!, null, null);
            GC.KeepAlive(invalid);
        });
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            ForwardingWitness<int> invalid = new(null!);
            GC.KeepAlive(invalid);
        });
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            StatefulWitness<int, string> invalid = new(State, null!, null, null);
            GC.KeepAlive(invalid);
        });
    }

    /// <summary>Asserts a callback witness forwards each notification, and rethrows when no error callback was given.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertCallbackWitnessForwardsEachNotification()
    {
        List<int> callbackValues = [];
        List<Exception> callbackErrors = [];
        List<Result> callbackCompletions = [];
        CallbackWitness<int> callback = new(callbackValues.Add, callbackErrors.Add, callbackCompletions.Add);
        InvalidOperationException callbackError = new("callback");
        callback.OnNext(One);
        callback.OnError(callbackError);
        callback.OnCompleted();
        await Assert.That(callbackValues.SequenceEqual([One])).IsTrue();
        await Assert.That(callbackErrors[0]).IsSameReferenceAs(callbackError);
        await Assert.That(callbackCompletions[0].IsSuccess).IsTrue();
        InvalidOperationException callbackFallback = new("callback fallback");
        _ = Assert.Throws<InvalidOperationException>(() =>
            new CallbackWitness<int>(static _ => { }, null, null).OnError(callbackFallback));
        new CallbackWitness<int>(static _ => { }, null, null).OnCompleted();
    }

    /// <summary>Asserts a forwarding witness passes every notification through to the observer it wraps.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertForwardingWitnessForwardsEachNotification()
    {
        Recorder<int> forwarded = new();
        ForwardingWitness<int> forwarding = new(forwarded);
        InvalidOperationException forwardingError = new("forwarding");
        forwarding.OnNext(Two);
        forwarding.OnError(forwardingError);
        forwarding.OnCompleted();
        await Assert.That(forwarded.Values.SequenceEqual([Two])).IsTrue();
        await Assert.That(forwarded.Errors[0]).IsSameReferenceAs(forwardingError);
        await Assert.That(forwarded.Completed).IsEqualTo(1);
    }

    /// <summary>Asserts a stateful witness hands its state to every callback, and rethrows without an error callback.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertStatefulWitnessForwardsEachNotificationWithItsState()
    {
        List<string> statefulValues = [];
        List<string> statefulErrors = [];
        List<string> statefulCompletions = [];
        StatefulWitness<int, string> stateful = new(
            State,
            (value, state) => statefulValues.Add($"{state}:{value}"),
            (error, state) => statefulErrors.Add($"{state}:{error.Message}"),
            (result, state) => statefulCompletions.Add($"{state}:{result.IsSuccess}"));
        InvalidOperationException statefulError = new("stateful");
        stateful.OnNext(One);
        stateful.OnError(statefulError);
        stateful.OnCompleted();
        await Assert.That(statefulValues.SequenceEqual([$"{State}:{One}"])).IsTrue();
        await Assert.That(statefulErrors.SequenceEqual([$"{State}:{statefulError.Message}"])).IsTrue();
        await Assert.That(statefulCompletions.SequenceEqual([$"{State}:True"])).IsTrue();
        InvalidOperationException statefulFallback = new("stateful fallback");
        _ = Assert.Throws<InvalidOperationException>(() =>
            new StatefulWitness<int, string>(State, static (_, _) => { }, null, null).OnError(statefulFallback));
        new StatefulWitness<int, string>(State, static (_, _) => { }, null, null).OnCompleted();
    }

    /// <summary>Asserts a safe witness drops every notification that arrives after its first terminal one.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertSafeWitnessIgnoresNotificationsAfterTheTerminal()
    {
        List<int> safeValues = [];
        List<Exception> safeErrors = [];
        var safeCompleted = 0;
        var safe = Witness.Safe(Witness.Create<int>(safeValues.Add, safeErrors.Add, () => safeCompleted++));
        safe.OnNext(One);
        safe.OnCompleted();
        safe.OnNext(Two);
        safe.OnError(new InvalidOperationException("ignored"));
        safe.OnCompleted();
        await Assert.That(safeValues.SequenceEqual([One])).IsTrue();
        await Assert.That(safeErrors.Count).IsEqualTo(0);
        await Assert.That(safeCompleted).IsEqualTo(1);
    }

    /// <summary>Waits for a task with a bounded timeout.</summary>
    /// <param name="task">The task to wait for.</param>
    /// <returns>A task that completes when the supplied task completes.</returns>
    /// <exception cref="TimeoutException">The supplied task did not complete within the bounded timeout.</exception>
    private static async Task WaitForAsync(Task task)
    {
        var timeout = Task.Delay(TimeSpan.FromSeconds(TimeoutSeconds));
        var completed = await Task.WhenAny(task, timeout).ConfigureAwait(false);
        if (completed == timeout)
        {
            throw new TimeoutException("Timed out waiting for scheduled observer dispatch.");
        }

        await task.ConfigureAwait(false);
    }

    /// <summary>Waits for a task with a bounded timeout and returns its result.</summary>
    /// <typeparam name="T">The task result type.</typeparam>
    /// <param name="task">The task to wait for.</param>
    /// <returns>The task result.</returns>
    private static async Task<T> WaitForAsync<T>(Task<T> task)
    {
        await WaitForAsync((Task)task).ConfigureAwait(false);
        return await task.ConfigureAwait(false);
    }

    /// <summary>Records observer notifications.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class Recorder<T> : IObserver<T>
    {
        /// <summary>Gets observed values.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets observed errors.</summary>
        public List<Exception> Errors { get; } = [];

        /// <summary>Gets the number of completion notifications.</summary>
        public int Completed { get; private set; }

        /// <inheritdoc/>
        public void OnCompleted() => Completed++;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => Errors.Add(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => Values.Add(value);
    }

    /// <summary>Observable with a disposable subscription tracker and captured observer.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class RecordingDisposableObservable<T> : IObservable<T>
    {
        /// <summary>Gets the captured observer.</summary>
        public IObserver<T>? Observer { get; private set; }

        /// <summary>Gets the number of times the source subscription was disposed.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            Observer = observer;
            return new ActionDisposable(() => DisposeCount++);
        }
    }
}
