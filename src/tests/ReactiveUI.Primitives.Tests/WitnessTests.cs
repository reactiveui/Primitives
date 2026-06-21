// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

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
            value => calls.Add("N" + value),
            ex => calls.Add("E" + ex.Message),
            () => calls.Add("C"));
        witness.OnNext(ObservedValue);
        witness.OnError(error);
        witness.OnCompleted();
        string[] expected = ["N" + ObservedValue, "Eboom", "C"];
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
                value => calls.Add("N" + value),
                ex => calls.Add("E" + ex.Message),
                () => calls.Add("C")),
            new ActionDisposable(() => disposed++));
        witness.OnNext(FirstValue);
        witness.OnCompleted();
        witness.OnNext(LateValue);
        witness.OnError(new InvalidOperationException("late"));
        witness.OnCompleted();
        string[] expected = ["N" + FirstValue, "C"];
        await Assert.That(calls.SequenceEqual(expected)).IsTrue();
        await Assert.That(disposed).IsEqualTo(1);
    }

    /// <summary>Covers internal witness implementations and safe observer terminal behavior.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WitnessesCoverDisposedThrowEmptyAndSafeBranches()
    {
        _ = Assert.Throws<ObjectDisposedException>(() => DisposedWitness<int>.Instance.OnNext(One));
        _ = Assert.Throws<ObjectDisposedException>(DisposedWitness<int>.Instance.OnCompleted);
        _ = Assert.Throws<ObjectDisposedException>(() =>
            DisposedWitness<int>.Instance.OnError(new InvalidOperationException("disposed")));
        ThrowWitness<int>.Instance.OnNext(One);
        ThrowWitness<int>.Instance.OnCompleted();
        _ = Assert.Throws<InvalidOperationException>(() =>
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
        _ = Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(_ => { }, (Action<Exception>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(_ => { }, (Action)null!));
        _ = Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(_ => { }, _ => { }, null!));
        _ = Assert.Throws<ArgumentNullException>(() => Witness.Safe<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(() => Witness.Safe(Witness.Create<int>(_ => { }), null!));
        List<string> events = [];
        var cancelDisposed = 0;
        var safe = Witness.Safe(
            Witness.Create<int>(
                value => events.Add("next:" + value),
                ex => events.Add("error:" + ex.Message),
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
            Witness.Create<int>(_ => throw new InvalidOperationException("next-failed")),
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
                   .Subscribe(_ => { }, observed.SetResult, () => { }))
        {
            await Assert.That(await WaitForAsync(observed.Task)).IsSameReferenceAs(error);
        }
    }

    /// <summary>Covers callback, forwarding, and stateful witness contracts.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WitnessImplementationsForwardNotificationsAndFallbackErrors()
    {
        _ = Assert.Throws<ArgumentNullException>(() =>
        {
            CallbackWitness<int> invalid = new(null!, null, null);
            GC.KeepAlive(invalid);
        });
        _ = Assert.Throws<ArgumentNullException>(() =>
        {
            ForwardingWitness<int> invalid = new(null!);
            GC.KeepAlive(invalid);
        });
        _ = Assert.Throws<ArgumentNullException>(() =>
        {
            StatefulWitness<int, string> invalid = new(State, null!, null, null);
            GC.KeepAlive(invalid);
        });
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
            new CallbackWitness<int>(_ => { }, null, null).OnError(callbackFallback));
        new CallbackWitness<int>(_ => { }, null, null).OnCompleted();
        Recorder<int> forwarded = new();
        ForwardingWitness<int> forwarding = new(forwarded);
        InvalidOperationException forwardingError = new("forwarding");
        forwarding.OnNext(Two);
        forwarding.OnError(forwardingError);
        forwarding.OnCompleted();
        await Assert.That(forwarded.Values.SequenceEqual([Two])).IsTrue();
        await Assert.That(forwarded.Errors[0]).IsSameReferenceAs(forwardingError);
        await Assert.That(forwarded.Completed).IsEqualTo(1);
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
            new StatefulWitness<int, string>(State, (_, _) => { }, null, null).OnError(statefulFallback));
        new StatefulWitness<int, string>(State, (_, _) => { }, null, null).OnCompleted();
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

    /// <summary>Covers witness factory and safe-wrapper null-callback validation.</summary>
    [Test]
    public void WitnessFactoriesValidateNullCallbacks()
    {
        _ = Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(null!, _ => { }, () => { }));
        _ = Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(_ => { }, null!, () => { }));
        _ = Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(_ => { }, _ => { }, null!));
        _ = Assert.Throws<ArgumentNullException>(() => Witness.Safe<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(() => Witness.Safe(Witness.Create<int>(_ => { }), null!));
        _ = Assert.Throws<ArgumentNullException>(() =>
            Witness.Safe(new Recorder<int>(), new RecordingDisposable()).OnError(null!));
        _ = Assert.Throws<ArgumentNullException>(() => Witness.Safe(new Recorder<int>()).OnError(null!));
        _ = Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(_ => { }, _ => { }, () => { }).OnError(null!));
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
        _ = Assert.Throws<ArgumentNullException>(() =>
        {
            TaskAnyWitness<int> invalid = new(null!, CancellationToken.None);
            GC.KeepAlive(invalid);
        });
        _ = Assert.Throws<ArgumentNullException>(() =>
        {
            TaskCountWitness<int> invalid = new(null!, CancellationToken.None);
            GC.KeepAlive(invalid);
        });
        _ = Assert.Throws<ArgumentNullException>(() =>
            new TaskAnyWitness<int>(CancellationToken.None).OnError(null!));
        _ = Assert.Throws<ArgumentNullException>(() =>
            new TaskCountWitness<int>(CancellationToken.None).OnError(null!));
        _ = Assert.Throws<ArgumentNullException>(() =>
            new TaskAnyWitness<int>(CancellationToken.None).SetSubscription(null!));
        _ = Assert.Throws<ArgumentNullException>(() =>
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

        TaskAnyWitness<int> unmatched = new(value => value > Four, CancellationToken.None);
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

        TaskCountWitness<int> even = new(value => value % Two == 0, CancellationToken.None);
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

    /// <summary>Waits for a task with a bounded timeout.</summary>
    /// <param name="task">The task to wait for.</param>
    /// <returns>A task that completes when the supplied task completes.</returns>
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
        public void OnError(Exception error) => Errors.Add(error);

        /// <inheritdoc/>
        public void OnNext(T value) => Values.Add(value);
    }
}
