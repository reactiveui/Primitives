// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="Signal"/> factory scheduling, alias, timer, and inline contracts.</summary>
public partial class SignalFactoriesTests
{
    /// <summary>The integer constant one.</summary>
    private const int One = 1;

    /// <summary>The integer constant two.</summary>
    private const int Two = 2;

    /// <summary>The integer constant three.</summary>
    private const int Three = 3;

    /// <summary>The integer constant four.</summary>
    private const int Four = 4;

    /// <summary>The integer constant five.</summary>
    private const int Five = 5;

    /// <summary>The integer constant six.</summary>
    private const int Six = 6;

    /// <summary>The integer constant seven.</summary>
    private const int Seven = 7;

    /// <summary>The integer constant nine.</summary>
    private const int Nine = 9;

    /// <summary>The integer constant ninety-nine.</summary>
    private const int NinetyNine = 99;

    /// <summary>The timeout in seconds used when waiting for asynchronous branches.</summary>
    private const int TimeoutSeconds = 2;

    /// <summary>The long constant zero.</summary>
    private const long ZeroLong = 0L;

    /// <summary>The long constant one.</summary>
    private const long OneLong = 1L;

    /// <summary>The long constant two.</summary>
    private const long TwoLong = 2L;

    /// <summary>Single-value return expectation.</summary>
    private static readonly int[] SingleFirstExpected = [One];

    /// <summary>The expected three-through-five sequence emitted by the range factory.</summary>
    private static readonly int[] ExpectedThreeToFive = [Three, Four, Five];

    /// <summary>The expected values produced by the looped string signal.</summary>
    private static readonly string[] ExpectedRepeatValues = ["r", "r", "r"];

    /// <summary>The expected repeated five values emitted by the bounded loop factory.</summary>
    private static readonly int[] ExpectedFiveFive = [Five, Five];

    /// <summary>The expected single seven produced by the start and scheduled branches.</summary>
    private static readonly int[] ExpectedSingleSeven = [Seven];

    /// <summary>The expected error type names produced by the task factory continuations.</summary>
    private static readonly string[] ExpectedTaskErrorNames =
        [nameof(InvalidOperationException), nameof(TaskCanceledException)];

    /// <summary>The expected single zero-tick emission produced by one-shot timer factories.</summary>
    private static readonly long[] ExpectedSingleZeroTick = [ZeroLong];

    /// <summary>The expected zero-through-two tick emissions produced by periodic timer factories.</summary>
    private static readonly long[] ExpectedZeroToTwoTicks = [ZeroLong, OneLong, TwoLong];

    /// <summary>The expected three repeated sevens produced by the bounded loop signal.</summary>
    private static readonly int[] ExpectedSevenSevenSeven = [Seven, Seven, Seven];

    /// <summary>The expected paired sums produced by the zip alias.</summary>
    private static readonly int[] ExpectedFiveSevenNine = [Five, Seven, Nine];

    /// <summary>The expected value emitted by the scheduled return signal.</summary>
    private static readonly string[] ExpectedScheduledReturn = ["scheduled"];

    /// <summary>The expected message from the map-selector fault branch.</summary>
    private static readonly string[] ExpectedMappedErrors = ["map-fault"];

    /// <summary>The expected values emitted by the direct task runner tests.</summary>
    private static readonly int[] ExpectedOneTwoThree = [One, Two, Three];

    /// <summary>Covers scheduled return, throw, and empty signal implementations.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduledScalarFactoriesUseNonImmediateSignalImplementations()
    {
        List<int> returned = [];
        var returnCompleted = 0;
        _ = Signal.Emit(One, Sequencer.CurrentThread)
            .Subscribe(returned.Add, static ex => throw ex, () => returnCompleted++);
        var emptyCompleted = 0;
        _ = Signal.None<int>(Sequencer.CurrentThread)
            .Subscribe(static _ => { }, static ex => throw ex, () => emptyCompleted++);
        InvalidOperationException error = new("scheduled");
        List<Exception> thrown = [];
        _ = Signal.Fail<int>(error, Sequencer.CurrentThread).Subscribe(static _ => { }, thrown.Add, static () => { });
        await Assert.That(returned.SequenceEqual(SingleFirstExpected)).IsTrue();
        await Assert.That(returnCompleted).IsEqualTo(1);
        await Assert.That(emptyCompleted).IsEqualTo(1);
        await Assert.That(thrown[0]).IsSameReferenceAs(error);
    }

    /// <summary>Covers factory scheduling, task continuations, and timer aliases with deterministic time.</summary>
    /// <returns>A task that completes when asynchronous continuations are observed.</returns>
    [Test]
    public async Task FactoryAliasesScheduledRangesTasksAndTimersCoverRemainderBranches()
    {
        List<int> rangeValues = [];
        List<string> repeatValues = [];
        List<int> repeatCountValues = [];
        List<int> startValues = [];
        var startActions = 0;
        List<int> taskValues = [];
        List<string> taskErrors = [];
        List<long> afterValues = [];
        List<long> everyValues = [];
        List<long> timerDateValues = [];
        List<long> timerPeriodicValues = [];
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        _ = Signal.Sequence(Three, Three, Sequencer.CurrentThread).Subscribe(rangeValues.Add);
        _ = Signal.Loop("r").Take(Three).Subscribe(repeatValues.Add);
        _ = Signal.Loop(Five, Two).Subscribe(repeatCountValues.Add);
        _ = Signal.Start(static () => Seven, Sequencer.CurrentThread).Subscribe(startValues.Add);
        _ = Signal.Start(() => startActions++, Sequencer.CurrentThread).Subscribe(static _ => { });
        _ = Signal.FromTask(Task.FromResult(Four)).Subscribe(taskValues.Add, ex => taskErrors.Add(ex.GetType().Name));
        _ = Signal.FromTask(Task.FromException<int>(new InvalidOperationException("task-fault")))
            .Subscribe(taskValues.Add, ex => taskErrors.Add(ex.GetType().Name));
        _ = Signal.FromTask(Task.FromCanceled<int>(new(true)))
            .Subscribe(taskValues.Add, ex => taskErrors.Add(ex.GetType().Name));
        await TestPolling.SpinUntil(
            () => taskValues.Count == One && taskErrors.Count == Two,
            TimeSpan.FromSeconds(TimeoutSeconds));
        using var disposedTaskSubscription = Signal.FromTask(Task.FromResult(NinetyNine))
            .Subscribe(_ => taskValues.Add(NinetyNine));
        disposedTaskSubscription.Dispose();
        _ = Signal.After(TimeSpan.FromTicks(Two), clock).Subscribe(afterValues.Add);
        _ = Signal.Every(TimeSpan.FromTicks(Two), clock).Take(Three).Subscribe(everyValues.Add);
        _ = Signal.After(DateTimeOffset.UnixEpoch.AddTicks(Three), clock).Subscribe(timerDateValues.Add);
        _ = Signal.After(TimeSpan.FromTicks(Three), TimeSpan.FromTicks(Two), clock).Subscribe(timerPeriodicValues.Add);
        clock.AdvanceBy(TimeSpan.FromTicks(Two));
        clock.AdvanceBy(TimeSpan.FromTicks(One));
        clock.AdvanceBy(TimeSpan.FromTicks(Four));
        await Assert.That(rangeValues.SequenceEqual(ExpectedThreeToFive)).IsTrue();
        await Assert.That(repeatValues.SequenceEqual(ExpectedRepeatValues)).IsTrue();
        await Assert.That(repeatCountValues.SequenceEqual(ExpectedFiveFive)).IsTrue();
        await Assert.That(startValues.SequenceEqual(ExpectedSingleSeven)).IsTrue();
        await Assert.That(startActions).IsEqualTo(1);
        await Assert.That(taskValues).Contains(Four);
        await Assert.That(taskErrors.SequenceEqual(ExpectedTaskErrorNames)).IsTrue();
        await Assert.That(afterValues.SequenceEqual(ExpectedSingleZeroTick)).IsTrue();
        await Assert.That(everyValues.SequenceEqual(ExpectedZeroToTwoTicks)).IsTrue();
        await Assert.That(timerDateValues.SequenceEqual(ExpectedSingleZeroTick)).IsTrue();
        await Assert.That(timerPeriodicValues.SequenceEqual(ExpectedZeroToTwoTicks)).IsTrue();
        AssertSchedulingFactoriesRejectInvalidArguments();
    }

    /// <summary>Covers small value/factory/inline branches with public surface behavior.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValueFactoryAndInlineBranchesCoverPublicEdgeBehavior()
    {
        List<int> emptyScheduled = [];
        var emptyCompleted = 0;
        VirtualClock emptyClock = new(DateTimeOffset.UnixEpoch);
        _ = Signal.None<int>(emptyClock).Subscribe(emptyScheduled.Add, static ex => throw ex, () => emptyCompleted++);
        await Assert.That(emptyCompleted).IsEqualTo(0);
        emptyClock.Start();
        await Assert.That(emptyCompleted).IsEqualTo(1);
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.None<int>().Subscribe((IObserver<int>)null!));
        List<int> repeatValues = [];
        var repeatCompleted = 0;
        var repeat = Signal.Loop(Seven, Three);
        await Assert.That(((IRequireCurrentThread<int>)repeat).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        repeat.Subscribe(new RecordingWitness<int>()).Dispose();
        _ = Assert.Throws<ArgumentNullException>(() => repeat.Subscribe((IObserver<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(() =>
            ((IInlineSignal<int>)repeat).Subscribe(null!, static _ => { }, static () => { }));
        _ = ((IInlineSignal<int>)repeat).Subscribe(repeatValues.Add, static ex => throw ex, () => repeatCompleted++);
        await Assert.That(repeatValues.SequenceEqual(ExpectedSevenSevenSeven)).IsTrue();
        await Assert.That(repeatCompleted).IsEqualTo(1);
        List<int> zippedValues = [];
        var zippedCompleted = 0;
        var zipped = Signal.Sequence(One, Three).Pair(Signal.Sequence(Four, Three), static (left, right) => left + right);
        await Assert.That(((IRequireCurrentThread<int>)zipped).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        _ = Assert.Throws<ArgumentNullException>(() => zipped.Subscribe((IObserver<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(() =>
            ((IInlineSignal<int>)zipped).Subscribe(null!, static _ => { }, static () => { }));
        _ = ((IInlineSignal<int>)zipped).Subscribe(zippedValues.Add, static ex => throw ex, () => zippedCompleted++);
        await Assert.That(zippedValues.SequenceEqual(ExpectedFiveSevenNine)).IsTrue();
        await Assert.That(zippedCompleted).IsEqualTo(1);
        List<string> returned = [];
        var returnCompleted = 0;
        VirtualClock returnClock = new(DateTimeOffset.UnixEpoch);
        _ = Signal.Emit("scheduled", returnClock).Subscribe(returned.Add, static ex => throw ex, () => returnCompleted++);
        await Assert.That(returnCompleted).IsEqualTo(0);
        returnClock.AdvanceBy(TimeSpan.FromTicks(One));
        await Assert.That(returned.SequenceEqual(ExpectedScheduledReturn)).IsTrue();
        await Assert.That(returnCompleted).IsEqualTo(1);
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Emit("immediate").Subscribe((IObserver<string>)null!));
        List<string> mappedErrors = [];
        _ = Signal.FromEnumerable([One, Two])
            .Map(static value => value == One ? value : throw new InvalidOperationException("map-fault"))
            .Subscribe(static _ => { }, ex => mappedErrors.Add(ex.Message));
        await Assert.That(mappedErrors.SequenceEqual(ExpectedMappedErrors)).IsTrue();
    }

    /// <summary>Covers create-with-state, defer, and immediate-throw factory observer error paths.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FactoryErrorPathsForwardObserverErrors()
    {
        RecordingWitness<int> createErrors = new();
        Signal.CreateWithState<int, int>(0, static (_, observer) =>
        {
            observer.OnError(new InvalidOperationException("create-error"));
            return null!;
        }).Subscribe(createErrors).Dispose();
        await Assert.That(createErrors.Errors[0].Message).IsEqualTo("create-error");
        RecordingWitness<int> deferErrors = new();
        Signal.Lazy<int>(static () => throw new InvalidOperationException("defer-factory")).Subscribe(deferErrors).Dispose();
        await Assert.That(deferErrors.Errors[0].Message).IsEqualTo("defer-factory");
        RecordingWitness<int> immediateThrow = new();
        Signal.Fail<int>(new InvalidOperationException("immediate-throw"), Sequencer.Immediate)
            .Subscribe(immediateThrow).Dispose();
        await Assert.That(immediateThrow.Errors[0].Message).IsEqualTo("immediate-throw");
    }

    /// <summary>Verifies that cancellable async factories receive a token owned by the subscription.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromAsyncCancellableFactoryDisposalCancelsSubscriptionToken()
    {
        TaskCompletionSource<CancellationToken> observedToken = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource canceled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingWitness<int> observer = new();
        var subscription = Signal.FromAsync(async token =>
        {
            observedToken.SetResult(token);
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(TimeoutSeconds), token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                canceled.SetResult();
                throw;
            }

            return One;
        }).Subscribe(observer);
        var token = await observedToken.Task.WaitAsync(TimeSpan.FromSeconds(TimeoutSeconds));
        subscription.Dispose();
        await canceled.Task.WaitAsync(TimeSpan.FromSeconds(TimeoutSeconds));
        await Assert.That(token.IsCancellationRequested).IsTrue();
        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>Verifies that disposal suppresses a task result when the factory ignores cancellation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromAsyncCancellableFactoryDisposalSuppressesIgnoredCancellationResult()
    {
        TaskCompletionSource subscribed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<int> complete = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingWitness<int> observer = new();
        var subscription = Signal.FromAsync(token =>
        {
            subscribed.SetResult();
            return complete.Task;
        }).Subscribe(observer);
        await subscribed.Task.WaitAsync(TimeSpan.FromSeconds(TimeoutSeconds));
        subscription.Dispose();
        complete.SetResult(NinetyNine);
        await Task.Yield();
        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>Verifies that external token cancellation remains a source error while subscribed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromAsyncCancellableFactoryExternalCancellationForwardsObserverError()
    {
        using CancellationTokenSource external = new();
        TaskCompletionSource<CancellationToken> observedToken = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingWitness<int> observer = new();
        using var subscription = Signal.FromAsync(
            async token =>
            {
                observedToken.SetResult(token);
                await Task.Delay(TimeSpan.FromSeconds(TimeoutSeconds), token).ConfigureAwait(false);
                return One;
            },
            external.Token).Subscribe(observer);
        await observedToken.Task.WaitAsync(TimeSpan.FromSeconds(TimeoutSeconds));
        await external.CancelAsync();
        await TestPolling.SpinUntil(() => observer.Errors.Count == One, TimeSpan.FromSeconds(TimeoutSeconds));
        await Assert.That(observer.Errors[0]).IsTypeOf<TaskCanceledException>();
        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>Verifies that external token cancellation forwards an error even when the task ignores the linked token.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromAsyncCancellableFactoryExternalCancellationForwardsObserverErrorWhenTaskIgnoresToken()
    {
        using CancellationTokenSource external = new();
        TaskCompletionSource subscribed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<int> complete = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingWitness<int> observer = new();
        using var subscription = Signal.FromAsync(
            token =>
            {
                subscribed.SetResult();
                return complete.Task;
            },
            external.Token).Subscribe(observer);
        await subscribed.Task.WaitAsync(TimeSpan.FromSeconds(TimeoutSeconds));
        await external.CancelAsync();
        await TestPolling.SpinUntil(() => observer.Errors.Count == One, TimeSpan.FromSeconds(TimeoutSeconds));
        complete.SetResult(NinetyNine);
        await Task.Yield();
        await Assert.That(observer.Errors[0]).IsTypeOf<TaskCanceledException>();
        await Assert.That(observer.Errors.Count).IsEqualTo(One);
        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>Verifies that cancellable async factories emit the successful task result and complete.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromAsyncCancellableFactorySuccessEmitsValueAndCompletes()
    {
        var calls = 0;
        RecordingWitness<int> observer = new();
        using var subscription = Signal.FromAsync(token =>
        {
            calls++;
            return Task.FromResult(token.IsCancellationRequested ? NinetyNine : Seven);
        }).Subscribe(observer);
        await Assert.That(calls).IsEqualTo(One);
        await Assert.That(observer.Values.SequenceEqual(ExpectedSingleSeven)).IsTrue();
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(One);
    }

    /// <summary>Verifies that cancellable async factory task faults are forwarded without wrapping.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromAsyncCancellableFactoryFaultForwardsOriginalException()
    {
        InvalidOperationException expected = new("from-async-fault");
        RecordingWitness<int> observer = new();
        Signal.FromAsync(_ => Task.FromException<int>(expected)).Subscribe(observer).Dispose();
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>Verifies that synchronous cancellable async factory failures are forwarded as observer errors.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromAsyncCancellableFactorySynchronousFailuresForwardObserverErrors()
    {
        InvalidOperationException expected = new("from-async-sync-fault");
        RecordingWitness<int> thrown = new();
        Signal.FromAsync<int>(_ => throw expected).Subscribe(thrown).Dispose();
        await Assert.That(thrown.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(thrown.Values.Count).IsEqualTo(0);
        await Assert.That(thrown.Completed).IsEqualTo(0);

        RecordingWitness<int> nullTask = new();
        Signal.FromAsync<int>(static _ => null!).Subscribe(nullTask).Dispose();
        await Assert.That(nullTask.Errors[0]).IsTypeOf<ArgumentNullException>();
        await Assert.That(nullTask.Values.Count).IsEqualTo(0);
        await Assert.That(nullTask.Completed).IsEqualTo(0);
    }

    /// <summary>Verifies that already-canceled task factories forward cancellation as observer errors.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromAsyncCancellableFactoryCanceledTasksForwardObserverErrors()
    {
        using CancellationTokenSource taskCancellation = new();
        await taskCancellation.CancelAsync();
        RecordingWitness<int> canceledTask = new();
        Signal.FromAsync(_ => Task.FromCanceled<int>(taskCancellation.Token))
            .Subscribe(canceledTask).Dispose();
        await Assert.That(canceledTask.Errors[0]).IsTypeOf<TaskCanceledException>();
        await Assert.That(canceledTask.Values.Count).IsEqualTo(0);
        await Assert.That(canceledTask.Completed).IsEqualTo(0);

        using CancellationTokenSource external = new();
        await external.CancelAsync();
        var invoked = 0;
        RecordingWitness<int> externallyCanceled = new();
        Signal.FromAsync(
            _ =>
            {
                invoked++;
                return Task.FromResult(One);
            },
            external.Token).Subscribe(externallyCanceled).Dispose();
        await Assert.That(invoked).IsEqualTo(0);
        await Assert.That(externallyCanceled.Errors[0]).IsTypeOf<TaskCanceledException>();
        await Assert.That(externallyCanceled.Values.Count).IsEqualTo(0);
        await Assert.That(externallyCanceled.Completed).IsEqualTo(0);
    }

    /// <summary>Verifies pending cancellable async factory tasks forward success, fault, and cancellation continuations.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromAsyncCancellableFactoryPendingTasksForwardTerminalContinuations()
    {
        TaskCompletionSource<int> successfulTask = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingWitness<int> successful = new();
        using (Signal.FromAsync(_ => successfulTask.Task).Subscribe(successful))
        {
            successfulTask.SetResult(Seven);
            await TestPolling.SpinUntil(
                () => successful.Completed == One,
                TimeSpan.FromSeconds(TimeoutSeconds));
        }

        await Assert.That(successful.Values.SequenceEqual(ExpectedSingleSeven)).IsTrue();
        await Assert.That(successful.Errors.Count).IsEqualTo(0);

        InvalidOperationException expected = new("from-async-pending-fault");
        TaskCompletionSource<int> faultedTask = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingWitness<int> faulted = new();
        using (Signal.FromAsync(_ => faultedTask.Task).Subscribe(faulted))
        {
            faultedTask.SetException(expected);
            await TestPolling.SpinUntil(
                () => faulted.Errors.Count == One,
                TimeSpan.FromSeconds(TimeoutSeconds));
        }

        await Assert.That(faulted.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(faulted.Values.Count).IsEqualTo(0);
        await Assert.That(faulted.Completed).IsEqualTo(0);

        using CancellationTokenSource cancellation = new();
        await cancellation.CancelAsync();
        TaskCompletionSource<int> canceledTask = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingWitness<int> canceled = new();
        using (Signal.FromAsync(_ => canceledTask.Task).Subscribe(canceled))
        {
            canceledTask.SetCanceled(cancellation.Token);
            await TestPolling.SpinUntil(
                () => canceled.Errors.Count == One,
                TimeSpan.FromSeconds(TimeoutSeconds));
        }

        await Assert.That(canceled.Errors[0]).IsTypeOf<TaskCanceledException>();
        await Assert.That(canceled.Values.Count).IsEqualTo(0);
        await Assert.That(canceled.Completed).IsEqualTo(0);
    }

    /// <summary>Verifies direct timeout and runner factory APIs without extension method syntax.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DirectExpireAndRunnerFactoriesMirrorExtensionBehavior()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        RecordingWitness<int> timedOut = new();
        var timeoutSubscription =
            Signal.Expire(Signal.Never<int>(), TimeSpan.FromTicks(One), clock).Subscribe(timedOut);
        clock.AdvanceBy(TimeSpan.FromTicks(One));
        timeoutSubscription.Dispose();
        await Assert.That(timedOut.Errors[0]).IsTypeOf<TimeoutException>();
        await Assert.That(await Signal.ToTask(Signal.Sequence(One, Three))).IsEqualTo(Three);
        await Assert.That(await Signal.RunAsync(Signal.Sequence(One, Three))).IsEqualTo(Three);
        List<int> values = [];
        _ = Signal.Timeout(Signal.FromEnumerable(ExpectedOneTwoThree), TimeSpan.FromTicks(One), clock)
            .Subscribe(values.Add);
        await Assert.That(values.SequenceEqual(ExpectedOneTwoThree)).IsTrue();
    }

    /// <summary>Verifies Rx-named factory aliases delegate to the matching Primitives factories.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RxFactoryAliasesRepeatGenerateUsingIfAndCase()
    {
        List<int> repeated = [];
        List<int> repeatedCount = [];
        List<int> repeatedZero = [];
        List<int> generated = [];
        List<int> conditional = [];
        List<int> selectedCase = [];
        List<int> defaultCase = [];
        List<int> resumedPair = [];
        var disposed = 0;
        var useCompleted = 0;
        var repeatedZeroCompleted = 0;

        _ = Signal.Repeat(Seven).Take(Three).Subscribe(repeated.Add);
        _ = Signal.Repeat(Five, Two).Subscribe(repeatedCount.Add);
        _ = Signal.Repeat(Five, 0).Subscribe(repeatedZero.Add, static ex => throw ex, () => repeatedZeroCompleted++);
        _ = Signal.Generate(One, static value => value <= Three, static value => value + One, static value => value * Two)
            .Subscribe(generated.Add);
        _ = Signal.OnErrorResumeNext(
                Signal.Fail<int>(new InvalidOperationException("resume")),
                Signal.Emit(Four))
            .Subscribe(resumedPair.Add);

        var chooseThen = true;
        var conditionalSource = Signal.If(() => chooseThen, Signal.Emit(One), Signal.Emit(Two));
        _ = conditionalSource.Subscribe(conditional.Add);
        chooseThen = false;
        _ = conditionalSource.Subscribe(conditional.Add);

        Dictionary<string, IObservable<int>> cases = new(StringComparer.Ordinal) { ["one"] = Signal.Emit(One) };
        _ = Signal.Case(static () => "one", cases, Signal.Emit(Two)).Subscribe(selectedCase.Add);
        _ = Signal.Case(static () => "missing", cases, Signal.Emit(Two)).Subscribe(defaultCase.Add);
        _ = Signal.Using(
                () => new TrackedDisposable(() => disposed++),
                static _ => Signal.Emit(Three))
            .Subscribe(static _ => { }, static ex => throw ex, () => useCompleted++);

        await Assert.That(repeated.SequenceEqual(ExpectedSevenSevenSeven)).IsTrue();
        await Assert.That(repeatedCount.SequenceEqual(ExpectedFiveFive)).IsTrue();
        await Assert.That(repeatedZero).IsEmpty();
        await Assert.That(repeatedZeroCompleted).IsEqualTo(One);
        await Assert.That(generated.SequenceEqual([Two, Four, Six])).IsTrue();
        await Assert.That(conditional.SequenceEqual([One, Two])).IsTrue();
        await Assert.That(selectedCase.SequenceEqual([One])).IsTrue();
        await Assert.That(defaultCase.SequenceEqual([Two])).IsTrue();
        await Assert.That(resumedPair.SequenceEqual([Four])).IsTrue();
        await Assert.That(disposed).IsEqualTo(One);
        await Assert.That(useCompleted).IsEqualTo(One);

        AssertRxFactoryAliasesRejectInvalidArguments(cases);
    }

    /// <summary>
    /// Verifies an empty range completes immediately without touching the sequencer. There is nothing to emit, so the
    /// factory hands back the shared empty signal rather than scheduling a walk over zero values.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SequenceOfNoValuesCompletesWithoutSchedulingAnything()
    {
        RecordingSequencer sequencer = new();
        List<int> values = [];
        var completed = 0;

        using var subscription = Signal
            .Sequence(One, 0, sequencer)
            .Subscribe(values.Add, () => completed++);

        await Assert.That(values).IsEmpty();
        await Assert.That(completed).IsEqualTo(One);
        await Assert.That(sequencer.ScheduleCount).IsEqualTo(0);
    }

    /// <summary>Asserts the Rx-named factory aliases reject a null callback, a null source, or a negative count.</summary>
    /// <param name="cases">The case map the alias assertions built.</param>
    private static void AssertRxFactoryAliasesRejectInvalidArguments(Dictionary<string, IObservable<int>> cases)
    {
        _ = Assert.Throws<ArgumentOutOfRangeException>(static () => Signal.Repeat(One, -1));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            Signal.Generate(One, null!, static value => value, static value => value));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            Signal.Generate(One, static _ => true, null!, static value => value));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            Signal.Generate<int, int>(One, static _ => true, static value => value, null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.If(null!, Signal.Emit(One)));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.If(static () => true, null!, Signal.Emit(One)));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.If(static () => true, Signal.Emit(One), null!));
        _ = Assert.Throws<ArgumentNullException>(() => Signal.Case(null!, cases));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Case<string, int>(static () => "one", null!));
        _ = Assert.Throws<ArgumentNullException>(() => Signal.Case(static () => "one", cases, null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Using<IDisposable, int>(null!, static _ => Signal.Emit(One)));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            Signal.Using(static () => EmptyDisposable.Instance, (Func<IDisposable, IObservable<int>>)null!));
    }

    /// <summary>Asserts the scheduling, task, and timer factories reject a null source, sequencer, or negative count.</summary>
    private static void AssertSchedulingFactoriesRejectInvalidArguments()
    {
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Sequence(One, Two, null!));
        _ = Assert.Throws<ArgumentOutOfRangeException>(static () => Signal.Sequence(One, -1));
        _ = Assert.Throws<ArgumentOutOfRangeException>(static () => Signal.Loop(One, -1));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.FromEnumerable<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.FromEnumerable<int>(null!, CancellationToken.None));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.FromTask((Task<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.FromAsync((Func<Task<int>>)null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.FromAsync((Func<CancellationToken, Task<int>>)null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Start<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Start(static () => One, null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Start((Action)null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Start(static () => { }, null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.FromAsyncEnumerable<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.FromAsyncEnumerable<int>(null!, CancellationToken.None));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.After(TimeSpan.Zero, null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Every(TimeSpan.FromTicks(One), null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.After(DateTimeOffset.UnixEpoch, null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.After(TimeSpan.Zero, TimeSpan.FromTicks(One), null!));
    }

    /// <summary>Disposable used by factory alias tests.</summary>
    /// <param name="onDispose">Action invoked when disposed.</param>
    private sealed class TrackedDisposable(Action onDispose) : IDisposable
    {
        /// <inheritdoc/>
        public void Dispose() => onDispose();
    }

    /// <summary>Sequencer that runs work inline and counts how often it was asked to schedule anything.</summary>
    private sealed class RecordingSequencer : ISequencer
    {
        /// <summary>Gets the number of work items the sequencer was handed.</summary>
        public int ScheduleCount { get; private set; }

        /// <inheritdoc/>
        public DateTimeOffset Now => Sequencer.Now;

        /// <inheritdoc/>
        public long Timestamp => Sequencer.Timestamp;

        /// <inheritdoc/>
        public void Schedule(IWorkItem item)
        {
            ScheduleCount++;
            Sequencer.Immediate.Schedule(item);
        }

        /// <inheritdoc/>
        public void Schedule(IWorkItem item, long dueTimestamp)
        {
            ScheduleCount++;
            Sequencer.Immediate.Schedule(item, dueTimestamp);
        }
    }
}
