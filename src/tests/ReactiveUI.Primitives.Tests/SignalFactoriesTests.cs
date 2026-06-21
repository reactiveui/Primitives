// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
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
            .Subscribe(returned.Add, ex => throw ex, () => returnCompleted++);
        var emptyCompleted = 0;
        _ = Signal.None<int>(Sequencer.CurrentThread)
            .Subscribe(_ => { }, ex => throw ex, () => emptyCompleted++);
        InvalidOperationException error = new("scheduled");
        List<Exception> thrown = [];
        _ = Signal.Fail<int>(error, Sequencer.CurrentThread).Subscribe(_ => { }, thrown.Add, () => { });
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
        _ = Signal.Start(() => Seven, Sequencer.CurrentThread).Subscribe(startValues.Add);
        _ = Signal.Start(() => startActions++, Sequencer.CurrentThread).Subscribe(_ => { });
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
        _ = Assert.Throws<ArgumentNullException>(() => Signal.Sequence(One, Two, null!));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Sequence(One, -1));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Loop(One, -1));
        _ = Assert.Throws<ArgumentNullException>(() => Signal.FromEnumerable<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(() => Signal.FromEnumerable<int>(null!, CancellationToken.None));
        _ = Assert.Throws<ArgumentNullException>(() => Signal.FromTask((Task<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => Signal.FromAsync((Func<Task<int>>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => Signal.FromAsync((Func<CancellationToken, Task<int>>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => Signal.Start<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(() => Signal.Start(() => One, null!));
        _ = Assert.Throws<ArgumentNullException>(() => Signal.Start((Action)null!));
        _ = Assert.Throws<ArgumentNullException>(() => Signal.Start(() => { }, null!));
        _ = Assert.Throws<ArgumentNullException>(() => Signal.FromAsyncEnumerable<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(() => Signal.FromAsyncEnumerable<int>(null!, CancellationToken.None));
        _ = Assert.Throws<ArgumentNullException>(() => Signal.After(TimeSpan.Zero, null!));
        _ = Assert.Throws<ArgumentNullException>(() => Signal.Every(TimeSpan.FromTicks(One), null!));
        _ = Assert.Throws<ArgumentNullException>(() => Signal.After(DateTimeOffset.UnixEpoch, null!));
        _ = Assert.Throws<ArgumentNullException>(() => Signal.After(TimeSpan.Zero, TimeSpan.FromTicks(One), null!));
    }

    /// <summary>Covers small value/factory/inline branches with public surface behavior.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValueFactoryAndInlineBranchesCoverPublicEdgeBehavior()
    {
        List<int> emptyScheduled = [];
        var emptyCompleted = 0;
        VirtualClock emptyClock = new(DateTimeOffset.UnixEpoch);
        _ = Signal.None<int>(emptyClock).Subscribe(emptyScheduled.Add, ex => throw ex, () => emptyCompleted++);
        await Assert.That(emptyCompleted).IsEqualTo(0);
        emptyClock.Start();
        await Assert.That(emptyCompleted).IsEqualTo(1);
        _ = Assert.Throws<ArgumentNullException>(() => Signal.None<int>().Subscribe((IObserver<int>)null!));
        List<int> repeatValues = [];
        var repeatCompleted = 0;
        var repeat = Signal.Loop(Seven, Three);
        await Assert.That(((IRequireCurrentThread<int>)repeat).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        repeat.Subscribe(new RecordingWitness<int>()).Dispose();
        _ = Assert.Throws<ArgumentNullException>(() => repeat.Subscribe((IObserver<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => ((IInlineSignal<int>)repeat).Subscribe(null!, _ => { }, () => { }));
        _ = ((IInlineSignal<int>)repeat).Subscribe(repeatValues.Add, ex => throw ex, () => repeatCompleted++);
        await Assert.That(repeatValues.SequenceEqual(ExpectedSevenSevenSeven)).IsTrue();
        await Assert.That(repeatCompleted).IsEqualTo(1);
        List<int> zippedValues = [];
        var zippedCompleted = 0;
        var zipped = Signal.Sequence(One, Three).Pair(Signal.Sequence(Four, Three), (left, right) => left + right);
        await Assert.That(((IRequireCurrentThread<int>)zipped).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        _ = Assert.Throws<ArgumentNullException>(() => zipped.Subscribe((IObserver<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => ((IInlineSignal<int>)zipped).Subscribe(null!, _ => { }, () => { }));
        _ = ((IInlineSignal<int>)zipped).Subscribe(zippedValues.Add, ex => throw ex, () => zippedCompleted++);
        await Assert.That(zippedValues.SequenceEqual(ExpectedFiveSevenNine)).IsTrue();
        await Assert.That(zippedCompleted).IsEqualTo(1);
        List<string> returned = [];
        var returnCompleted = 0;
        VirtualClock returnClock = new(DateTimeOffset.UnixEpoch);
        _ = Signal.Emit("scheduled", returnClock).Subscribe(returned.Add, ex => throw ex, () => returnCompleted++);
        await Assert.That(returnCompleted).IsEqualTo(0);
        returnClock.AdvanceBy(TimeSpan.FromTicks(One));
        await Assert.That(returned.SequenceEqual(ExpectedScheduledReturn)).IsTrue();
        await Assert.That(returnCompleted).IsEqualTo(1);
        _ = Assert.Throws<ArgumentNullException>(() => Signal.Emit("immediate").Subscribe((IObserver<string>)null!));
        List<string> mappedErrors = [];
        _ = Signal.FromEnumerable([One, Two])
            .Map(value => value == One ? value : throw new InvalidOperationException("map-fault"))
            .Subscribe(_ => { }, ex => mappedErrors.Add(ex.Message));
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
        Signal.Lazy<int>(() => throw new InvalidOperationException("defer-factory")).Subscribe(deferErrors).Dispose();
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
        var subscription = Signal.FromAsync<int>(async token =>
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
        var subscription = Signal.FromAsync<int>(token =>
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
        using var subscription = Signal.FromAsync<int>(
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
        using var subscription = Signal.FromAsync<int>(
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
        using var subscription = Signal.FromAsync<int>(token =>
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
        Signal.FromAsync<int>(_ => Task.FromException<int>(expected)).Subscribe(observer).Dispose();
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>Verifies direct timeout and runner factory APIs without extension method syntax.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DirectExpireAndRunnerFactoriesMirrorExtensionBehavior()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        RecordingWitness<int> timedOut = new();
        var timeoutSubscription = Signal.Expire(Signal.Never<int>(), TimeSpan.FromTicks(One), clock).Subscribe(timedOut);
        clock.AdvanceBy(TimeSpan.FromTicks(One));
        timeoutSubscription.Dispose();
        await Assert.That(timedOut.Errors[0]).IsTypeOf<TimeoutException>();
        await Assert.That(await Signal.ToTask(Signal.Sequence(One, Three))).IsEqualTo(Three);
        await Assert.That(await Signal.RunAsync(Signal.Sequence(One, Three))).IsEqualTo(Three);
        List<int> values = [];
        _ = Signal.Timeout(Signal.FromEnumerable(ExpectedOneTwoThree), TimeSpan.FromTicks(One), clock).Subscribe(values.Add);
        await Assert.That(values.SequenceEqual(ExpectedOneTwoThree)).IsTrue();
    }
}
