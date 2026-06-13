// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Signals;
using ReactiveUI.Primitives.Signals.Core;

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

    /// <summary>Covers scheduled return, throw, and empty signal implementations.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduledScalarFactoriesUseNonImmediateSignalImplementations()
    {
        List<int> returned = [];
        var returnCompleted = 0;
        Signal.Emit(One, Sequencer.CurrentThread)
            .Subscribe(returned.Add, ex => throw ex, () => returnCompleted++);
        var emptyCompleted = 0;
        Signal.None<int>(Sequencer.CurrentThread)
            .Subscribe(_ => { }, ex => throw ex, () => emptyCompleted++);
        InvalidOperationException error = new("scheduled");
        List<Exception> thrown = [];
        Signal.Fail<int>(error, Sequencer.CurrentThread).Subscribe(_ => { }, thrown.Add, () => { });
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
        TestClock clock = new(DateTimeOffset.UnixEpoch);
        Signal.Sequence(Three, Three, Sequencer.CurrentThread).Subscribe(rangeValues.Add);
        Signal.Loop("r").Take(Three).Subscribe(repeatValues.Add);
        Signal.Loop(Five, Two).Subscribe(repeatCountValues.Add);
        Signal.Start(() => Seven, Sequencer.CurrentThread).Subscribe(startValues.Add);
        Signal.Start(() => startActions++, Sequencer.CurrentThread).Subscribe(_ => { });
        Signal.FromTask(Task.FromResult(Four)).Subscribe(taskValues.Add, ex => taskErrors.Add(ex.GetType().Name));
        Signal.FromTask(Task.FromException<int>(new InvalidOperationException("task-fault")))
            .Subscribe(taskValues.Add, ex => taskErrors.Add(ex.GetType().Name));
        Signal.FromTask(Task.FromCanceled<int>(new(true)))
            .Subscribe(taskValues.Add, ex => taskErrors.Add(ex.GetType().Name));
        await TestPolling.SpinUntil(
            () => taskValues.Count == One && taskErrors.Count == Two,
            TimeSpan.FromSeconds(TimeoutSeconds));
        using var disposedTaskSubscription = Signal.FromTask(Task.FromResult(NinetyNine))
            .Subscribe(_ => taskValues.Add(NinetyNine));
        disposedTaskSubscription.Dispose();
        Signal.After(TimeSpan.FromTicks(Two), clock).Subscribe(afterValues.Add);
        Signal.Every(TimeSpan.FromTicks(Two), clock).Take(Three).Subscribe(everyValues.Add);
        Signal.After(DateTimeOffset.UnixEpoch.AddTicks(Three), clock).Subscribe(timerDateValues.Add);
        Signal.After(TimeSpan.FromTicks(Three), TimeSpan.FromTicks(Two), clock).Subscribe(timerPeriodicValues.Add);
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
        Assert.Throws<ArgumentNullException>(() => Signal.Sequence(One, Two, null!));
        Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Sequence(One, -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Loop(One, -1));
        Assert.Throws<ArgumentNullException>(() => Signal.FromEnumerable<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromEnumerable<int>(null!, CancellationToken.None));
        Assert.Throws<ArgumentNullException>(() => Signal.FromTask((Task<int>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromAsync((Func<Task<int>>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromAsync((Func<CancellationToken, Task<int>>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Start<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Start(() => One, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Start((Action)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Start(() => { }, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromAsyncEnumerable<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromAsyncEnumerable<int>(null!, CancellationToken.None));
        Assert.Throws<ArgumentNullException>(() => Signal.After(TimeSpan.Zero, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Every(TimeSpan.FromTicks(One), null!));
        Assert.Throws<ArgumentNullException>(() => Signal.After(DateTimeOffset.UnixEpoch, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.After(TimeSpan.Zero, TimeSpan.FromTicks(One), null!));
    }

    /// <summary>Covers small value/factory/inline branches with public surface behavior.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValueFactoryAndInlineBranchesCoverPublicEdgeBehavior()
    {
        List<int> emptyScheduled = [];
        var emptyCompleted = 0;
        TestClock emptyClock = new(DateTimeOffset.UnixEpoch);
        Signal.None<int>(emptyClock).Subscribe(emptyScheduled.Add, ex => throw ex, () => emptyCompleted++);
        await Assert.That(emptyCompleted).IsEqualTo(0);
        emptyClock.Start();
        await Assert.That(emptyCompleted).IsEqualTo(1);
        Assert.Throws<ArgumentNullException>(() => Signal.None<int>().Subscribe((IObserver<int>)null!));
        List<int> repeatValues = [];
        var repeatCompleted = 0;
        var repeat = Signal.Loop(Seven, Three);
        await Assert.That(((IRequireCurrentThread<int>)repeat).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        repeat.Subscribe(new RecordingWitness<int>()).Dispose();
        Assert.Throws<ArgumentNullException>(() => repeat.Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => ((IInlineSignal<int>)repeat).Subscribe(null!, _ => { }, () => { }));
        ((IInlineSignal<int>)repeat).Subscribe(repeatValues.Add, ex => throw ex, () => repeatCompleted++);
        await Assert.That(repeatValues.SequenceEqual(ExpectedSevenSevenSeven)).IsTrue();
        await Assert.That(repeatCompleted).IsEqualTo(1);
        List<int> zippedValues = [];
        var zippedCompleted = 0;
        var zipped = Signal.Sequence(One, Three).Pair(Signal.Sequence(Four, Three), (left, right) => left + right);
        await Assert.That(((IRequireCurrentThread<int>)zipped).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        Assert.Throws<ArgumentNullException>(() => zipped.Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => ((IInlineSignal<int>)zipped).Subscribe(null!, _ => { }, () => { }));
        ((IInlineSignal<int>)zipped).Subscribe(zippedValues.Add, ex => throw ex, () => zippedCompleted++);
        await Assert.That(zippedValues.SequenceEqual(ExpectedFiveSevenNine)).IsTrue();
        await Assert.That(zippedCompleted).IsEqualTo(1);
        List<string> returned = [];
        var returnCompleted = 0;
        TestClock returnClock = new(DateTimeOffset.UnixEpoch);
        Signal.Emit("scheduled", returnClock).Subscribe(returned.Add, ex => throw ex, () => returnCompleted++);
        await Assert.That(returnCompleted).IsEqualTo(0);
        returnClock.AdvanceBy(TimeSpan.FromTicks(One));
        await Assert.That(returned.SequenceEqual(ExpectedScheduledReturn)).IsTrue();
        await Assert.That(returnCompleted).IsEqualTo(1);
        Assert.Throws<ArgumentNullException>(() => Signal.Emit("immediate").Subscribe((IObserver<string>)null!));
        List<string> mappedErrors = [];
        Signal.FromEnumerable([One, Two])
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
}
