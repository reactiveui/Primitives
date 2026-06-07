// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Completes branch and contract coverage for primitive signals and support types.
/// </summary>
public class PublicApiBehaviorTests
{
    /// <summary>
    /// Two as a named value for analyzer-friendly coverage assertions.
    /// </summary>
    private const int Two = 2;

    /// <summary>
    /// Three as a named value for analyzer-friendly coverage assertions.
    /// </summary>
    private const int Three = 3;

    /// <summary>
    /// Four as a named value for analyzer-friendly coverage assertions.
    /// </summary>
    private const int Four = 4;

    /// <summary>
    /// Five as a named value for analyzer-friendly coverage assertions.
    /// </summary>
    private const int Five = 5;

    /// <summary>
    /// Six as a named value for analyzer-friendly coverage assertions.
    /// </summary>
    private const int Six = 6;

    /// <summary>
    /// Seven as a named value for analyzer-friendly coverage assertions.
    /// </summary>
    private const int Seven = 7;

    /// <summary>
    /// Eight as a named value for analyzer-friendly coverage assertions.
    /// </summary>
    private const int Eight = 8;

    /// <summary>
    /// Nine as a named value for analyzer-friendly coverage assertions.
    /// </summary>
    private const int Nine = 9;

    /// <summary>
    /// Ten as a named value for analyzer-friendly coverage assertions.
    /// </summary>
    private const int Ten = 10;

    /// <summary>
    /// Eleven as a named value for analyzer-friendly coverage assertions.
    /// </summary>
    private const int Eleven = 11;

    /// <summary>
    /// Twenty as a named value for analyzer-friendly coverage assertions.
    /// </summary>
    private const int Twenty = 20;

    /// <summary>
    /// Twenty-one as a named value for analyzer-friendly coverage assertions.
    /// </summary>
    private const int TwentyOne = 21;

    /// <summary>
    /// Forty-two as a named value for analyzer-friendly coverage assertions.
    /// </summary>
    private const int FortyTwo = 42;

    /// <summary>
    /// Forty-three as a named value for analyzer-friendly coverage assertions.
    /// </summary>
    private const int FortyThree = 43;

    /// <summary>
    /// Calendar year used by value-type timestamp coverage.
    /// </summary>
    private const int CalendarYear = 2024;

    /// <summary>
    /// Ninety-nine as a named value for analyzer-friendly coverage assertions.
    /// </summary>
    private const int NinetyNine = 99;

    /// <summary>
    /// Polling delay used by asynchronous spin assertions.
    /// </summary>
    private const int PollDelayMilliseconds = 10;

    /// <summary>
    /// Shared completed result text.
    /// </summary>
    private const string CompletedText = "completed";

    /// <summary>
    /// Shared completed function result text.
    /// </summary>
    private const string FunctionCompletedText = "fn-completed";

    /// <summary>
    /// Expected operator values.
    /// </summary>
    private static readonly string[] ExpectedOperatorValues = ["bbb!", "cc!"];

    /// <summary>
    /// Expected side-effect values.
    /// </summary>
    private static readonly string[] ExpectedSideEffects = ["a!", "bbb!", "cc!"];

    /// <summary>
    /// Expected non-null values.
    /// </summary>
    private static readonly string[] ExpectedKeepNotNull = ["x", "y"];

    /// <summary>
    /// Expected false scalar sequence.
    /// </summary>
    private static readonly bool[] ExpectedFalse = [false];

    /// <summary>
    /// Expected long-count result.
    /// </summary>
    private static readonly long[] ExpectedLongCount = [2L];

    /// <summary>
    /// Expected select-many result.
    /// </summary>
    private static readonly string[] ExpectedSelectMany = ["1:1", "1:11", "2:2", "2:12"];

    /// <summary>
    /// Expected values projected from enumerable collections.
    /// </summary>
    private static readonly int[] ExpectedFlatMapValues = [1, Ten, Two, 20];

    /// <summary>
    /// Expected spark kind sequence.
    /// </summary>
    private static readonly SparkKind[] ExpectedSparkKinds = [SparkKind.OnError];

    /// <summary>
    /// Expected spark error messages.
    /// </summary>
    private static readonly string[] ExpectedSparkErrors = ["spark"];

    /// <summary>
    /// Expected unspark values.
    /// </summary>
    private static readonly int[] ExpectedUnsparkValues = [1];

    /// <summary>
    /// Expected unspark errors.
    /// </summary>
    private static readonly string[] ExpectedUnsparkErrors = ["unspark"];

    /// <summary>
    /// Expected rescue values.
    /// </summary>
    private static readonly int[] ExpectedRescueValues = [Seven];

    /// <summary>
    /// Expected resume values.
    /// </summary>
    private static readonly int[] ExpectedResumeValues = [Four, Five];

    /// <summary>
    /// Expected final errors.
    /// </summary>
    private static readonly string[] ExpectedFinalErrors = ["stop"];

    /// <summary>
    /// Expected concat values.
    /// </summary>
    private static readonly int[] ExpectedConcatValues = [1, Two, TwentyOne];

    /// <summary>
    /// Expected merge values.
    /// </summary>
    private static readonly int[] ExpectedMergeValues = [1, Two, Three];

    /// <summary>
    /// Expected race values.
    /// </summary>
    private static readonly int[] ExpectedRaceValues = [Seven];

    /// <summary>
    /// Expected switch values.
    /// </summary>
    private static readonly int[] ExpectedSwitchValues = [1, Three];

    /// <summary>
    /// Expected latest-combination values.
    /// </summary>
    private static readonly string[] ExpectedWithLatestValues = ["2a", "3b"];

    /// <summary>
    /// Expected zip values.
    /// </summary>
    private static readonly int[] ExpectedZipShortValues = [Eleven];

    /// <summary>
    /// Expected delayed scalar values.
    /// </summary>
    private static readonly int[] ExpectedDelayedValues = [Three, Four];

    /// <summary>
    /// Expected delay-start scalar values.
    /// </summary>
    private static readonly int[] ExpectedDelayStartValues = [Two];

    /// <summary>
    /// Expected timer values.
    /// </summary>
    private static readonly long[] ExpectedTimerValues = [0L, 1L, 2L];

    /// <summary>
    /// Expected timeout error names.
    /// </summary>
    private static readonly string[] ExpectedTimeoutErrors = [nameof(TimeoutException)];

    /// <summary>
    /// Expected use-factory errors.
    /// </summary>
    private static readonly string[] ExpectedUseErrors = ["The signal factory returned null.", "resource"];

    /// <summary>
    /// Expected task error names.
    /// </summary>
    private static readonly string[] ExpectedTaskErrors = [nameof(TaskCanceledException), nameof(InvalidOperationException)];

    /// <summary>
    /// Expected async error messages.
    /// </summary>
    private static readonly string[] ExpectedAsyncErrors = ["async"];

    /// <summary>
    /// Expected observable values.
    /// </summary>
    private static readonly int[] ExpectedObservableValues = [FortyTwo];

    /// <summary>
    /// Expected timestamp values.
    /// </summary>
    private static readonly int[] ExpectedTimestampValues = [Eight, Nine];

    /// <summary>
    /// Validates null guard coverage across public factories, operators, and observers.
    /// </summary>
    [Test]
    public void NullGuardsCoverPublicFactoryOperatorAndObserverContracts()
    {
        IObservable<int> source = Signal.Emit(1);
        IObservable<object?> objects = Signal.Emit<object?>("value");

        CoverUnaryOperatorNullGuards(source);
        CoverHigherOrderOperatorNullGuards(source);
        CoverParityOperatorNullGuards(source);
        CoverFactoryAndObserverNullGuards(objects);
    }

    /// <summary>
    /// Exercises successful operator paths and early-termination branches.
    /// </summary>
    [Test]
    public void OperatorSurfaceCoversSuccessErrorAndEarlyTerminationBranches()
    {
        var values = new List<string>();
        var sideEffects = new List<string>();
        var terminal = 0;

        Signal.FromEnumerable(new object?[] { "a", null, Two, "bbb", "cc", Three })
            .KeepType<string>()
            .MapWith("!", (suffix, value) => value + suffix)
            .KeepWith(Two, (min, value) => value.Length >= min)
            .TapWith(sideEffects, (sink, value) => sink.Add(value))
            .CastTo<string>()
            .Skip(1)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Unique(StringComparer.OrdinalIgnoreCase)
            .Subscribe(values.Add, ex => throw ex, () => terminal++);

        Assert.Equal(ExpectedOperatorValues, values);
        Assert.Equal(ExpectedSideEffects, sideEffects);
        Assert.Equal(1, terminal);

        var keepNotNull = new List<string>();
        Signal.FromEnumerable([null, "x", null, "y"]).KeepNotNull().Subscribe(keepNotNull.Add);
        Assert.Equal(ExpectedKeepNotNull, keepNotNull);

        var emptyTake = new List<int>();
        var emptyTakeCompleted = 0;
        Signal.Sequence(1, Three).Take(0).Subscribe(emptyTake.Add, ex => throw ex, () => emptyTakeCompleted++);
        Assert.Equal(0, emptyTake.Count);
        Assert.Equal(1, emptyTakeCompleted);

        var skipAll = new List<int>();
        Signal.Sequence(1, Three).Skip(Ten).Subscribe(skipAll.Add);
        Assert.Equal(0, skipAll.Count);

        var anyFalse = new List<bool>();
        var allFalse = new List<bool>();
        var containsFalse = new List<bool>();
        var longCount = new List<long>();
        Signal.FromEnumerable([1, Two, Three]).Any(value => value > Nine).Subscribe(anyFalse.Add);
        Signal.FromEnumerable([Two, Four, Five]).All(value => value % Two == 0).Subscribe(allFalse.Add);
        Signal.FromEnumerable([Two, Four, Six]).Contains(Seven).Subscribe(containsFalse.Add);
        Signal.FromEnumerable([1, Two, Three, Four]).LongCount(value => value % Two == 0).Subscribe(longCount.Add);
        Assert.Equal(ExpectedFalse, anyFalse);
        Assert.Equal(ExpectedFalse, allFalse);
        Assert.Equal(ExpectedFalse, containsFalse);
        Assert.Equal(ExpectedLongCount, longCount);

        var selectMany = new List<string>();
        Signal.FromEnumerable([1, Two])
            .FlatMap(value => Signal.FromEnumerable([value, value + Ten]), (outer, inner) => outer + ":" + inner)
            .Subscribe(selectMany.Add);
        Assert.Equal(ExpectedSelectMany, selectMany);

        var flatMapValues = new List<int>();
        Signal.FromEnumerable([1, Two])
            .FlatMapValues<int, int>(value => [value, value * Ten])
            .Subscribe(flatMapValues.Add);
        Assert.Equal(ExpectedFlatMapValues, flatMapValues);
    }

    /// <summary>
    /// Exercises error materialization, recovery, resume, and retry branches.
    /// </summary>
    [Test]
    public void ErrorOperatorsMaterializeRecoverAndResumeDeterministically()
    {
        var sparkKinds = new List<SparkKind>();
        var sparkErrors = new List<string>();
        var unsparkValues = new List<int>();
        var unsparkErrors = new List<string>();
        var rescueValues = new List<int>();
        var resumeValues = new List<int>();
        var finalErrors = new List<string>();

        Signal.Fail<int>(new InvalidOperationException("spark"))
            .Spark()
            .Subscribe(spark =>
            {
                sparkKinds.Add(spark.Kind);
                if (spark.Exception == null)
                {
                    return;
                }

                sparkErrors.Add(spark.Exception.Message);
            });

        Signal.FromEnumerable(
            [
                Spark.CreateOnNext(1),
                Spark.CreateOnError<int>(new InvalidOperationException("unspark")),
                Spark.CreateOnCompleted<int>(),
            ])
            .Unspark()
            .Subscribe(unsparkValues.Add, ex => unsparkErrors.Add(ex.Message));

        Signal.Fail<int>(new InvalidOperationException("recover"))
            .Rescue(error => Signal.Emit(error.Message.Length))
            .Subscribe(rescueValues.Add);

        Signal.Fail<int>(new InvalidOperationException("resume"))
            .Resume(Signal.FromEnumerable([Four, Five]))
            .Subscribe(resumeValues.Add);

        Signal.Lazy(() => Signal.Fail<int>(new InvalidOperationException("stop")))
            .Reattempt(1)
            .Subscribe(_ => { }, ex => finalErrors.Add(ex.Message));

        Assert.Equal(ExpectedSparkKinds, sparkKinds);
        Assert.Equal(ExpectedSparkErrors, sparkErrors);
        Assert.Equal(ExpectedUnsparkValues, unsparkValues);
        Assert.Equal(ExpectedUnsparkErrors, unsparkErrors);
        Assert.Equal(ExpectedRescueValues, rescueValues);
        Assert.Equal(ExpectedResumeValues, resumeValues);
        Assert.Equal(ExpectedFinalErrors, finalErrors);
    }

    /// <summary>
    /// Exercises higher-order ordering, racing, switching, and latest-value behavior.
    /// </summary>
    [Test]
    public void HigherOrderOperatorsHandleAsyncOrderingRacesSwitchingAndLatestValues()
    {
        var first = new Signal<int>();
        var second = new Signal<int>();
        var outer = new Signal<IObservable<int>>();
        var concatValues = new List<int>();
        var mergeValues = new List<int>();
        var raceValues = new List<int>();
        var switchValues = new List<int>();
        var withLatestValues = new List<string>();
        var zipShortValues = new List<int>();
        var forkJoinEmpty = new List<int>();
        var completed = new Dictionary<string, int>();

        outer.Chain().Subscribe(concatValues.Add, ex => throw ex, () => completed["concat"] = 1);
        outer.OnNext(first);
        outer.OnNext(second);
        first.OnNext(1);
        second.OnNext(Twenty);
        first.OnNext(Two);
        first.OnCompleted();
        second.OnNext(TwentyOne);
        second.OnCompleted();
        outer.OnCompleted();

        Signal.Blend(Signal.FromEnumerable([1, Two]), Signal.FromEnumerable([Three]))
            .Subscribe(mergeValues.Add, ex => throw ex, () => completed["merge"] = 1);

        var raceLoser = new Signal<int>();
        var raceWinner = new Signal<int>();
        Signal.Race(raceLoser, raceWinner).Subscribe(raceValues.Add, ex => throw ex, () => completed["race"] = 1);
        raceWinner.OnNext(Seven);
        raceLoser.OnNext(NinetyNine);
        raceWinner.OnCompleted();

        var switchOuter = new Signal<IObservable<int>>();
        var oldInner = new Signal<int>();
        var newInner = new Signal<int>();
        switchOuter.SwitchTo().Subscribe(switchValues.Add, ex => throw ex, () => completed["switch"] = 1);
        switchOuter.OnNext(oldInner);
        oldInner.OnNext(1);
        switchOuter.OnNext(newInner);
        oldInner.OnNext(Two);
        newInner.OnNext(Three);
        switchOuter.OnCompleted();
        newInner.OnCompleted();

        var left = new Signal<int>();
        var right = new Signal<string>();
        left.Latch(right, (l, r) => l + r).Subscribe(withLatestValues.Add);
        left.OnNext(1);
        right.OnNext("a");
        left.OnNext(Two);
        right.OnNext("b");
        left.OnNext(Three);
        left.OnCompleted();

        Signal.FromEnumerable([1, Two, Three])
            .Pair(Signal.Emit(Ten), (l, r) => l + r)
            .Subscribe(zipShortValues.Add, ex => throw ex, () => completed["zip"] = 1);
        Signal.None<int>()
            .ForkJoin(Signal.Emit(1), (l, r) => l + r)
            .Subscribe(forkJoinEmpty.Add, ex => throw ex, () => completed["forkJoinEmpty"] = 1);

        Assert.Equal(ExpectedConcatValues, concatValues);
        Assert.Equal(ExpectedMergeValues, mergeValues.Order());
        Assert.Equal(ExpectedRaceValues, raceValues);
        Assert.Equal(ExpectedSwitchValues, switchValues);
        Assert.Equal(ExpectedWithLatestValues, withLatestValues);
        Assert.Equal(ExpectedZipShortValues, zipShortValues);
        Assert.Equal(0, forkJoinEmpty.Count);
        Assert.Equal(1, completed["concat"]);
        Assert.Equal(1, completed["merge"]);
        Assert.Equal(1, completed["race"]);
        Assert.Equal(1, completed["switch"]);
        Assert.Equal(1, completed["zip"]);
        Assert.Equal(1, completed["forkJoinEmpty"]);
    }

    /// <summary>
    /// Exercises virtual-time operators and aliases.
    /// </summary>
    [Test]
    public void VirtualTimeOperatorsCoverDelayTimeoutSampleTimerAndTimestampAliases()
    {
        var clock = new TestClock();
        var delayStartValues = new List<int>();
        var delayedValues = new List<int>();
        var timeoutValues = new List<int>();
        var timeoutErrors = new List<string>();
        var pulseValues = new List<long>();
        var timerValues = new List<long>();
        var timestamps = new List<Moment<int>>();

        var manual = new Signal<int>();
        manual.DelayStart(TimeSpan.FromTicks(Five), clock).Subscribe(delayStartValues.Add);
        manual.OnNext(1);
        clock.AdvanceBy(TimeSpan.FromTicks(Four));
        Assert.Equal(0, delayStartValues.Count);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        manual.OnNext(Two);
        Assert.Equal(ExpectedDelayStartValues, delayStartValues);

        Signal.FromEnumerable([Three, Four]).Shift(TimeSpan.FromTicks(Three), clock).Subscribe(delayedValues.Add);
        clock.AdvanceBy(TimeSpan.FromTicks(Two));
        Assert.Equal(0, delayedValues.Count);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        Assert.Equal(ExpectedDelayedValues, delayedValues);

        var never = new Signal<int>();
        never.Expire(TimeSpan.FromTicks(Four), clock).Subscribe(timeoutValues.Add, ex => timeoutErrors.Add(ex.GetType().Name));
        clock.AdvanceBy(TimeSpan.FromTicks(Four));
        never.OnNext(FortyTwo);
        Assert.Equal(0, timeoutValues.Count);
        Assert.Equal(ExpectedTimeoutErrors, timeoutErrors);

        var completed = new Signal<int>();
        completed.Expire(TimeSpan.FromTicks(Ten), clock).Subscribe(timeoutValues.Add);
        completed.OnNext(Seven);
        completed.OnCompleted();
        clock.AdvanceBy(TimeSpan.FromTicks(Ten));
        Assert.Equal(ExpectedRaceValues, timeoutValues);

        var pulse = Signal.Pulse(TimeSpan.FromTicks(Two), clock).Subscribe(pulseValues.Add);
        clock.AdvanceBy(TimeSpan.FromTicks(Six));
        pulse.Dispose();
        Assert.Equal(ExpectedTimerValues, pulseValues);

        var timer = Signal.After(TimeSpan.FromTicks(Three), TimeSpan.FromTicks(Two), clock).Subscribe(timerValues.Add);
        clock.AdvanceBy(TimeSpan.FromTicks(Three));
        clock.AdvanceBy(TimeSpan.FromTicks(Four));
        timer.Dispose();
        Assert.Equal(ExpectedTimerValues, timerValues);

        Signal.FromEnumerable([Eight, Nine]).Timestamp(clock).Subscribe(timestamps.Add);
        Assert.Equal(ExpectedTimestampValues, timestamps.Select(item => item.Value));
        Assert.True(timestamps.TrueForAll(item => item.Timestamp == clock.Now));
    }

    /// <summary>
    /// Exercises task, async-enumerable, and terminal task branches.
    /// </summary>
    /// <returns>A task that completes when asynchronous coverage has run.</returns>
    [Test]
    public async Task FactoriesTasksAndTerminalTasksCoverCancellationFaultAndEmptyBranches()
    {
        var useErrors = new List<string>();
        var taskErrors = new List<string>();
        var asyncValues = new List<int>();
        var asyncErrors = new List<string>();

        Signal.Use(() => Disposable.Empty, _ => (IObservable<int>)null!).Subscribe(_ => { }, ex => useErrors.Add(ex.Message));
        Signal.Use<IDisposable, int>(() => throw new InvalidOperationException("resource"), _ => Signal.Emit(1)).Subscribe(_ => { }, ex => useErrors.Add(ex.Message));

        await ObserveTaskError(Task.FromCanceled<int>(new(true)), taskErrors);
        await ObserveTaskError(Task.FromException<int>(new InvalidOperationException("faulted")), taskErrors);

        static async IAsyncEnumerable<int> ThrowingAsyncEnumerable()
        {
            yield return 1;
            await Task.Yield();
            throw new InvalidOperationException("async");
        }

        Signal.FromAsyncEnumerable(ThrowingAsyncEnumerable()).Subscribe(asyncValues.Add, ex => asyncErrors.Add(ex.Message));
        await SpinUntil(() => asyncErrors.Count == 1, TimeSpan.FromSeconds(2));

        var firstFailure = await AssertTaskFault(() => Signal.None<int>().FirstAsync(), typeof(InvalidOperationException));
        var collectFailure = await AssertTaskFault(
            () => Signal.Fail<int>(new InvalidOperationException("collect")).CollectArrayAsync(),
            typeof(InvalidOperationException));
        var listFailure = await AssertTaskFault(
            () => Signal.Fail<int>(new InvalidOperationException("list")).CollectListAsync(),
            typeof(InvalidOperationException));

        Assert.Equal(ExpectedUseErrors, useErrors);
        Assert.Equal(ExpectedTaskErrors, taskErrors);
        Assert.Equal(ExpectedUnsparkValues, asyncValues);
        Assert.Equal(ExpectedAsyncErrors, asyncErrors);
        Assert.Equal("The source completed without producing a value.", firstFailure.Message);
        Assert.Equal("collect", collectFailure.Message);
        Assert.Equal("list", listFailure.Message);
    }

    /// <summary>
    /// Exercises value types, disposables, and handle delegates.
    /// </summary>
    [Test]
    public void CoreValueTypesDisposablesAndHandlesCoverEqualityAndLifecycleBranches()
    {
        var moment = new Moment<int>(Seven, new(CalendarYear, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var sameMoment = new Moment<int>(Seven, moment.Timestamp);
        var differentMoment = new Moment<int>(Eight, moment.Timestamp.AddTicks(1));
        var interval = new TimeInterval<int>(Seven, TimeSpan.FromTicks(Three));
        var sameInterval = new TimeInterval<int>(Seven, TimeSpan.FromTicks(Three));
        var differentInterval = new TimeInterval<int>(Eight, TimeSpan.FromTicks(Four));
        var rxVoid = default(RxVoid);
        var ignored = 0;
        var thrown = new InvalidOperationException("throw-me");

        Assert.True(moment == sameMoment);
        Assert.True(moment.Equals((object)sameMoment));
        Assert.False(moment == differentMoment);
        Assert.True(moment != differentMoment);
        Assert.False(moment.Equals("not a moment"));
        Assert.Equal(moment.GetHashCode(), sameMoment.GetHashCode());
        Assert.True(moment.ToString().Contains("7", StringComparison.Ordinal));

        Assert.True(interval == sameInterval);
        Assert.True(interval.Equals((object)sameInterval));
        Assert.False(interval == differentInterval);
        Assert.True(interval != differentInterval);
        Assert.False(interval.Equals("not an interval"));
        Assert.Equal(interval.GetHashCode(), sameInterval.GetHashCode());
        Assert.True(interval.ToString().Contains("7", StringComparison.Ordinal));

        Assert.True(rxVoid.Equals(default));
        Assert.True(rxVoid.Equals((object)default(RxVoid)));
        Assert.Equal(0, rxVoid.GetHashCode());
        Assert.Equal("()", rxVoid.ToString());

        InvokeInternalHandleMembers(thrown);
        Handle.CatchIgnore<int>(new InvalidOperationException("ignored")).Subscribe(_ => ignored++);
        Assert.Equal(0, ignored);

        var boolean = new BooleanDisposable();
        Assert.False(boolean.IsDisposed);
        boolean.Dispose();
        Assert.True(boolean.IsDisposed);

        var slotDisposed = 0;
        var assignmentDisposed = 0;
        var pocketDisposed = 0;
        new Slot(Disposable.Create(() => slotDisposed++), () => slotDisposed++).Dispose();
        new AssignmentSlot(Disposable.Create(() => assignmentDisposed++), () => assignmentDisposed++).Dispose();
        new Pocket(Disposable.Create(() => pocketDisposed++)).Dispose();
        Assert.Equal(Two, slotDisposed);
        Assert.Equal(Two, assignmentDisposed);
        Assert.Equal(1, pocketDisposed);

        var single = new SingleDisposable(Disposable.Create(() => { }), () => { });
        Assert.Throws<InvalidOperationException>(() => single.Create(Disposable.Empty));
        var replaceableFirst = 0;
        var replaceableSecond = 0;
        var replaceable = new SingleReplaceableDisposable(Disposable.Create(() => replaceableFirst++));
        replaceable.Create(Disposable.Create(() => replaceableSecond++));
        replaceable.Dispose();
        Assert.Equal(1, replaceableFirst);
        Assert.Equal(1, replaceableSecond);

        var multiple = new MultipleDisposable();
        Assert.Throws<ArgumentNullException>(() => multiple.Remove(null));
        multiple.Dispose();
        var lateDisposed = 0;
        multiple.Add(Disposable.Create(() => lateDisposed++));
        Assert.Equal(1, lateDisposed);
        Assert.True(multiple.IsDisposed);
    }

    /// <summary>
    /// Exercises spark value, error, completion, equality, and accept overloads.
    /// </summary>
    [Test]
    public void SparksCoverValueErrorCompletionEqualityAndAcceptOverloads()
    {
        var next = Spark.CreateOnNext(FortyTwo);
        var sameNext = Spark.CreateOnNext(FortyTwo);
        var differentNext = Spark.CreateOnNext(FortyThree);
        var error = new InvalidOperationException("spark-error");
        var errorSpark = Spark.CreateOnError<int>(error);
        var sameError = Spark.CreateOnError<int>(error);
        var completed = Spark.CreateOnCompleted<int>();
        var completedAgain = Spark.CreateOnCompleted<int>();
        var observer = new RecordingResultObserver<int>();
        var observableValues = new List<int>();
        var observableCompleted = 0;

        Assert.True(next == sameNext);
        Assert.True(next != differentNext);
        Assert.False(next.Equals(completed));
        Assert.True(next.HasValue);
        Assert.Equal(FortyTwo, next.Value);
        Assert.Equal(SparkKind.OnNext, next.Kind);
        Assert.True(next.ToString().Contains(FortyTwo.ToString(), StringComparison.Ordinal));
        Assert.Equal(next.GetHashCode(), sameNext.GetHashCode());
        next.Accept((IObserver<int>)observer);
        Assert.Equal("next:42", next.Accept((IObserver<int, string>)observer));
        next.Accept(value => observer.Events.Add("delegate-next:" + value), ex => observer.Events.Add(ex.Message), () => observer.Events.Add("delegate-completed"));
        Assert.Equal("fn-next:42", next.Accept(value => "fn-next:" + value, ex => ex.Message, () => FunctionCompletedText));
        Assert.Throws<ArgumentNullException>(() => next.Accept((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => next.Accept((IObserver<int, string>)null!));
        Assert.Throws<ArgumentNullException>(() => next.Accept(null!, ex => { }, () => { }));
        Assert.Throws<ArgumentNullException>(() => next.Accept(value => { }, null!, () => { }));
        Assert.Throws<ArgumentNullException>(() => next.Accept(value => { }, ex => { }, null!));
        Assert.Throws<ArgumentNullException>(() => next.Accept(null!, ex => ex.Message, () => "done"));
        Assert.Throws<ArgumentNullException>(() => next.Accept(value => value.ToString(), null!, () => "done"));
        Assert.Throws<ArgumentNullException>(() => next.Accept(value => value.ToString(), ex => ex.Message, null!));

        Assert.True(errorSpark == sameError);
        Assert.True(errorSpark != next);
        Assert.False(errorSpark.HasValue);
        Assert.Equal(error, errorSpark.Exception);
        Assert.Equal(SparkKind.OnError, errorSpark.Kind);
        Assert.Equal(0, errorSpark.Value);
        Assert.True(errorSpark.ToString().Contains(nameof(InvalidOperationException), StringComparison.Ordinal));
        Assert.Equal(errorSpark.GetHashCode(), sameError.GetHashCode());
        errorSpark.Accept((IObserver<int>)observer);
        Assert.Equal("error:spark-error", errorSpark.Accept((IObserver<int, string>)observer));
        errorSpark.Accept(value => observer.Events.Add(value.ToString()), ex => observer.Events.Add("delegate-error:" + ex.Message), () => observer.Events.Add("delegate-completed"));
        Assert.Equal("fn-error:spark-error", errorSpark.Accept(value => value.ToString(), ex => "fn-error:" + ex.Message, () => FunctionCompletedText));
        Assert.Throws<ArgumentNullException>(() => Spark.CreateOnError<int>(null!));
        Assert.Throws<ArgumentNullException>(() => errorSpark.Accept((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => errorSpark.Accept((IObserver<int, string>)null!));
        Assert.Throws<ArgumentNullException>(() => errorSpark.Accept(null!, ex => { }, () => { }));
        Assert.Throws<ArgumentNullException>(() => errorSpark.Accept(value => { }, null!, () => { }));
        Assert.Throws<ArgumentNullException>(() => errorSpark.Accept(value => { }, ex => { }, null!));
        Assert.Throws<ArgumentNullException>(() => errorSpark.Accept(null!, ex => ex.Message, () => "done"));
        Assert.Throws<ArgumentNullException>(() => errorSpark.Accept(value => value.ToString(), null!, () => "done"));
        Assert.Throws<ArgumentNullException>(() => errorSpark.Accept(value => value.ToString(), ex => ex.Message, null!));

        Assert.True(completed == completedAgain);
        Assert.True(completed.Equals(completedAgain));
        Assert.False(completed.HasValue);
        Assert.Equal(SparkKind.OnCompleted, completed.Kind);
        Assert.Equal(0, completed.Value);
        Assert.Equal("OnCompleted()", completed.ToString());
        completed.Accept((IObserver<int>)observer);
        Assert.Equal(CompletedText, completed.Accept((IObserver<int, string>)observer));
        completed.Accept(value => observer.Events.Add(value.ToString()), ex => observer.Events.Add(ex.Message), () => observer.Events.Add("delegate-completed"));
        Assert.Equal(FunctionCompletedText, completed.Accept(value => value.ToString(), ex => ex.Message, () => FunctionCompletedText));
        Assert.Throws<ArgumentNullException>(() => completed.Accept((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => completed.Accept((IObserver<int, string>)null!));
        Assert.Throws<ArgumentNullException>(() => completed.Accept(null!, ex => { }, () => { }));
        Assert.Throws<ArgumentNullException>(() => completed.Accept(value => { }, null!, () => { }));
        Assert.Throws<ArgumentNullException>(() => completed.Accept(value => { }, ex => { }, null!));
        Assert.Throws<ArgumentNullException>(() => completed.Accept(null!, ex => ex.Message, () => "done"));
        Assert.Throws<ArgumentNullException>(() => completed.Accept(value => value.ToString(), null!, () => "done"));
        Assert.Throws<ArgumentNullException>(() => completed.Accept(value => value.ToString(), ex => ex.Message, null!));
        Assert.Throws<ArgumentNullException>(() => completed.ToObservable(null!));

        next.ToObservable().Subscribe(observableValues.Add, ex => throw ex, () => observableCompleted++);
        Assert.Equal(ExpectedObservableValues, observableValues);
        Assert.Equal(1, observableCompleted);
        Assert.Contains("next:42", observer.Events);
        Assert.Contains("error:spark-error", observer.Events);
        Assert.Contains(CompletedText, observer.Events);
    }

    /// <summary>
    /// Covers null guards for unary operators.
    /// </summary>
    /// <param name="source">The non-null source used for null argument checks.</param>
    private static void CoverUnaryOperatorNullGuards(IObservable<int> source)
    {
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Map(value => value));
        Assert.Throws<ArgumentNullException>(() => source.Map<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).MapWith(1, (_, value) => value));
        Assert.Throws<ArgumentNullException>(() => source.MapWith<int, int, int>(1, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Keep(value => true));
        Assert.Throws<ArgumentNullException>(() => source.Keep(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).KeepWith(1, (_, _) => true));
        Assert.Throws<ArgumentNullException>(() => source.KeepWith(1, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<string?>)null!).KeepNotNull());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<object>)null!).KeepType<string>());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<object>)null!).CastTo<string>());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Tap(value => { }));
        Assert.Throws<ArgumentNullException>(() => source.Tap(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).TapWith(1, (_, _) => { }));
        Assert.Throws<ArgumentNullException>(() => source.TapWith(1, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Fold(0, (left, right) => left + right));
        Assert.Throws<ArgumentNullException>(() => source.Fold(0, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Reduce(0, (left, right) => left + right));
        Assert.Throws<ArgumentNullException>(() => source.Reduce(0, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Take(1));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Skip(1));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Distinct());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Unique());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Spark());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<Spark<int>>)null!).Unspark());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Shift(TimeSpan.Zero, Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Expire(TimeSpan.Zero, Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).CollectList());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).ToSignal());
    }

    /// <summary>
    /// Covers null guards for higher-order operators.
    /// </summary>
    /// <param name="source">The non-null source used for null argument checks.</param>
    private static void CoverHigherOrderOperatorNullGuards(IObservable<int> source)
    {
        Assert.Throws<ArgumentNullException>(() => ((IObservable<IObservable<int>>)null!).Chain());
        Assert.Throws<ArgumentNullException>(() => Signal.Chain<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Chain(source, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Blend<int>((IObservable<int>[])null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Blend(source, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Race<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Race(source, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Pair(source, (left, _) => left));
        Assert.Throws<ArgumentNullException>(() => source.Pair<int, int, int>(null!, (left, _) => left));
        Assert.Throws<ArgumentNullException>(() => source.Pair<int, int, int>(source, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).SyncLatest(source, (left, _) => left));
        Assert.Throws<ArgumentNullException>(() => source.SyncLatest<int, int, int>(null!, (left, _) => left));
        Assert.Throws<ArgumentNullException>(() => source.SyncLatest<int, int, int>(source, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Latch(source, (left, _) => left));
        Assert.Throws<ArgumentNullException>(() => source.Latch<int, int, int>(null!, (left, _) => left));
        Assert.Throws<ArgumentNullException>(() => source.Latch<int, int, int>(source, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<IObservable<int>>)null!).SwitchTo());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Reattempt(1));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Resume(source));
        Assert.Throws<ArgumentNullException>(() => source.Resume(null!));
    }

    /// <summary>
    /// Covers null guards for parity operators.
    /// </summary>
    /// <param name="source">The non-null source used for null argument checks.</param>
    private static void CoverParityOperatorNullGuards(IObservable<int> source)
    {
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Prepend(1));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Append(1));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).IgnoreValues());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).DefaultIfEmpty());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).DistinctBy(value => value));
        Assert.Throws<ArgumentNullException>(() => source.DistinctBy<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).UniqueBy(value => value));
        Assert.Throws<ArgumentNullException>(() => source.UniqueBy<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).TakeWhile(value => true));
        Assert.Throws<ArgumentNullException>(() => source.TakeWhile(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).SkipWhile(value => true));
        Assert.Throws<ArgumentNullException>(() => source.SkipWhile(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).FlatMap(value => source));
        Assert.Throws<ArgumentNullException>(() => source.FlatMap<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).FlatMapValues<int, int>(value => [value]));
        Assert.Throws<ArgumentNullException>(() => source.FlatMapValues<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Count());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).LongCount());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Any());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).All(value => true));
        Assert.Throws<ArgumentNullException>(() => source.All(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).DelayStart(TimeSpan.Zero, Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Calm(TimeSpan.Zero, Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Probe(TimeSpan.Zero, Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Timestamp(Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).TimeInterval(Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).ForkJoin(source, (left, _) => left));
        Assert.Throws<ArgumentNullException>(() => source.ForkJoin<int, int, int>(null!, (left, _) => left));
        Assert.Throws<ArgumentNullException>(() => source.ForkJoin<int, int, int>(source, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).CollectArrayAsync());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).CollectListAsync());
    }

    /// <summary>
    /// Covers null guards for factories and observers.
    /// </summary>
    /// <param name="objects">The non-null object source used for null argument checks.</param>
    private static void CoverFactoryAndObserverNullGuards(IObservable<object?> objects)
    {
        Assert.Throws<ArgumentNullException>(() => Signal.Create<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Lazy<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromEnumerable<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromTask((Task<int>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromAsyncEnumerable<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Use<IDisposable, int>(null!, resource => Signal.Emit(1)));
        Assert.Throws<ArgumentNullException>(() => Signal.Use(() => Disposable.Empty, (Func<IDisposable, IObservable<int>>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Use<IDisposable, int>(null!, resource => Signal.Emit(1)));
        Assert.Throws<ArgumentNullException>(() => Signal.Use(() => Disposable.Empty, (Func<IDisposable, IObservable<int>>)null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Subscribe(value => { }));
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(1).Subscribe((Action<int>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(1).Subscribe(value => { }, (Action<Exception>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(1).Subscribe(value => { }, ex => { }, null!));
        Assert.Throws<ArgumentNullException>(() => objects.CastTo<string>().Subscribe((IObserver<string>)null!));
    }

    /// <summary>
    /// Observes the error produced by a task-backed signal.
    /// </summary>
    /// <param name="task">The source task.</param>
    /// <param name="errors">The error name sink.</param>
    /// <returns>A task that completes when the error has been observed.</returns>
    private static async Task ObserveTaskError(Task<int> task, List<string> errors)
    {
        Signal.FromTask(task).Subscribe(_ => { }, ex => errors.Add(ex.GetType().Name));
        await SpinUntil(() => errors.Count > 0, TimeSpan.FromSeconds(2));
    }

    /// <summary>
    /// Invokes the public handle members directly.
    /// </summary>
    /// <param name="exception">The exception expected from throwing delegates.</param>
    private static void InvokeInternalHandleMembers(Exception exception)
    {
        Handle.Nop();
        Handle<int>.Ignore(1);
        Handle<int, int>.Ignore(1, Two);
        Handle<int, int, int>.Ignore(1, Two, Three);

        Assert.Equal("x", Handle<string>.Identity("x"));

        Assert.Throws<InvalidOperationException>(() => Handle.Throw(exception));
        Assert.Throws<InvalidOperationException>(() => Handle<int>.Throw(exception, 1));
        Assert.Throws<InvalidOperationException>(() => Handle<int, int>.Throw(exception, 1, Two));
        Assert.Throws<InvalidOperationException>(() => Handle<int, int, int>.Throw(exception, 1, Two, Three));
    }

    /// <summary>
    /// Asserts that a task factory faults with the expected exception type.
    /// </summary>
    /// <param name="taskFactory">The task factory.</param>
    /// <param name="expectedExceptionType">The expected exception type.</param>
    /// <returns>The captured exception.</returns>
    private static async Task<Exception> AssertTaskFault(Func<Task> taskFactory, Type expectedExceptionType)
    {
        try
        {
            await taskFactory();
        }
        catch (Exception exception) when (exception.GetType() == expectedExceptionType)
        {
            return exception;
        }

        throw new InvalidOperationException("Expected task fault " + expectedExceptionType.Name + ".");
    }

    /// <summary>
    /// Spins asynchronously until the condition is true or the timeout elapses.
    /// </summary>
    /// <param name="condition">The completion condition.</param>
    /// <param name="timeout">The maximum wait duration.</param>
    /// <returns>A task that completes when the condition is reached.</returns>
    private static async Task SpinUntil(Func<bool> condition, TimeSpan timeout)
    {
        var timeoutTask = Task.Delay(timeout);
        while (!condition())
        {
            if (timeoutTask.IsCompleted)
            {
                throw new TimeoutException("Condition was not reached before " + nameof(timeout) + ".");
            }

            await Task.Delay(PollDelayMilliseconds);
        }
    }

    /// <summary>
    /// Records observer events and result values.
    /// </summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class RecordingResultObserver<T> : IObserver<T>, IObserver<T, string>
    {
        /// <summary>
        /// Gets the recorded events.
        /// </summary>
        public List<string> Events { get; } = [];

        /// <summary>
        /// Records completion.
        /// </summary>
        public void OnCompleted() => Events.Add(CompletedText);

        /// <summary>
        /// Records an error.
        /// </summary>
        /// <param name="error">The observed error.</param>
        public void OnError(Exception error) => Events.Add("error:" + error.Message);

        /// <summary>
        /// Records a next value.
        /// </summary>
        /// <param name="value">The observed value.</param>
        public void OnNext(T value) => Events.Add("next:" + value);

        /// <summary>
        /// Records completion and returns a result.
        /// </summary>
        /// <returns>The completion result.</returns>
        string IObserver<T, string>.OnCompleted()
        {
            Events.Add(CompletedText);
            return CompletedText;
        }

        /// <summary>
        /// Records an error and returns a result.
        /// </summary>
        /// <param name="exception">The observed error.</param>
        /// <returns>The error result.</returns>
        string IObserver<T, string>.OnError(Exception exception)
        {
            Events.Add("error:" + exception.Message);
            return "error:" + exception.Message;
        }

        /// <summary>
        /// Records a next value and returns a result.
        /// </summary>
        /// <param name="value">The observed value.</param>
        /// <returns>The next result.</returns>
        string IObserver<T, string>.OnNext(T value)
        {
            Events.Add("next:" + value);
            return "next:" + value;
        }
    }
}
