// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;
using TUnit.Core;

namespace ReactiveUI.Primitives.Tests;

#pragma warning disable CS8602 // Tests intentionally exercise nullable edge cases and Spark null payload contracts.

public class CoverageCompletionTests
{
    [Test]
    public void NullGuardsCoverPublicFactoryOperatorAndObserverContracts()
    {
        IObservable<int> source = Signal.Return(1);
        IObservable<object?> objects = Signal.Return<object?>("value");

        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Map(value => value));
        Assert.Throws<ArgumentNullException>(() => source.Map<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => source.MapWith<int, int, int>(1, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Keep(value => true));
        Assert.Throws<ArgumentNullException>(() => source.Keep(null!));
        Assert.Throws<ArgumentNullException>(() => source.KeepWith(1, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<string?>)null!).KeepNotNull());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<object?>)null!).OfType<string>());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<object?>)null!).Cast<string>());
        Assert.Throws<ArgumentNullException>(() => source.Tap(null!));
        Assert.Throws<ArgumentNullException>(() => source.TapWith(1, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Scan(0, (sum, value) => sum + value));
        Assert.Throws<ArgumentNullException>(() => source.Scan<int, int>(0, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Fold(0, (sum, value) => sum + value));
        Assert.Throws<ArgumentNullException>(() => source.Fold<int, int>(0, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Take(1));
        Assert.Throws<ArgumentOutOfRangeException>(() => source.Take(-1));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Skip(1));
        Assert.Throws<ArgumentOutOfRangeException>(() => source.Skip(-1));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Distinct());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).DistinctUntilChanged());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Sparkify());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<Spark<int>>)null!).Unspark());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<IObservable<int>>)null!).Concat());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<IObservable<int>>)null!).Merge());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<IObservable<int>>)null!).Race());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Zip<int, int, int>(Signal.Return(1), (left, right) => left + right));
        Assert.Throws<ArgumentNullException>(() => source.Zip<int, int, int>(null!, (left, right) => left + right));
        Assert.Throws<ArgumentNullException>(() => source.Zip<int, int, int>(Signal.Return(1), null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).CombineLatest<int, int, int>(Signal.Return(1), (left, right) => left + right));
        Assert.Throws<ArgumentNullException>(() => source.CombineLatest<int, int, int>(null!, (left, right) => left + right));
        Assert.Throws<ArgumentNullException>(() => source.CombineLatest<int, int, int>(Signal.Return(1), null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).WithLatest<int, int, int>(Signal.Return(1), (left, right) => left + right));
        Assert.Throws<ArgumentNullException>(() => source.WithLatest<int, int, int>(null!, (left, right) => left + right));
        Assert.Throws<ArgumentNullException>(() => source.WithLatest<int, int, int>(Signal.Return(1), null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<IObservable<int>>)null!).Switch());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Retry(1));
        Assert.Throws<ArgumentOutOfRangeException>(() => source.Retry(-1));
        Assert.Throws<ArgumentNullException>(() => source.Resume(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Delay(TimeSpan.Zero));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Timeout(TimeSpan.Zero));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).CollectList());
        Assert.Throws<ArgumentNullException>(() => ((IEnumerable<int>)null!).ToSignal());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).ToSignal());

        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Prepend(1));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Append(1));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).IgnoreValues());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).DefaultIfEmpty());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).DistinctBy(value => value));
        Assert.Throws<ArgumentNullException>(() => source.DistinctBy<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).DistinctUntilChangedBy(value => value));
        Assert.Throws<ArgumentNullException>(() => source.DistinctUntilChangedBy<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).TakeWhile(value => true));
        Assert.Throws<ArgumentNullException>(() => source.TakeWhile(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).SkipWhile(value => true));
        Assert.Throws<ArgumentNullException>(() => source.SkipWhile(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).SelectMany(value => Signal.Return(value)));
        Assert.Throws<ArgumentNullException>(() => source.SelectMany<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => source.SelectMany<int, int, int>(null!, (outer, inner) => outer + inner));
        Assert.Throws<ArgumentNullException>(() => source.SelectMany<int, int, int>(value => Signal.Return(value), null!));
        Assert.Throws<ArgumentNullException>(() => source.Count(null!));
        Assert.Throws<ArgumentNullException>(() => source.LongCount(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Any(value => true));
        Assert.Throws<ArgumentNullException>(() => source.Any(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).All(value => true));
        Assert.Throws<ArgumentNullException>(() => source.All(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).DelayStart(TimeSpan.Zero));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Throttle(TimeSpan.Zero));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Sample(TimeSpan.FromTicks(1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => source.Sample(TimeSpan.FromTicks(-1)));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Timestamp());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).TimeInterval());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).ForkJoin<int, int, int>(Signal.Return(1), (left, right) => left + right));
        Assert.Throws<ArgumentNullException>(() => source.ForkJoin<int, int, int>(null!, (left, right) => left + right));
        Assert.Throws<ArgumentNullException>(() => source.ForkJoin<int, int, int>(Signal.Return(1), null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).CollectArrayAsync());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).CollectListAsync());

        Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Range(0, -1));
        Assert.Throws<ArgumentNullException>(() => Signal.Range(0, 1, null!));
        Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Repeat(1, -1));
        Assert.Throws<ArgumentNullException>(() => Signal.Unfold(0, null!, value => value + 1, value => value));
        Assert.Throws<ArgumentNullException>(() => Signal.Unfold<int, int>(0, value => true, null!, value => value));
        Assert.Throws<ArgumentNullException>(() => Signal.Unfold<int, int>(0, value => true, value => value + 1, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Use<IDisposable, int>(null!, _ => Signal.Return(1)));
        Assert.Throws<ArgumentNullException>(() => Signal.Use<IDisposable, int>(() => Disposable.Empty, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromEnumerable<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromTask((Task<int>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromAsyncEnumerable<int>(null!));
        Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Every(TimeSpan.FromTicks(-1)));

        Assert.Throws<NullReferenceException>(() => objects.Subscribe(null!));
        Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(_ => { }, (Action<Exception>)null!));
        Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(_ => { }, (Action)null!));
        Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(_ => { }, _ => { }, null!));
        Assert.Throws<ArgumentNullException>(() => Witness.Safe<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Witness.Safe(Witness.Create<int>(_ => { }), null!));
        Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(_ => { }).OnError(null!));
        Assert.Throws<ArgumentNullException>(() => new CancellationDisposable(null!));
    }

    [Test]
    public void OperatorSurfaceCoversSuccessErrorAndEarlyTerminationBranches()
    {
        var values = new List<string>();
        var sideEffects = new List<string>();
        var terminal = 0;

        Signal.FromEnumerable(new object?[] { "a", null, 2, "bbb", "cc", 3 })
            .OfType<string>()
            .MapWith("!", (suffix, value) => value + suffix)
            .KeepWith(2, (min, value) => value.Length >= min)
            .TapWith(sideEffects, (sink, value) => sink.Add(value))
            .Cast<string>()
            .Skip(1)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .DistinctUntilChanged(StringComparer.OrdinalIgnoreCase)
            .Subscribe(values.Add, ex => throw ex, () => terminal++);

        Assert.Equal(new[] { "bbb!", "cc!" }, values);
        Assert.Equal(new[] { "a!", "bbb!", "cc!" }, sideEffects);
        Assert.Equal(1, terminal);

        var keepNotNull = new List<string>();
        Signal.FromEnumerable([null, "x", null, "y"]).KeepNotNull().Subscribe(keepNotNull.Add);
        Assert.Equal(new[] { "x", "y" }, keepNotNull);

        var emptyTake = new List<int>();
        var emptyTakeCompleted = 0;
        Signal.Range(1, 3).Take(0).Subscribe(emptyTake.Add, ex => throw ex, () => emptyTakeCompleted++);
        Assert.Equal(0, emptyTake.Count);
        Assert.Equal(1, emptyTakeCompleted);

        var skipAll = new List<int>();
        Signal.Range(1, 3).Skip(10).Subscribe(skipAll.Add);
        Assert.Equal(0, skipAll.Count);

        var anyFalse = new List<bool>();
        var allFalse = new List<bool>();
        var containsFalse = new List<bool>();
        var longCount = new List<long>();
        Signal.FromEnumerable([1, 2, 3]).Any(value => value > 9).Subscribe(anyFalse.Add);
        Signal.FromEnumerable([2, 4, 5]).All(value => value % 2 == 0).Subscribe(allFalse.Add);
        Signal.FromEnumerable([2, 4, 6]).Contains(7).Subscribe(containsFalse.Add);
        Signal.FromEnumerable([1, 2, 3, 4]).LongCount(value => value % 2 == 0).Subscribe(longCount.Add);
        Assert.Equal(new[] { false }, anyFalse);
        Assert.Equal(new[] { false }, allFalse);
        Assert.Equal(new[] { false }, containsFalse);
        Assert.Equal(new[] { 2L }, longCount);

        var selectMany = new List<string>();
        Signal.FromEnumerable([1, 2])
            .SelectMany(value => Signal.FromEnumerable([value, value + 10]), (outer, inner) => outer + ":" + inner)
            .Subscribe(selectMany.Add);
        Assert.Equal(new[] { "1:1", "1:11", "2:2", "2:12" }, selectMany);
    }

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

        Signal.Throw<int>(new InvalidOperationException("spark"))
            .Sparkify()
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

        Signal.Throw<int>(new InvalidOperationException("recover"))
            .Rescue(error => Signal.Return(error.Message.Length))
            .Subscribe(rescueValues.Add);

        Signal.Throw<int>(new InvalidOperationException("resume"))
            .Resume(Signal.FromEnumerable([4, 5]))
            .Subscribe(resumeValues.Add);

        Signal.Defer(() => Signal.Throw<int>(new InvalidOperationException("stop")))
            .Retry(1)
            .Subscribe(_ => { }, ex => finalErrors.Add(ex.Message));

        Assert.Equal(new[] { SparkKind.OnError }, sparkKinds);
        Assert.Equal(new[] { "spark" }, sparkErrors);
        Assert.Equal(new[] { 1 }, unsparkValues);
        Assert.Equal(new[] { "unspark" }, unsparkErrors);
        Assert.Equal(new[] { 7 }, rescueValues);
        Assert.Equal(new[] { 4, 5 }, resumeValues);
        Assert.Equal(new[] { "stop" }, finalErrors);
    }

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

        outer.Concat().Subscribe(concatValues.Add, ex => throw ex, () => completed["concat"] = 1);
        outer.OnNext(first);
        outer.OnNext(second);
        first.OnNext(1);
        second.OnNext(20);
        first.OnNext(2);
        first.OnCompleted();
        second.OnNext(21);
        second.OnCompleted();
        outer.OnCompleted();

        Signal.Merge(Signal.FromEnumerable([1, 2]), Signal.FromEnumerable([3])).Subscribe(mergeValues.Add, ex => throw ex, () => completed["merge"] = 1);

        var raceLoser = new Signal<int>();
        var raceWinner = new Signal<int>();
        Signal.Race(raceLoser, raceWinner).Subscribe(raceValues.Add, ex => throw ex, () => completed["race"] = 1);
        raceWinner.OnNext(7);
        raceLoser.OnNext(99);
        raceWinner.OnCompleted();

        var switchOuter = new Signal<IObservable<int>>();
        var oldInner = new Signal<int>();
        var newInner = new Signal<int>();
        switchOuter.Switch().Subscribe(switchValues.Add, ex => throw ex, () => completed["switch"] = 1);
        switchOuter.OnNext(oldInner);
        oldInner.OnNext(1);
        switchOuter.OnNext(newInner);
        oldInner.OnNext(2);
        newInner.OnNext(3);
        switchOuter.OnCompleted();
        newInner.OnCompleted();

        var left = new Signal<int>();
        var right = new Signal<string>();
        left.WithLatest(right, (l, r) => l + r).Subscribe(withLatestValues.Add);
        left.OnNext(1);
        right.OnNext("a");
        left.OnNext(2);
        right.OnNext("b");
        left.OnNext(3);
        left.OnCompleted();

        Signal.FromEnumerable([1, 2, 3]).Zip(Signal.Return(10), (l, r) => l + r).Subscribe(zipShortValues.Add, ex => throw ex, () => completed["zip"] = 1);
        Signal.Empty<int>().ForkJoin(Signal.Return(1), (l, r) => l + r).Subscribe(forkJoinEmpty.Add, ex => throw ex, () => completed["forkJoinEmpty"] = 1);

        Assert.Equal(new[] { 1, 2, 21 }, concatValues);
        Assert.Equal([1, 2, 3], mergeValues.Order());
        Assert.Equal(new[] { 7 }, raceValues);
        Assert.Equal(new[] { 1, 3 }, switchValues);
        Assert.Equal(new[] { "2a", "3b" }, withLatestValues);
        Assert.Equal(new[] { 11 }, zipShortValues);
        Assert.Equal(0, forkJoinEmpty.Count);
        Assert.Equal(1, completed["concat"]);
        Assert.Equal(1, completed["merge"]);
        Assert.Equal(1, completed["race"]);
        Assert.Equal(1, completed["switch"]);
        Assert.Equal(1, completed["zip"]);
        Assert.Equal(1, completed["forkJoinEmpty"]);
    }

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
        manual.DelayStart(TimeSpan.FromTicks(5), clock).Subscribe(delayStartValues.Add);
        manual.OnNext(1);
        clock.AdvanceBy(TimeSpan.FromTicks(4));
        Assert.Equal(0, delayStartValues.Count);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        manual.OnNext(2);
        Assert.Equal(new[] { 2 }, delayStartValues);

        Signal.FromEnumerable([3, 4]).Delay(TimeSpan.FromTicks(3), clock).Subscribe(delayedValues.Add);
        clock.AdvanceBy(TimeSpan.FromTicks(2));
        Assert.Equal(0, delayedValues.Count);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        Assert.Equal(new[] { 3, 4 }, delayedValues);

        var never = new Signal<int>();
        never.Timeout(TimeSpan.FromTicks(4), clock).Subscribe(timeoutValues.Add, ex => timeoutErrors.Add(ex.GetType().Name));
        clock.AdvanceBy(TimeSpan.FromTicks(4));
        never.OnNext(42);
        Assert.Equal(0, timeoutValues.Count);
        Assert.Equal(new[] { nameof(TimeoutException) }, timeoutErrors);

        var completed = new Signal<int>();
        completed.Timeout(TimeSpan.FromTicks(10), clock).Subscribe(timeoutValues.Add);
        completed.OnNext(7);
        completed.OnCompleted();
        clock.AdvanceBy(TimeSpan.FromTicks(10));
        Assert.Equal(new[] { 7 }, timeoutValues);

        var pulse = Signal.Pulse(TimeSpan.FromTicks(2), clock).Subscribe(pulseValues.Add);
        clock.AdvanceBy(TimeSpan.FromTicks(6));
        pulse.Dispose();
        Assert.Equal(new[] { 0L, 1L, 2L }, pulseValues);

        var timer = Signal.Timer(TimeSpan.FromTicks(3), TimeSpan.FromTicks(2), clock).Subscribe(timerValues.Add);
        clock.AdvanceBy(TimeSpan.FromTicks(3));
        clock.AdvanceBy(TimeSpan.FromTicks(4));
        timer.Dispose();
        Assert.Equal(new[] { 0L, 1L, 2L }, timerValues);

        Signal.FromEnumerable([8, 9]).Timestamp(clock).Subscribe(timestamps.Add);
        Assert.Equal([8, 9], timestamps.Select(item => item.Value));
        Assert.True(timestamps.All(item => item.Timestamp == clock.Now));
    }

    [Test]
    public async Task FactoriesTasksAndTerminalTasksCoverCancellationFaultAndEmptyBranches()
    {
        var useErrors = new List<string>();
        var taskErrors = new List<string>();
        var asyncValues = new List<int>();
        var asyncErrors = new List<string>();

        Signal.Use(() => Disposable.Empty, _ => (IObservable<int>)null!).Subscribe(_ => { }, ex => useErrors.Add(ex.Message));
        Signal.Use<IDisposable, int>(() => throw new InvalidOperationException("resource"), _ => Signal.Return(1)).Subscribe(_ => { }, ex => useErrors.Add(ex.Message));

        await ObserveTaskError(Task.FromCanceled<int>(new CancellationToken(true)), taskErrors);
        await ObserveTaskError(Task.FromException<int>(new InvalidOperationException("faulted")), taskErrors);

        static async IAsyncEnumerable<int> ThrowingAsyncEnumerable()
        {
            yield return 1;
            await Task.Yield();
            throw new InvalidOperationException("async");
        }

        Signal.FromAsyncEnumerable(ThrowingAsyncEnumerable()).Subscribe(asyncValues.Add, ex => asyncErrors.Add(ex.Message));
        await SpinUntil(() => asyncErrors.Count == 1, TimeSpan.FromSeconds(2));

        var firstFailure = await AssertTaskFault<InvalidOperationException>(() => Signal.Empty<int>().FirstAsync());
        var collectFailure = await AssertTaskFault<InvalidOperationException>(() => Signal.Throw<int>(new InvalidOperationException("collect")).CollectArrayAsync());
        var listFailure = await AssertTaskFault<InvalidOperationException>(() => Signal.Throw<int>(new InvalidOperationException("list")).CollectListAsync());

        Assert.Equal(new[] { "The signal factory returned null.", "resource" }, useErrors);
        Assert.Equal(new[] { nameof(TaskCanceledException), nameof(InvalidOperationException) }, taskErrors);
        Assert.Equal(new[] { 1 }, asyncValues);
        Assert.Equal(new[] { "async" }, asyncErrors);
        Assert.Equal("The source completed without producing a value.", firstFailure.Message);
        Assert.Equal("collect", collectFailure.Message);
        Assert.Equal("list", listFailure.Message);
    }

    [Test]
    [RequiresUnreferencedCode("The test exercises reflection and dynamic-code coverage branches.")]
    [RequiresDynamicCode("The test exercises reflection and dynamic-code coverage branches.")]
    public void CoreValueTypesDisposablesAndHandlesCoverEqualityAndLifecycleBranches()
    {
        var moment = new Moment<int>(7, new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var sameMoment = new Moment<int>(7, moment.Timestamp);
        var differentMoment = new Moment<int>(8, moment.Timestamp.AddTicks(1));
        var interval = new TimeInterval<int>(7, TimeSpan.FromTicks(3));
        var sameInterval = new TimeInterval<int>(7, TimeSpan.FromTicks(3));
        var differentInterval = new TimeInterval<int>(8, TimeSpan.FromTicks(4));
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

        Assert.True(rxVoid == default);
        Assert.False(rxVoid != default);
        Assert.True(rxVoid.Equals(default));
        Assert.True(rxVoid.Equals((object)default(RxVoid)));
        Assert.Equal(0, rxVoid.GetHashCode());
        Assert.Equal("()", rxVoid.ToString());

        InvokeInternalHandleMembers(thrown);
        InvokeInternalCatchIgnore<int>(new InvalidOperationException("ignored")).Subscribe(_ => ignored++);
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
        Assert.Equal(2, slotDisposed);
        Assert.Equal(2, assignmentDisposed);
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

    [Test]
    public void SparksCoverValueErrorCompletionEqualityAndAcceptOverloads()
    {
        var next = Spark.CreateOnNext(42);
        var sameNext = Spark.CreateOnNext(42);
        var differentNext = Spark.CreateOnNext(43);
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
        Assert.False(next.Equals((Spark<int>?)null));
        Assert.False(next.Equals(completed));
        Assert.True(next.HasValue);
        Assert.Equal(42, next.Value);
        Assert.Equal(SparkKind.OnNext, next.Kind);
        Assert.True(next.ToString().Contains("42", StringComparison.Ordinal));
        Assert.Equal(next.GetHashCode(), sameNext.GetHashCode());
        next.Accept((IObserver<int>)observer);
        Assert.Equal("next:42", next.Accept<string>((IObserver<int, string>)observer));
        next.Accept(value => observer.Events.Add("delegate-next:" + value), ex => observer.Events.Add(ex.Message), () => observer.Events.Add("delegate-completed"));
        Assert.Equal("fn-next:42", next.Accept(value => "fn-next:" + value, ex => ex.Message, () => "fn-completed"));
        Assert.Throws<ArgumentNullException>(() => next.Accept((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => next.Accept((IObserver<int, string>)null!));
        Assert.Throws<ArgumentNullException>(() => next.Accept(null!, ex => { }, () => { }));
        Assert.Throws<ArgumentNullException>(() => next.Accept(value => { }, null!, () => { }));
        Assert.Throws<ArgumentNullException>(() => next.Accept(value => { }, ex => { }, null!));
        Assert.Throws<ArgumentNullException>(() => next.Accept<string>(null!, ex => ex.Message, () => "done"));
        Assert.Throws<ArgumentNullException>(() => next.Accept(value => value.ToString(), null!, () => "done"));
        Assert.Throws<ArgumentNullException>(() => next.Accept(value => value.ToString(), ex => ex.Message, null!));

        Assert.True(errorSpark == sameError);
        Assert.True(errorSpark != next);
        Assert.False(errorSpark.Equals((Spark<int>?)null));
        Assert.False(errorSpark.HasValue);
        Assert.Equal(error, errorSpark.Exception);
        Assert.Equal(SparkKind.OnError, errorSpark.Kind);
        Assert.Throws<InvalidOperationException>(() => _ = errorSpark.Value);
        Assert.True(errorSpark.ToString().Contains(nameof(InvalidOperationException), StringComparison.Ordinal));
        Assert.Equal(errorSpark.GetHashCode(), sameError.GetHashCode());
        errorSpark.Accept((IObserver<int>)observer);
        Assert.Equal("error:spark-error", errorSpark.Accept<string>((IObserver<int, string>)observer));
        errorSpark.Accept(value => observer.Events.Add(value.ToString()), ex => observer.Events.Add("delegate-error:" + ex.Message), () => observer.Events.Add("delegate-completed"));
        Assert.Equal("fn-error:spark-error", errorSpark.Accept(value => value.ToString(), ex => "fn-error:" + ex.Message, () => "fn-completed"));
        Assert.Throws<ArgumentNullException>(() => Spark.CreateOnError<int>(null!));
        Assert.Throws<ArgumentNullException>(() => errorSpark.Accept((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => errorSpark.Accept((IObserver<int, string>)null!));
        Assert.Throws<ArgumentNullException>(() => errorSpark.Accept(null!, ex => { }, () => { }));
        Assert.Throws<ArgumentNullException>(() => errorSpark.Accept(value => { }, null!, () => { }));
        Assert.Throws<ArgumentNullException>(() => errorSpark.Accept(value => { }, ex => { }, null!));
        Assert.Throws<ArgumentNullException>(() => errorSpark.Accept<string>(null!, ex => ex.Message, () => "done"));
        Assert.Throws<ArgumentNullException>(() => errorSpark.Accept(value => value.ToString(), null!, () => "done"));
        Assert.Throws<ArgumentNullException>(() => errorSpark.Accept(value => value.ToString(), ex => ex.Message, null!));

        Assert.Same(completed, completedAgain);
        Assert.True(completed.Equals(completedAgain));
        Assert.False(completed.Equals((Spark<int>?)null));
        Assert.False(completed.HasValue);
        Assert.Equal(SparkKind.OnCompleted, completed.Kind);
        Assert.Throws<InvalidOperationException>(() => _ = completed.Value);
        Assert.Equal("OnCompleted()", completed.ToString());
        completed.Accept((IObserver<int>)observer);
        Assert.Equal("completed", completed.Accept<string>((IObserver<int, string>)observer));
        completed.Accept(value => observer.Events.Add(value.ToString()), ex => observer.Events.Add(ex.Message), () => observer.Events.Add("delegate-completed"));
        Assert.Equal("fn-completed", completed.Accept(value => value.ToString(), ex => ex.Message, () => "fn-completed"));
        Assert.Throws<ArgumentNullException>(() => completed.Accept((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => completed.Accept((IObserver<int, string>)null!));
        Assert.Throws<ArgumentNullException>(() => completed.Accept(null!, ex => { }, () => { }));
        Assert.Throws<ArgumentNullException>(() => completed.Accept(value => { }, null!, () => { }));
        Assert.Throws<ArgumentNullException>(() => completed.Accept(value => { }, ex => { }, null!));
        Assert.Throws<ArgumentNullException>(() => completed.Accept<string>(null!, ex => ex.Message, () => "done"));
        Assert.Throws<ArgumentNullException>(() => completed.Accept(value => value.ToString(), null!, () => "done"));
        Assert.Throws<ArgumentNullException>(() => completed.Accept(value => value.ToString(), ex => ex.Message, null!));
        Assert.Throws<ArgumentNullException>(() => completed.ToObservable(null!));

        next.ToObservable().Subscribe(observableValues.Add, ex => throw ex, () => observableCompleted++);
        Assert.Equal(new[] { 42 }, observableValues);
        Assert.Equal(1, observableCompleted);
        Assert.Contains("next:42", observer.Events);
        Assert.Contains("error:spark-error", observer.Events);
        Assert.Contains("completed", observer.Events);
    }

    private static async Task ObserveTaskError(Task<int> task, List<string> errors)
    {
        Signal.FromTask(task).Subscribe(_ => { }, ex => errors.Add(ex.GetType().Name));
        await SpinUntil(() => errors.Count > 0, TimeSpan.FromSeconds(2));
    }

    [RequiresDynamicCode("Calls System.Type.MakeGenericType(params Type[])")]
    [RequiresUnreferencedCode("Calls System.Reflection.Assembly.GetType(String)")]
    private static void InvokeInternalHandleMembers(Exception exception)
    {
        var assembly = typeof(RxVoid).Assembly;
        InvokeAction(assembly.GetType("ReactiveUI.Primitives.Handle")!.GetField("Nop", BindingFlags.Public | BindingFlags.Static)!.GetValue(null)!);
        InvokeAction(assembly.GetType("ReactiveUI.Primitives.Handle`1")!.MakeGenericType(typeof(int)).GetField("Ignore", BindingFlags.Public | BindingFlags.Static)!.GetValue(null)!, 1);
        InvokeAction(assembly.GetType("ReactiveUI.Primitives.Handle`2")!.MakeGenericType(typeof(int), typeof(int)).GetField("Ignore", BindingFlags.Public | BindingFlags.Static)!.GetValue(null)!, 1, 2);
        InvokeAction(assembly.GetType("ReactiveUI.Primitives.Handle`3")!.MakeGenericType(typeof(int), typeof(int), typeof(int)).GetField("Ignore", BindingFlags.Public | BindingFlags.Static)!.GetValue(null)!, 1, 2, 3);

        var identity = (Delegate)assembly.GetType("ReactiveUI.Primitives.Handle`1")!.MakeGenericType(typeof(string)).GetField("Identity", BindingFlags.Public | BindingFlags.Static)!.GetValue(null)!;
        Assert.Equal("x", identity.DynamicInvoke("x"));

        InvokeThrows(assembly.GetType("ReactiveUI.Primitives.Handle")!.GetField("Throw", BindingFlags.Public | BindingFlags.Static)!.GetValue(null)!, exception);
        InvokeThrows(assembly.GetType("ReactiveUI.Primitives.Handle`1")!.MakeGenericType(typeof(int)).GetField("Throw", BindingFlags.Public | BindingFlags.Static)!.GetValue(null)!, exception, 1);
        InvokeThrows(assembly.GetType("ReactiveUI.Primitives.Handle`2")!.MakeGenericType(typeof(int), typeof(int)).GetField("Throw", BindingFlags.Public | BindingFlags.Static)!.GetValue(null)!, exception, 1, 2);
        InvokeThrows(assembly.GetType("ReactiveUI.Primitives.Handle`3")!.MakeGenericType(typeof(int), typeof(int), typeof(int)).GetField("Throw", BindingFlags.Public | BindingFlags.Static)!.GetValue(null)!, exception, 1, 2, 3);
    }

    [RequiresUnreferencedCode("Calls System.Reflection.Assembly.GetType(String)")]
    [RequiresDynamicCode("Calls System.Reflection.MethodInfo.MakeGenericMethod(params Type[])")]
    private static IObservable<T> InvokeInternalCatchIgnore<T>(Exception exception)
    {
        var handle = typeof(RxVoid).Assembly.GetType("ReactiveUI.Primitives.Handle")!;
        var method = handle.GetMethod("CatchIgnore", BindingFlags.Public | BindingFlags.Static)!.MakeGenericMethod(typeof(T));
        return (IObservable<T>)method.Invoke(null, [exception])!;
    }

    private static void InvokeAction(object action, params object[] args) => ((Delegate)action).DynamicInvoke(args);

    private static void InvokeThrows(object action, params object[] args)
    {
        try
        {
            ((Delegate)action).DynamicInvoke(args);
        }
        catch (TargetInvocationException ex) when (ex.InnerException is InvalidOperationException)
        {
            return;
        }

        throw new InvalidOperationException("Expected internal handle throw delegate to throw InvalidOperationException.");
    }

    private static async Task<TException> AssertTaskFault<TException>(Func<Task> taskFactory)
        where TException : Exception
    {
        try
        {
            await taskFactory();
        }
        catch (TException exception)
        {
            return exception;
        }

        throw new InvalidOperationException($"Expected task fault {typeof(TException).Name}.");
    }

    private static async Task SpinUntil(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        while (!condition())
        {
            if (DateTimeOffset.UtcNow >= deadline)
            {
                throw new TimeoutException("Condition was not reached before timeout.");
            }

            await Task.Delay(10);
        }
    }

    private sealed class RecordingResultObserver<T> : IObserver<T>, IObserver<T, string>
    {
        public List<string> Events { get; } = [];

        public void OnCompleted() => Events.Add("completed");

        public void OnError(Exception error) => Events.Add("error:" + error.Message);

        public void OnNext(T value) => Events.Add("next:" + value);

        string IObserver<T, string>.OnCompleted()
        {
            Events.Add("completed");
            return "completed";
        }

        string IObserver<T, string>.OnError(Exception exception)
        {
            Events.Add("error:" + exception.Message);
            return "error:" + exception.Message;
        }

        string IObserver<T, string>.OnNext(T value)
        {
            Events.Add("next:" + value);
            return "next:" + value;
        }
    }
}
