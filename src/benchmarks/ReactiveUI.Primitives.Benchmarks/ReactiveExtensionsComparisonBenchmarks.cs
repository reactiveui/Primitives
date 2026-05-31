// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#pragma warning disable CA2012, CS1591, RCS1047, RCS1196, RCS1208, RCS1222, RCS1238, S104, S109, S1128, S1226, S138, S3218, S3358, S4462, S5034, S881, SA1201, SA1600, SA1602, SA1611, SA1615, SA1618, SYSLIB1045

using System.ComponentModel;
using System.Globalization;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Text.RegularExpressions;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;
using PackageContinuation = ReactiveUI.Extensions.Continuation;
using PackageExtensions = ReactiveUI.Extensions.ReactiveExtensions;
using PackageObservables = ReactiveUI.Extensions.Observables;
using PackageObserverExtensions = ReactiveUI.Extensions.ObserverExtensions;
using PackageSubscriptionExtensions = ReactiveUI.Extensions.ObservableSubscriptionExtensions;
using PrimitivesContinuation = ReactiveUI.Primitives.Extensions.Continuation;
using PrimitivesExtensions = ReactiveUI.Primitives.Extensions.ReactiveExtensions;
using PrimitivesObservables = ReactiveUI.Primitives.Extensions.Observables;
using PrimitivesObserverExtensions = ReactiveUI.Primitives.Extensions.ObserverExtensions;
using PrimitivesSubscriptionExtensions = ReactiveUI.Primitives.Extensions.ObservableSubscriptionExtensions;
using RxObservable = System.Reactive.Linq.Observable;
using RxUnit = System.Reactive.Unit;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks the complete synchronous ReactiveUI.Primitives.Extensions public helper surface.
/// </summary>
[MemoryDiagnoser]
public class ReactiveExtensionsComparisonBenchmarks
{
    private const int Count = 32;
    private const int Fallback = 42;
    private const int Match = 8;
    private const int Value = 7;
    private static readonly TimeSpan Tick = TimeSpan.FromTicks(1);

    private static readonly Exception Boom = new InvalidOperationException("benchmark");
    private static readonly Regex EvenRegex = new("^[02468]$", RegexOptions.Compiled);
    private static readonly bool[] BooleanValues = [true, false, false, true, false, true, false, false];
    private static readonly char[] BufferCharacters = "xx[abc]yy[de]".ToCharArray();
    private static readonly int[] Values = CreateValues();
    private static readonly string?[] NullableStrings = ["one", null, "two", null, "three"];
    private static readonly string[] SkipStrings = ["one", null!, "two", null!, "three"];
    private static readonly string[] StringValues = ["0", "1", "2", "3", "4", "5"];

    private static readonly ExtensionScenario[] PrimitivesScenarioItems =
        CreateLibraryScenarios(ExtensionsLibrary.Primitives);

    private static readonly ExtensionScenario[] PackageScenarioItems =
        CreateLibraryScenarios(ExtensionsLibrary.ReactiveUIExtensions);

    private static readonly ExtensionScenario[] SystemReactiveScenarioItems =
    [
        Scenario("AsSignal", SystemReactiveAsSignal),
        Scenario("CatchAndReturn", SystemReactiveCatchAndReturn),
        Scenario("CatchIgnore", SystemReactiveCatchIgnore),
        Scenario("CatchReturn", SystemReactiveCatchAndReturn),
        Scenario("CombineLatestValuesAreAllFalse", SystemReactiveCombineLatestValuesAreAllFalse),
        Scenario("CombineLatestValuesAreAllTrue", SystemReactiveCombineLatestValuesAreAllTrue),
        Scenario("Filter", SystemReactiveFilter),
        Scenario("ForEach", SystemReactiveForEach),
        Scenario("FromArray", SystemReactiveFromArray),
        Scenario("GetMax", SystemReactiveGetMax),
        Scenario("GetMin", SystemReactiveGetMin),
        Scenario("Not", SystemReactiveNot),
        Scenario("Pairwise", SystemReactivePairwise),
        Scenario("Return", SystemReactiveReturn),
        Scenario("ScanWithInitial", SystemReactiveScanWithInitial),
        Scenario("SelectAsync", SystemReactiveSelectAsync),
        Scenario("SelectConstant", SystemReactiveSelectConstant),
        Scenario("SelectManyThen", SystemReactiveSelectManyThen),
        Scenario("SkipWhileNull", SystemReactiveSkipWhileNull),
        Scenario("TakeUntil", SystemReactiveTakeUntil),
        Scenario("ToHotTask", SystemReactiveToHotTask),
        Scenario("WaitUntil", SystemReactiveWaitUntil),
        Scenario("WhereFalse", SystemReactiveWhereFalse),
        Scenario("WhereIsNotNull", SystemReactiveWhereIsNotNull),
        Scenario("WhereSelect", SystemReactiveWhereSelect),
        Scenario("WhereTrue", SystemReactiveWhereTrue),
    ];

    private static readonly ExtensionScenario[] R3ScenarioItems =
    [
        Scenario("AsSignal", R3AsSignal),
        Scenario("CatchAndReturn", R3CatchAndReturn),
        Scenario("CatchIgnore", R3CatchIgnore),
        Scenario("CatchReturn", R3CatchAndReturn),
        Scenario("FromArray", R3FromArray),
        Scenario("Not", R3Not),
        Scenario("Return", R3Return),
        Scenario("SelectConstant", R3SelectConstant),
        Scenario("WhereFalse", R3WhereFalse),
        Scenario("WhereIsNotNull", R3WhereIsNotNull),
        Scenario("WhereSelect", R3WhereSelect),
        Scenario("WhereTrue", R3WhereTrue),
    ];

    public IEnumerable<ExtensionScenario> PrimitivesScenarios => PrimitivesScenarioItems;

    public IEnumerable<ExtensionScenario> ReactiveUIExtensionsScenarios => PackageScenarioItems;

    public IEnumerable<ExtensionScenario> SystemReactiveScenarios => SystemReactiveScenarioItems;

    public IEnumerable<ExtensionScenario> R3Scenarios => R3ScenarioItems;

    [Benchmark]
    [ArgumentsSource(nameof(PrimitivesScenarios))]
    public int Primitives(ExtensionScenario scenario) => scenario.Run();

    [Benchmark]
    [ArgumentsSource(nameof(ReactiveUIExtensionsScenarios))]
    public int ReactiveUIExtensions(ExtensionScenario scenario) => scenario.Run();

    [Benchmark]
    [ArgumentsSource(nameof(SystemReactiveScenarios))]
    public int SystemReactive(ExtensionScenario scenario) => scenario.Run();

    [Benchmark]
    [ArgumentsSource(nameof(R3Scenarios))]
    public int R3Library(ExtensionScenario scenario) => scenario.Run();

    private static ExtensionScenario[] CreateLibraryScenarios(ExtensionsLibrary library) =>
    [
        Scenario("AsSignal", () => RunAsSignal(library)),
        Scenario("BufferUntil", () => RunBufferUntil(library)),
        Scenario("BufferUntilIdle", () => RunBufferUntilIdle(library)),
        Scenario("BufferUntilInactive", () => RunBufferUntilInactive(library)),
        Scenario("CatchAndReturn", () => RunCatchAndReturn(library)),
        Scenario("CatchIgnore", () => RunCatchIgnore(library)),
        Scenario("CatchReturn", () => RunCatchReturn(library)),
        Scenario("CatchReturnUnit", () => RunCatchReturnUnit(library)),
        Scenario("CombineLatestValuesAreAllFalse", () => RunCombineLatestValuesAreAllFalse(library)),
        Scenario("CombineLatestValuesAreAllTrue", () => RunCombineLatestValuesAreAllTrue(library)),
        Scenario("Conflate", () => RunConflate(library)),
        Scenario("Continuation.Dispose", () => RunContinuationDispose(library)),
        Scenario("Continuation.Lock", () => RunContinuationLock(library)),
        Scenario("Continuation.LockValueTask", () => RunContinuationLockValueTask(library)),
        Scenario("DebounceImmediate", () => RunDebounceImmediate(library)),
        Scenario("DebounceUntil", () => RunDebounceUntil(library)),
        Scenario("DetectStale", () => RunDetectStale(library)),
        Scenario("DoOnDispose", () => RunDoOnDispose(library)),
        Scenario("DoOnSubscribe", () => RunDoOnSubscribe(library)),
        Scenario("DropIfBusy", () => RunDropIfBusy(library)),
        Scenario("FastForEach", () => RunFastForEach(library)),
        Scenario("Filter", () => RunFilter(library)),
        Scenario("FirstMatchFromCandidates", () => RunFirstMatchFromCandidates(library)),
        Scenario("ForEach", () => RunForEach(library)),
        Scenario("FromArray", () => RunFromArray(library)),
        Scenario("GetMax", () => RunGetMax(library)),
        Scenario("GetMin", () => RunGetMin(library)),
        Scenario("Heartbeat", () => RunHeartbeat(library)),
        Scenario("LatestOrDefault", () => RunLatestOrDefault(library)),
        Scenario("LogErrors", () => RunLogErrors(library)),
        Scenario("Not", () => RunNot(library)),
        Scenario("ObserveOnIf", () => RunObserveOnIf(library)),
        Scenario("ObserveOnSafe", () => RunObserveOnSafe(library)),
        Scenario("OnErrorRetry", () => RunOnErrorRetry(library)),
        Scenario("OnNext", () => RunOnNext(library)),
        Scenario("Pairwise", () => RunPairwise(library)),
        Scenario("Partition", () => RunPartition(library)),
        Scenario("ReplayLastOnSubscribe", () => RunReplayLastOnSubscribe(library)),
        Scenario("RetryForeverWithDelay", () => RunRetryForeverWithDelay(library)),
        Scenario("RetryWithBackoff", () => RunRetryWithBackoff(library)),
        Scenario("RetryWithDelay", () => RunRetryWithDelay(library)),
        Scenario("RetryWithFixedDelay", () => RunRetryWithFixedDelay(library)),
        Scenario("Return", () => RunReturn(library)),
        Scenario("RunAll", () => RunRunAll(library)),
        Scenario("SampleLatest", () => RunSampleLatest(library)),
        Scenario("ScanWithInitial", () => RunScanWithInitial(library)),
        Scenario("Schedule", () => RunSchedule(library)),
        Scenario("ScheduleSafe", () => RunScheduleSafe(library)),
        Scenario("SelectAsync", () => RunSelectAsync(library)),
        Scenario("SelectAsyncConcurrent", () => RunSelectAsyncConcurrent(library)),
        Scenario("SelectAsyncSequential", () => RunSelectAsyncSequential(library)),
        Scenario("SelectConstant", () => RunSelectConstant(library)),
        Scenario("SelectLatestAsync", () => RunSelectLatestAsync(library)),
        Scenario("SelectManyThen", () => RunSelectManyThen(library)),
        Scenario("Shuffle", () => RunShuffle(library)),
        Scenario("SkipWhileNull", () => RunSkipWhileNull(library)),
        Scenario("Start", () => RunStart(library)),
        Scenario("SubscribeAndComplete", () => RunSubscribeAndComplete(library)),
        Scenario("SubscribeAsync", () => RunSubscribeAsync(library)),
        Scenario("SubscribeGetError", () => RunSubscribeGetError(library)),
        Scenario("SubscribeGetValue", () => RunSubscribeGetValue(library)),
        Scenario("SubscribeSynchronous", () => RunSubscribeSynchronous(library)),
        Scenario("SwitchIfEmpty", () => RunSwitchIfEmpty(library)),
        Scenario("SyncTimer", () => RunSyncTimer(library)),
        Scenario("SynchronizeAsync", () => RunSynchronizeAsync(library)),
        Scenario("SynchronizeSynchronous", () => RunSynchronizeSynchronous(library)),
        Scenario("TakeUntil", () => RunTakeUntil(library)),
        Scenario("ThrottleDistinct", () => RunThrottleDistinct(library)),
        Scenario("ThrottleFirst", () => RunThrottleFirst(library)),
        Scenario("ThrottleOnScheduler", () => RunThrottleOnScheduler(library)),
        Scenario("ThrottleUntilTrue", () => RunThrottleUntilTrue(library)),
        Scenario("ToHotTask", () => RunToHotTask(library)),
        Scenario("ToHotValueTask", () => RunToHotValueTask(library)),
        Scenario("ToPropertyObservable", () => RunToPropertyObservable(library)),
        Scenario("ToReadOnlyBehavior", () => RunToReadOnlyBehavior(library)),
        Scenario("TrySelect", () => RunTrySelect(library)),
        Scenario("Using", () => RunUsing(library)),
        Scenario("WaitForCompletion", () => RunWaitForCompletion(library)),
        Scenario("WaitForError", () => RunWaitForError(library)),
        Scenario("WaitForValue", () => RunWaitForValue(library)),
        Scenario("WaitUntil", () => RunWaitUntil(library)),
        Scenario("WhereFalse", () => RunWhereFalse(library)),
        Scenario("WhereIsNotNull", () => RunWhereIsNotNull(library)),
        Scenario("WhereSelect", () => RunWhereSelect(library)),
        Scenario("WhereTrue", () => RunWhereTrue(library)),
        Scenario("While", () => RunWhile(library)),
        Scenario("WithLimitedConcurrency", () => RunWithLimitedConcurrency(library)),
    ];

    private static ExtensionScenario Scenario(string name, Func<int> run) => new(name, run);

    private static int RunAsSignal(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainPrimitiveUnit(PrimitivesExtensions.AsSignal(Range(library)))
            : DrainPackageUnit(PackageExtensions.AsSignal(Range(library)));

    private static int RunBufferUntil(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainString(PrimitivesExtensions.BufferUntil(PrimitivesExtensions.FromArray(BufferCharacters), '[', ']'))
            : DrainString(PackageExtensions.BufferUntil(PackageExtensions.FromArray(BufferCharacters), '[', ']'));

    private static int RunBufferUntilIdle(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainList(PrimitivesExtensions.BufferUntilIdle(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate))
            : DrainList(PackageExtensions.BufferUntilIdle(ArraySource(library), TimeSpan.Zero, ImmediateScheduler.Instance));

    private static int RunBufferUntilInactive(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainList(PrimitivesExtensions.BufferUntilInactive(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate))
            : DrainList(PackageExtensions.BufferUntilInactive(ArraySource(library), TimeSpan.Zero, ImmediateScheduler.Instance));

    private static int RunCatchAndReturn(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.CatchAndReturn<int, InvalidOperationException>(ThrowInt(library), static _ => Fallback)
            : PackageExtensions.CatchAndReturn<int, InvalidOperationException>(ThrowInt(library), static _ => Fallback));

    private static int RunCatchIgnore(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.CatchIgnore<int, InvalidOperationException>(ThrowInt(library), static _ => { })
            : PackageExtensions.CatchIgnore<int, InvalidOperationException>(ThrowInt(library), static _ => { }));

    private static int RunCatchReturn(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.CatchReturn(ThrowInt(library), Fallback)
            : PackageExtensions.CatchReturn(ThrowInt(library), Fallback));

    private static int RunCatchReturnUnit(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainPrimitiveUnit(PrimitivesExtensions.CatchReturnUnit(ThrowPrimitiveUnit()))
            : DrainPackageUnit(PackageExtensions.CatchReturnUnit(ThrowPackageUnit()));

    private static int RunCombineLatestValuesAreAllFalse(ExtensionsLibrary library) =>
        DrainBool(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.CombineLatestValuesAreAllFalse(BoolSources(library, false))
            : PackageExtensions.CombineLatestValuesAreAllFalse(BoolSources(library, false)));

    private static int RunCombineLatestValuesAreAllTrue(ExtensionsLibrary library) =>
        DrainBool(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.CombineLatestValuesAreAllTrue(BoolSources(library, true))
            : PackageExtensions.CombineLatestValuesAreAllTrue(BoolSources(library, true)));

    private static int RunConflate(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.Conflate(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate)
            : PackageExtensions.Conflate(ArraySource(library), TimeSpan.Zero, ImmediateScheduler.Instance));

    private static int RunContinuationDispose(ExtensionsLibrary library)
    {
        if (library == ExtensionsLibrary.Primitives)
        {
            using var continuation = new PrimitivesContinuation();
            return (int)continuation.CompletedPhases;
        }

        using var packageContinuation = new PackageContinuation();
        return (int)packageContinuation.CompletedPhases;
    }

    private static int RunContinuationLock(ExtensionsLibrary library)
    {
        var observer = new TupleObserver<int>();
        if (library == ExtensionsLibrary.Primitives)
        {
            var continuation = new PrimitivesContinuation();
            var task = continuation.Lock(Value, observer);
            task.GetAwaiter().GetResult();
            return observer.Count;
        }

        var packageContinuation = new PackageContinuation();
        var packageTask = packageContinuation.Lock(Value, observer);
        packageTask.GetAwaiter().GetResult();
        return observer.Count;
    }

    private static int RunContinuationLockValueTask(ExtensionsLibrary library)
    {
        var observer = new TupleObserver<int>();
        if (library == ExtensionsLibrary.Primitives)
        {
            var continuation = new PrimitivesContinuation();
            var task = continuation.LockValueTask(Value, observer);
            task.GetAwaiter().GetResult();
            return observer.Count;
        }

        var packageContinuation = new PackageContinuation();
        var packageTask = packageContinuation.LockValueTask(Value, observer);
        packageTask.GetAwaiter().GetResult();
        return observer.Count;
    }

    private static int RunDebounceImmediate(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.DebounceImmediate(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate)
            : PackageExtensions.DebounceImmediate(ArraySource(library), TimeSpan.Zero, ImmediateScheduler.Instance));

    private static int RunDebounceUntil(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.DebounceUntil(ArraySource(library), TimeSpan.Zero, static value => value >= Match, Sequencer.Immediate)
            : PackageExtensions.DebounceUntil(ArraySource(library), TimeSpan.Zero, static value => value >= Match, ImmediateScheduler.Instance));

    private static int RunDetectStale(ExtensionsLibrary library)
    {
        if (library == ExtensionsLibrary.Primitives)
        {
            var clock = new TestClock();
            var observer = new CountingSignalObserver<ReactiveUI.Primitives.Extensions.Stale<int>>();
            using var subscription = PrimitivesExtensions.DetectStale(Signal.Silent<int>(), Tick, clock).Subscribe(observer);
            clock.AdvanceBy(Tick);
            return observer.Count + observer.CompletionCount;
        }

        var scheduler = new HistoricalScheduler();
        var packageObserver = new CountingSignalObserver<ReactiveUI.Extensions.Stale<int>>();
        using var packageSubscription = PackageExtensions.DetectStale(RxObservable.Never<int>(), Tick, scheduler)
            .Subscribe(packageObserver);
        scheduler.AdvanceBy(Tick);
        return packageObserver.Count + packageObserver.CompletionCount;
    }

    private static int RunDoOnDispose(ExtensionsLibrary library)
    {
        var count = 0;
        using var subscription = (library == ExtensionsLibrary.Primitives
                ? PrimitivesExtensions.DoOnDispose(ArraySource(library), () => count++)
                : PackageExtensions.DoOnDispose(ArraySource(library), () => count++))
            .Subscribe(new IntSignalObserver());
        return count;
    }

    private static int RunDoOnSubscribe(ExtensionsLibrary library)
    {
        var count = 0;
        var total = DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.DoOnSubscribe(ArraySource(library), () => count++)
            : PackageExtensions.DoOnSubscribe(ArraySource(library), () => count++));
        return total + count;
    }

    private static int RunDropIfBusy(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.DropIfBusy(ArraySource(library), static _ => default)
            : PackageExtensions.DropIfBusy(ArraySource(library), static _ => default));

    private static int RunFastForEach(ExtensionsLibrary library)
    {
        var observer = new IntSignalObserver();
        if (library == ExtensionsLibrary.Primitives)
        {
            PrimitivesObserverExtensions.FastForEach(observer, Values);
        }
        else
        {
            PackageObserverExtensions.FastForEach(observer, Values);
        }

        return observer.Total;
    }

    private static int RunFilter(ExtensionsLibrary library) =>
        DrainString(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.Filter(PrimitivesExtensions.FromArray(StringValues), EvenRegex)
            : PackageExtensions.Filter(PackageExtensions.FromArray(StringValues), EvenRegex));

    private static int RunFirstMatchFromCandidates(ExtensionsLibrary library)
    {
        var candidates = Values;
        return DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.FirstMatchFromCandidates(
                candidates,
                static value => PrimitivesObservables.Return(value),
                static value => value * 2,
                static value => value >= Match,
                Fallback)
            : PackageExtensions.FirstMatchFromCandidates(
                candidates,
                static value => PackageObservables.Return(value),
                static value => value * 2,
                static value => value >= Match,
                Fallback));
    }

    private static int RunForEach(ExtensionsLibrary library)
    {
        var batches = new[] { Values };
        return DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ForEach(PrimitivesExtensions.FromArray<IEnumerable<int>>(batches), null)
            : PackageExtensions.ForEach(PackageExtensions.FromArray<IEnumerable<int>>(batches), null));
    }

    private static int RunFromArray(ExtensionsLibrary library) => DrainInt(ArraySource(library));

    private static int RunGetMax(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.GetMax(PrimitivesObservables.Return(1), PrimitivesObservables.Return(2))
            : PackageExtensions.GetMax(PackageObservables.Return(1), PackageObservables.Return(2)));

    private static int RunGetMin(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.GetMin(PrimitivesObservables.Return(1), PrimitivesObservables.Return(2))
            : PackageExtensions.GetMin(PackageObservables.Return(1), PackageObservables.Return(2)));

    private static int RunHeartbeat(ExtensionsLibrary library)
    {
        if (library == ExtensionsLibrary.Primitives)
        {
            var clock = new TestClock();
            var observer = new CountingSignalObserver<ReactiveUI.Primitives.Extensions.Heartbeat<int>>();
            using var subscription = PrimitivesExtensions.Heartbeat(Signal.Silent<int>(), Tick, clock).Subscribe(observer);
            clock.AdvanceBy(Tick);
            return observer.Count + observer.CompletionCount;
        }

        var scheduler = new HistoricalScheduler();
        var packageObserver = new CountingSignalObserver<ReactiveUI.Extensions.Heartbeat<int>>();
        using var packageSubscription = PackageExtensions.Heartbeat(RxObservable.Never<int>(), Tick, scheduler)
            .Subscribe(packageObserver);
        scheduler.AdvanceBy(Tick);
        return packageObserver.Count + packageObserver.CompletionCount;
    }

    private static int RunLatestOrDefault(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.LatestOrDefault(ArraySource(library), Fallback)
            : PackageExtensions.LatestOrDefault(ArraySource(library), Fallback));

    private static int RunLogErrors(ExtensionsLibrary library)
    {
        var errors = 0;
        var total = DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.LogErrors(ArraySource(library), _ => errors++)
            : PackageExtensions.LogErrors(ArraySource(library), _ => errors++));
        return total + errors;
    }

    private static int RunNot(ExtensionsLibrary library) =>
        DrainBool(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.Not(BoolSource(library))
            : PackageExtensions.Not(BoolSource(library)));

    private static int RunObserveOnIf(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ObserveOnIf(ArraySource(library), true, Sequencer.Immediate)
            : PackageExtensions.ObserveOnIf(ArraySource(library), true, ImmediateScheduler.Instance));

    private static int RunObserveOnSafe(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ObserveOnSafe(ArraySource(library), Sequencer.Immediate)
            : PackageExtensions.ObserveOnSafe(ArraySource(library), ImmediateScheduler.Instance));

    private static int RunOnErrorRetry(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.OnErrorRetry<int, InvalidOperationException>(ArraySource(library), static _ => { }, 1, TimeSpan.Zero, Sequencer.Immediate)
            : PackageExtensions.OnErrorRetry<int, InvalidOperationException>(ArraySource(library), static _ => { }, 1, TimeSpan.Zero, ImmediateScheduler.Instance));

    private static int RunOnNext(ExtensionsLibrary library)
    {
        var observer = new IntSignalObserver();
        if (library == ExtensionsLibrary.Primitives)
        {
            PrimitivesExtensions.OnNext(observer, Values);
        }
        else
        {
            PackageExtensions.OnNext(observer, Values);
        }

        return observer.Total;
    }

    private static int RunPairwise(ExtensionsLibrary library)
    {
        var observer = new PairObserver();
        using var subscription = (library == ExtensionsLibrary.Primitives
                ? PrimitivesExtensions.Pairwise(ArraySource(library))
                : PackageExtensions.Pairwise(ArraySource(library)))
            .Subscribe(observer);
        return observer.Total;
    }

    private static int RunPartition(ExtensionsLibrary library)
    {
        var observer = new IntSignalObserver();
        if (library == ExtensionsLibrary.Primitives)
        {
            var (even, odd) = PrimitivesExtensions.Partition(ArraySource(library), static value => (value & 1) == 0);
            using var evenSubscription = even.Subscribe(observer);
            using var oddSubscription = odd.Subscribe(observer);
        }
        else
        {
            var (even, odd) = PackageExtensions.Partition(ArraySource(library), static value => (value & 1) == 0);
            using var evenSubscription = even.Subscribe(observer);
            using var oddSubscription = odd.Subscribe(observer);
        }

        return observer.Total;
    }

    private static int RunReplayLastOnSubscribe(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ReplayLastOnSubscribe(ArraySource(library), Fallback)
            : PackageExtensions.ReplayLastOnSubscribe(ArraySource(library), Fallback));

    private static int RunRetryForeverWithDelay(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.RetryForeverWithDelay(ArraySource(library), TimeSpan.Zero)
            : PackageExtensions.RetryForeverWithDelay(ArraySource(library), TimeSpan.Zero));

    private static int RunRetryWithBackoff(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.RetryWithBackoff(ArraySource(library), 1, TimeSpan.Zero, 1.0, TimeSpan.Zero, Sequencer.Immediate)
            : PackageExtensions.RetryWithBackoff(ArraySource(library), 1, TimeSpan.Zero, 1.0, TimeSpan.Zero, ImmediateScheduler.Instance));

    private static int RunRetryWithDelay(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.RetryWithDelay(ArraySource(library), 1, static _ => TimeSpan.Zero)
            : PackageExtensions.RetryWithDelay(ArraySource(library), 1, static _ => TimeSpan.Zero));

    private static int RunRetryWithFixedDelay(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.RetryWithFixedDelay(ArraySource(library), 1, TimeSpan.Zero)
            : PackageExtensions.RetryWithFixedDelay(ArraySource(library), 1, TimeSpan.Zero));

    private static int RunReturn(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesObservables.Return(Value)
            : PackageObservables.Return(Value));

    private static int RunRunAll(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainPrimitiveUnit(PrimitivesExtensions.RunAll([PrimitivesObservables.Return(RxVoid.Default)]))
            : DrainPackageUnit(PackageExtensions.RunAll([PackageObservables.Return(RxUnit.Default)]));

    private static int RunSampleLatest(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SampleLatest(ArraySource(library), PrimitivesExtensions.SelectConstant(ArraySource(library), new object()))
            : PackageExtensions.SampleLatest(ArraySource(library), PackageExtensions.SelectConstant(ArraySource(library), new object())));

    private static int RunScanWithInitial(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ScanWithInitial(ArraySource(library), 0, static (acc, value) => acc + value)
            : PackageExtensions.ScanWithInitial(ArraySource(library), 0, static (acc, value) => acc + value));

    private static int RunSchedule(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.Schedule(Value, Sequencer.Immediate, static value => value + 1)
            : PackageExtensions.Schedule(Value, ImmediateScheduler.Instance, static value => value + 1));

    private static int RunScheduleSafe(ExtensionsLibrary library)
    {
        var count = 0;
        using var scheduled = library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ScheduleSafe(Sequencer.Immediate, () => count++)
            : PackageExtensions.ScheduleSafe(ImmediateScheduler.Instance, () => count++);
        return count;
    }

    private static int RunSelectAsync(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SelectAsync(ArraySource(library), static value => Task.FromResult(value + 1))
            : PackageExtensions.SelectAsync(ArraySource(library), static value => Task.FromResult(value + 1)));

    private static int RunSelectAsyncConcurrent(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SelectAsyncConcurrent(ArraySource(library), static value => Task.FromResult(value + 1), 4)
            : PackageExtensions.SelectAsyncConcurrent(ArraySource(library), static value => Task.FromResult(value + 1), 4));

    private static int RunSelectAsyncSequential(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SelectAsyncSequential(ArraySource(library), static value => Task.FromResult(value + 1))
            : PackageExtensions.SelectAsyncSequential(ArraySource(library), static value => Task.FromResult(value + 1)));

    private static int RunSelectConstant(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SelectConstant(ArraySource(library), Value)
            : PackageExtensions.SelectConstant(ArraySource(library), Value));

    private static int RunSelectLatestAsync(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SelectLatestAsync(ArraySource(library), static value => Task.FromResult(value + 1))
            : PackageExtensions.SelectLatestAsync(ArraySource(library), static value => Task.FromResult(value + 1)));

    private static int RunSelectManyThen(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SelectManyThen(
                PrimitivesObservables.Return(Value),
                static value => PrimitivesObservables.Return(value + 1),
                static value => PrimitivesObservables.Return(value + 1))
            : PackageExtensions.SelectManyThen(
                PackageObservables.Return(Value),
                static value => PackageObservables.Return(value + 1),
                static value => PackageObservables.Return(value + 1)));

    private static int RunShuffle(ExtensionsLibrary library) =>
        DrainArray(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.Shuffle(PrimitivesObservables.Return(Values))
            : PackageExtensions.Shuffle(PackageObservables.Return(Values)));

    private static int RunSkipWhileNull(ExtensionsLibrary library) =>
        DrainString(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SkipWhileNull(PrimitivesExtensions.FromArray(SkipStrings))
            : PackageExtensions.SkipWhileNull(PackageExtensions.FromArray(SkipStrings)));

    private static int RunStart(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainPrimitiveUnit(PrimitivesExtensions.Start(static () => { }, Sequencer.Immediate))
            : DrainPackageUnit(PackageExtensions.Start(static () => { }, ImmediateScheduler.Instance));

    private static int RunSubscribeAndComplete(ExtensionsLibrary library)
    {
        if (library == ExtensionsLibrary.Primitives)
        {
            PrimitivesSubscriptionExtensions.SubscribeAndComplete(PrimitivesObservables.Return(RxVoid.Default));
        }
        else
        {
            PackageSubscriptionExtensions.SubscribeAndComplete(PackageObservables.Return(RxUnit.Default));
        }

        return 1;
    }

    private static int RunSubscribeAsync(ExtensionsLibrary library)
    {
        var total = 0;
        using var subscription = library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SubscribeAsync(ArraySource(library), value =>
            {
                total += value;
                return default;
            })
            : PackageExtensions.SubscribeAsync(ArraySource(library), value =>
            {
                total += value;
                return default;
            });
        return total;
    }

    private static int RunSubscribeGetError(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? PrimitivesSubscriptionExtensions.SubscribeGetError(ThrowInt(library)) is null ? 0 : 1
            : PackageSubscriptionExtensions.SubscribeGetError(ThrowInt(library)) is null ? 0 : 1;

    private static int RunSubscribeGetValue(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? PrimitivesSubscriptionExtensions.SubscribeGetValue(ArraySource(library))
            : PackageSubscriptionExtensions.SubscribeGetValue(ArraySource(library));

    private static int RunSubscribeSynchronous(ExtensionsLibrary library)
    {
        var total = 0;
        using var subscription = library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SubscribeSynchronous(ArraySource(library), value =>
            {
                total += value;
                return default;
            })
            : PackageExtensions.SubscribeSynchronous(ArraySource(library), value =>
            {
                total += value;
                return default;
            });
        return total;
    }

    private static int RunSwitchIfEmpty(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SwitchIfEmpty(Signal.None<int>(), PrimitivesObservables.Return(Value))
            : PackageExtensions.SwitchIfEmpty(RxObservable.Empty<int>(), PackageObservables.Return(Value)));

    private static int RunSyncTimer(ExtensionsLibrary library)
    {
        if (library == ExtensionsLibrary.Primitives)
        {
            var clock = new TestClock();
            var observer = new CountingSignalObserver<DateTime>();
            using var subscription = PrimitivesExtensions.SyncTimer(Tick, clock).Subscribe(observer);
            clock.AdvanceBy(Tick);
            return observer.Count + observer.CompletionCount;
        }

        var scheduler = new HistoricalScheduler();
        var packageObserver = new CountingSignalObserver<DateTime>();
        using var packageSubscription = PackageExtensions.SyncTimer(Tick, scheduler).Subscribe(packageObserver);
        scheduler.AdvanceBy(Tick);
        return packageObserver.Count + packageObserver.CompletionCount;
    }

    private static int RunSynchronizeAsync(ExtensionsLibrary library) =>
        DrainSyncTuple(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SynchronizeAsync(ArraySource(library))
            : PackageExtensions.SynchronizeAsync(ArraySource(library)));

    private static int RunSynchronizeSynchronous(ExtensionsLibrary library) =>
        DrainSyncTuple(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SynchronizeSynchronous(ArraySource(library))
            : PackageExtensions.SynchronizeSynchronous(ArraySource(library)));

    private static int RunTakeUntil(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.TakeUntil(ArraySource(library), static value => value == Match)
            : PackageExtensions.TakeUntil(ArraySource(library), static value => value == Match));

    private static int RunThrottleDistinct(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ThrottleDistinct(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate)
            : PackageExtensions.ThrottleDistinct(ArraySource(library), TimeSpan.Zero, ImmediateScheduler.Instance));

    private static int RunThrottleFirst(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ThrottleFirst(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate)
            : PackageExtensions.ThrottleFirst(ArraySource(library), TimeSpan.Zero, ImmediateScheduler.Instance));

    private static int RunThrottleOnScheduler(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ThrottleOnScheduler(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate)
            : PackageExtensions.ThrottleOnScheduler(ArraySource(library), TimeSpan.Zero, ImmediateScheduler.Instance));

    private static int RunThrottleUntilTrue(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ThrottleUntilTrue(ArraySource(library), TimeSpan.Zero, static value => value >= Match)
            : PackageExtensions.ThrottleUntilTrue(ArraySource(library), TimeSpan.Zero, static value => value >= Match));

    private static int RunToHotTask(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ToHotTask(PrimitivesObservables.Return(Value)).GetAwaiter().GetResult()
            : PackageExtensions.ToHotTask(PackageObservables.Return(Value)).GetAwaiter().GetResult();

    private static int RunToHotValueTask(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ToHotValueTask(PrimitivesObservables.Return(Value)).GetAwaiter().GetResult()
            : PackageExtensions.ToHotValueTask(PackageObservables.Return(Value)).GetAwaiter().GetResult();

    private static int RunToPropertyObservable(ExtensionsLibrary library)
    {
        var source = new PropertySource();
        var observer = new IntSignalObserver();
        using var subscription = (library == ExtensionsLibrary.Primitives
                ? PrimitivesExtensions.ToPropertyObservable(source, static item => item.Value)
                : PackageExtensions.ToPropertyObservable(source, static item => item.Value))
            .Subscribe(observer);
        source.Value = Value;
        return observer.Total;
    }

    private static int RunToReadOnlyBehavior(ExtensionsLibrary library)
    {
        var observer = new IntSignalObserver();
        if (library == ExtensionsLibrary.Primitives)
        {
            var behavior = PrimitivesExtensions.ToReadOnlyBehavior(Value);
            using var subscription = behavior.Observable.Subscribe(observer);
            behavior.Observer.OnNext(Value + 1);
        }
        else
        {
            var behavior = PackageExtensions.ToReadOnlyBehavior(Value);
            using var subscription = behavior.Observable.Subscribe(observer);
            behavior.Observer.OnNext(Value + 1);
        }

        return observer.Total;
    }

    private static int RunTrySelect(ExtensionsLibrary library) =>
        DrainString(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.TrySelect<int, string>(ArraySource(library), static value => value % 2 == 0 ? value.ToString(CultureInfo.InvariantCulture) : null)
            : PackageExtensions.TrySelect<int, string>(ArraySource(library), static value => value % 2 == 0 ? value.ToString(CultureInfo.InvariantCulture) : null));

    private static int RunUsing(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainPrimitiveUnit(PrimitivesExtensions.Using(new DummyResource(), static resource => resource.Touch()))
            : DrainPackageUnit(PackageExtensions.Using(new DummyResource(), static resource => resource.Touch()));

    private static int RunWaitForCompletion(ExtensionsLibrary library)
    {
        if (library == ExtensionsLibrary.Primitives)
        {
            PrimitivesSubscriptionExtensions.WaitForCompletion(PrimitivesObservables.Return(RxVoid.Default), TimeSpan.FromSeconds(1));
        }
        else
        {
            PackageSubscriptionExtensions.WaitForCompletion(PackageObservables.Return(RxUnit.Default), TimeSpan.FromSeconds(1));
        }

        return 1;
    }

    private static int RunWaitForError(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? PrimitivesSubscriptionExtensions.WaitForError(ThrowInt(library), TimeSpan.FromSeconds(1)) is null ? 0 : 1
            : PackageSubscriptionExtensions.WaitForError(ThrowInt(library), TimeSpan.FromSeconds(1)) is null ? 0 : 1;

    private static int RunWaitForValue(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? PrimitivesSubscriptionExtensions.WaitForValue(ArraySource(library), TimeSpan.FromSeconds(1))
            : PackageSubscriptionExtensions.WaitForValue(ArraySource(library), TimeSpan.FromSeconds(1));

    private static int RunWaitUntil(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.WaitUntil(ArraySource(library), static value => value == Match)
            : PackageExtensions.WaitUntil(ArraySource(library), static value => value == Match));

    private static int RunWhereFalse(ExtensionsLibrary library) =>
        DrainBool(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.WhereFalse(BoolSource(library))
            : PackageExtensions.WhereFalse(BoolSource(library)));

    private static int RunWhereIsNotNull(ExtensionsLibrary library) =>
        DrainString(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.WhereIsNotNull(PrimitivesExtensions.FromArray(NullableStrings))
            : PackageExtensions.WhereIsNotNull(PackageExtensions.FromArray(NullableStrings)));

    private static int RunWhereSelect(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.WhereSelect(ArraySource(library), static value => (value & 1) == 0, static value => value * 3)
            : PackageExtensions.WhereSelect(ArraySource(library), static value => (value & 1) == 0, static value => value * 3));

    private static int RunWhereTrue(ExtensionsLibrary library) =>
        DrainBool(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.WhereTrue(BoolSource(library))
            : PackageExtensions.WhereTrue(BoolSource(library)));

    private static int RunWhile(ExtensionsLibrary library)
    {
        var remaining = Count;
        var total = 0;
        return library == ExtensionsLibrary.Primitives
            ? DrainPrimitiveUnit(PrimitivesExtensions.While(() => remaining-- > 0, () => total++)) + total
            : DrainPackageUnit(PackageExtensions.While(() => remaining-- > 0, () => total++)) + total;
    }

    private static int RunWithLimitedConcurrency(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.WithLimitedConcurrency(CompletedTasks(), 4)
            : PackageExtensions.WithLimitedConcurrency(CompletedTasks(), 4));

    private static int SystemReactiveAsSignal() =>
        DrainPrimitiveUnit(RxObservable.Range(0, Count).Select(static _ => RxVoid.Default));

    private static int SystemReactiveCatchAndReturn() =>
        DrainInt(RxObservable.Throw<int>(Boom).Catch(RxObservable.Return(Fallback)));

    private static int SystemReactiveCatchIgnore() =>
        DrainInt(RxObservable.Throw<int>(Boom).Catch(RxObservable.Empty<int>()));

    private static int SystemReactiveCombineLatestValuesAreAllFalse() =>
        DrainBool(BoolSources(ExtensionsLibrary.ReactiveUIExtensions, false).CombineLatest(static values => values.All(static value => !value)));

    private static int SystemReactiveCombineLatestValuesAreAllTrue() =>
        DrainBool(BoolSources(ExtensionsLibrary.ReactiveUIExtensions, true).CombineLatest(static values => values.All(static value => value)));

    private static int SystemReactiveFilter() =>
        DrainString(RxObservable.ToObservable(StringValues).Where(value => EvenRegex.IsMatch(value)));

    private static int SystemReactiveForEach() =>
        DrainInt(RxObservable.Return(Values.AsEnumerable()).SelectMany(static values => values));

    private static int SystemReactiveFromArray() =>
        DrainInt(RxObservable.ToObservable(Values));

    private static int SystemReactiveGetMax() =>
        DrainInt(RxObservable.CombineLatest(RxObservable.Return(1), RxObservable.Return(2), static (left, right) => Math.Max(left, right)));

    private static int SystemReactiveGetMin() =>
        DrainInt(RxObservable.CombineLatest(RxObservable.Return(1), RxObservable.Return(2), static (left, right) => Math.Min(left, right)));

    private static int SystemReactiveNot() =>
        DrainBool(RxObservable.ToObservable(BooleanValues).Select(static value => !value));

    private static int SystemReactivePairwise()
    {
        var observer = new PairObserver();
        using var subscription = RxObservable.Range(0, Count)
            .Buffer(2, 1)
            .Where(static values => values.Count == 2)
            .Select(static values => (Previous: values[0], Current: values[1]))
            .Subscribe(observer);
        return observer.Total;
    }

    private static int SystemReactiveReturn() =>
        DrainInt(RxObservable.Return(Value));

    private static int SystemReactiveScanWithInitial() =>
        DrainInt(RxObservable.Range(0, Count).Scan(0, static (acc, value) => acc + value));

    private static int SystemReactiveSelectAsync() =>
        DrainInt(RxObservable.Range(0, Count).SelectMany(static value => RxObservable.FromAsync(() => Task.FromResult(value + 1))));

    private static int SystemReactiveSelectConstant() =>
        DrainInt(RxObservable.Range(0, Count).Select(static _ => Value));

    private static int SystemReactiveSelectManyThen() =>
        DrainInt(RxObservable.Return(Value)
            .SelectMany(static value => RxObservable.Return(value + 1))
            .SelectMany(static value => RxObservable.Return(value + 1)));

    private static int SystemReactiveSkipWhileNull() =>
        DrainString(RxObservable.ToObservable(NullableStrings).SkipWhile(static value => value is null).Select(static value => value!));

    private static int SystemReactiveTakeUntil() =>
        DrainInt(RxObservable.Range(0, Count).TakeWhile(static value => value <= Match));

    private static int SystemReactiveToHotTask() =>
        RxObservable.Return(Value).ToTask().GetAwaiter().GetResult();

    private static int SystemReactiveWaitUntil() =>
        DrainInt(RxObservable.Range(0, Count).FirstAsync(static value => value == Match));

    private static int SystemReactiveWhereFalse() =>
        DrainBool(RxObservable.ToObservable(BooleanValues).Where(static value => !value));

    private static int SystemReactiveWhereIsNotNull() =>
        DrainString(RxObservable.ToObservable(NullableStrings).Where(static value => value is not null).Select(static value => value!));

    private static int SystemReactiveWhereSelect() =>
        DrainInt(RxObservable.Range(0, Count).Where(static value => (value & 1) == 0).Select(static value => value * 3));

    private static int SystemReactiveWhereTrue() =>
        DrainBool(RxObservable.ToObservable(BooleanValues).Where(static value => value));

    private static int R3AsSignal()
    {
        var observer = new IntR3Observer();
        using var subscription = global::R3.ObservableExtensions.Select(global::R3.Observable.Range(0, Count), static _ => 1).Subscribe(observer);
        return observer.Total;
    }

    private static int R3CatchAndReturn()
    {
        var observer = new IntR3Observer();
        using var subscription = global::R3.ObservableExtensions.Catch<int, Exception>(
                global::R3.Observable.Throw<int>(Boom),
                static _ => global::R3.Observable.Return(Fallback))
            .Subscribe(observer);
        return observer.Total;
    }

    private static int R3CatchIgnore()
    {
        var observer = new IntR3Observer();
        using var subscription = global::R3.ObservableExtensions.Catch<int, Exception>(
                global::R3.Observable.Throw<int>(Boom),
                static _ => global::R3.Observable.Empty<int>())
            .Subscribe(observer);
        return observer.Total;
    }

    private static int R3FromArray()
    {
        var observer = new IntR3Observer();
        using var subscription = global::R3.Observable.ToObservable(Values, CancellationToken.None).Subscribe(observer);
        return observer.Total;
    }

    private static int R3Not()
    {
        var observer = new R3BoolObserver();
        using var subscription = global::R3.ObservableExtensions.Select(
                global::R3.Observable.ToObservable(BooleanValues, CancellationToken.None),
                static value => !value)
            .Subscribe(observer);
        return observer.Total;
    }

    private static int R3Return()
    {
        var observer = new IntR3Observer();
        using var subscription = global::R3.Observable.Return(Value).Subscribe(observer);
        return observer.Total;
    }

    private static int R3SelectConstant()
    {
        var observer = new IntR3Observer();
        using var subscription = global::R3.ObservableExtensions.Select(global::R3.Observable.Range(0, Count), static _ => Value).Subscribe(observer);
        return observer.Total;
    }

    private static int R3WhereFalse()
    {
        var observer = new R3BoolObserver();
        using var subscription = global::R3.ObservableExtensions.Where(
                global::R3.Observable.ToObservable(BooleanValues, CancellationToken.None),
                static value => !value)
            .Subscribe(observer);
        return observer.Total;
    }

    private static int R3WhereIsNotNull()
    {
        var observer = new R3CountingObserver<string>();
        var source = global::R3.Observable.ToObservable(NullableStrings, CancellationToken.None);
        var filtered = global::R3.ObservableExtensions.Where(source, static value => value is not null);
        using var subscription = global::R3.ObservableExtensions.Select(filtered, static value => value!).Subscribe(observer);
        return observer.Count;
    }

    private static int R3WhereSelect()
    {
        var observer = new IntR3Observer();
        using var subscription = global::R3.ObservableExtensions.Select(
                global::R3.ObservableExtensions.Where(global::R3.Observable.Range(0, Count), static value => (value & 1) == 0),
                static value => value * 3)
            .Subscribe(observer);
        return observer.Total;
    }

    private static int R3WhereTrue()
    {
        var observer = new R3BoolObserver();
        using var subscription = global::R3.ObservableExtensions.Where(
                global::R3.Observable.ToObservable(BooleanValues, CancellationToken.None),
                static value => value)
            .Subscribe(observer);
        return observer.Total;
    }

    private static IObservable<int> ArraySource(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.FromArray(Values)
            : PackageExtensions.FromArray(Values);

    private static IObservable<bool> BoolSource(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.FromArray(BooleanValues)
            : PackageExtensions.FromArray(BooleanValues);

    private static IEnumerable<IObservable<bool>> BoolSources(ExtensionsLibrary library, bool value)
    {
        yield return library == ExtensionsLibrary.Primitives
            ? PrimitivesObservables.Return(value)
            : PackageObservables.Return(value);
        yield return library == ExtensionsLibrary.Primitives
            ? PrimitivesObservables.Return(value)
            : PackageObservables.Return(value);
    }

    private static IEnumerable<Task<int>> CompletedTasks()
    {
        for (var i = 0; i < Count; i++)
        {
            yield return Task.FromResult(i);
        }
    }

    private static int[] CreateValues()
    {
        var values = new int[Count];
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = i;
        }

        return values;
    }

    private static int DrainArray(IObservable<int[]> source)
    {
        var observer = new ArrayObserver();
        using var subscription = source.Subscribe(observer);
        return observer.Total;
    }

    private static int DrainBool(IObservable<bool> source)
    {
        var observer = new BoolSignalObserver();
        using var subscription = source.Subscribe(observer);
        return observer.Total + observer.NextCount;
    }

    private static int DrainDateTime(IObservable<DateTime> source)
    {
        var observer = new CountingSignalObserver<DateTime>();
        using var subscription = source.Subscribe(observer);
        return observer.Count + observer.CompletionCount;
    }

    private static int DrainHeartbeat(IObservable<ReactiveUI.Primitives.Extensions.Heartbeat<int>> source)
    {
        var observer = new CountingSignalObserver<ReactiveUI.Primitives.Extensions.Heartbeat<int>>();
        using var subscription = source.Subscribe(observer);
        return observer.Count + observer.CompletionCount;
    }

    private static int DrainInt(IObservable<int> source)
    {
        var observer = new IntSignalObserver();
        using var subscription = source.Subscribe(observer);
        return observer.Total + observer.NextCount;
    }

    private static int DrainList(IObservable<IList<int>> source)
    {
        var observer = new ListObserver();
        using var subscription = source.Subscribe(observer);
        return observer.Total;
    }

    private static int DrainPackageHeartbeat(IObservable<ReactiveUI.Extensions.Heartbeat<int>> source)
    {
        var observer = new CountingSignalObserver<ReactiveUI.Extensions.Heartbeat<int>>();
        using var subscription = source.Subscribe(observer);
        return observer.Count + observer.CompletionCount;
    }

    private static int DrainPackageStale(IObservable<ReactiveUI.Extensions.Stale<int>> source)
    {
        var observer = new CountingSignalObserver<ReactiveUI.Extensions.Stale<int>>();
        using var subscription = source.Subscribe(observer);
        return observer.Count + observer.CompletionCount;
    }

    private static int DrainPackageUnit(IObservable<RxUnit> source)
    {
        var observer = new CountingSignalObserver<RxUnit>();
        using var subscription = source.Subscribe(observer);
        return observer.Count + observer.CompletionCount;
    }

    private static int DrainPrimitiveUnit(IObservable<RxVoid> source)
    {
        var observer = new CountingSignalObserver<RxVoid>();
        using var subscription = source.Subscribe(observer);
        return observer.Count + observer.CompletionCount;
    }

    private static int DrainStale(IObservable<ReactiveUI.Primitives.Extensions.Stale<int>> source)
    {
        var observer = new CountingSignalObserver<ReactiveUI.Primitives.Extensions.Stale<int>>();
        using var subscription = source.Subscribe(observer);
        return observer.Count + observer.CompletionCount;
    }

    private static int DrainString(IObservable<string?> source)
    {
        var observer = new NullableStringLengthObserver();
        using var subscription = source.Subscribe(observer);
        return observer.TotalLength + observer.Count;
    }

    private static int DrainSyncTuple(IObservable<(int Value, IDisposable Sync)> source)
    {
        var observer = new SyncTupleObserver();
        using var subscription = source.Subscribe(observer);
        return observer.Total;
    }

    private static IObservable<int> Range(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? Signal.Sequence(0, Count)
            : RxObservable.Range(0, Count);

    private static IObservable<int> ThrowInt(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? Signal.Fail<int>(Boom)
            : RxObservable.Throw<int>(Boom);

    private static IObservable<RxUnit> ThrowPackageUnit() => RxObservable.Throw<RxUnit>(Boom);

    private static IObservable<RxVoid> ThrowPrimitiveUnit() => Signal.Fail<RxVoid>(Boom);

    private enum ExtensionsLibrary
    {
        Primitives,
        ReactiveUIExtensions,
    }

    public sealed class ExtensionScenario(string name, Func<int> run)
    {
        public int Run() => run();

        public override string ToString() => name;
    }

    private sealed class ArrayObserver : IObserver<int[]>
    {
        public int Total { get; private set; }

        public void OnNext(int[] value) => Total += value.Length;

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }

    private sealed class DummyResource : IDisposable
    {
        public int Count { get; private set; }

        public void Dispose()
        {
        }

        public void Touch() => Count++;
    }

    private sealed class ListObserver : IObserver<IList<int>>
    {
        public int Total { get; private set; }

        public void OnNext(IList<int> value) => Total += value.Count;

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }

    private sealed class NullableStringLengthObserver : IObserver<string?>
    {
        public int Count { get; private set; }

        public int TotalLength { get; private set; }

        public void OnNext(string? value)
        {
            Count++;
            TotalLength += value?.Length ?? 0;
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }

    private sealed class PairObserver : IObserver<(int Previous, int Current)>
    {
        public int Total { get; private set; }

        public void OnNext((int Previous, int Current) value) => Total += value.Previous + value.Current;

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }

    private sealed class PropertySource : INotifyPropertyChanged
    {
        private int _value;

        public event PropertyChangedEventHandler? PropertyChanged;

        public int Value
        {
            get => _value;
            set
            {
                _value = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Value)));
            }
        }
    }

    private sealed class R3BoolObserver : global::R3.Observer<bool>
    {
        public int Total { get; private set; }

        protected override void OnNextCore(bool value)
        {
            if (value)
            {
                Total++;
            }
        }

        protected override void OnErrorResumeCore(Exception error)
        {
        }

        protected override void OnCompletedCore(global::R3.Result result)
        {
        }
    }

    private sealed class R3CountingObserver<T> : global::R3.Observer<T>
    {
        public int Count { get; private set; }

        protected override void OnNextCore(T value) => Count++;

        protected override void OnErrorResumeCore(Exception error)
        {
        }

        protected override void OnCompletedCore(global::R3.Result result)
        {
        }
    }

    private sealed class SyncTupleObserver : IObserver<(int Value, IDisposable Sync)>
    {
        public int Total { get; private set; }

        public void OnNext((int Value, IDisposable Sync) value)
        {
            Total += value.Value;
            value.Sync.Dispose();
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }

    private sealed class TupleObserver<T> : IObserver<(T Value, IDisposable Sync)>
    {
        public int Count { get; private set; }

        public void OnNext((T Value, IDisposable Sync) value)
        {
            Count++;
            value.Sync.Dispose();
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }
}
