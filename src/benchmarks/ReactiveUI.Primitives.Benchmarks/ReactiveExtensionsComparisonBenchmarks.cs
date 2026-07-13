// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks the complete synchronous ReactiveUI.Primitives.Extensions public helper surface.</summary>
[MemoryDiagnoser]
public partial class ReactiveExtensionsComparisonBenchmarks
{
    /// <summary>The number of values emitted by range-based scenarios.</summary>
    private const int Count = 32;

    /// <summary>The multiplier used by candidate projection scenarios.</summary>
    private const int CandidateMultiplier = 2;

    /// <summary>The divisor used by even-number predicates.</summary>
    private const int EvenDivisor = 2;

    /// <summary>The fallback value used by catch and latest scenarios.</summary>
    private const int Fallback = 42;

    /// <summary>The first scalar value used by min and max comparisons.</summary>
    private const int FirstValue = 1;

    /// <summary>The match threshold used by predicate scenarios.</summary>
    private const int Match = 8;

    /// <summary>The concurrency cap used by concurrent scenarios.</summary>
    private const int MaxConcurrency = 4;

    /// <summary>The sliding window size used to rebuild pairwise semantics from buffering.</summary>
    private const int PairwiseWindow = 2;

    /// <summary>The multiplier used by where-select scenarios.</summary>
    private const int ResultMultiplier = 3;

    /// <summary>The second scalar value used by min and max comparisons.</summary>
    private const int SecondValue = 2;

    /// <summary>The scalar payload used by single-value scenarios.</summary>
    private const int Value = 7;

    /// <summary>The scheduler tick used by virtual-time scenarios.</summary>
    private static readonly TimeSpan Tick = TimeSpan.FromTicks(1);

    /// <summary>The timeout used by blocking subscription helpers.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(1);

    /// <summary>The exception instance used by error-path scenarios.</summary>
    private static readonly Exception Boom = new InvalidOperationException("benchmark");

    /// <summary>Boolean payloads used by predicate and combine-latest scenarios.</summary>
    private static readonly bool[] BooleanValues = [true, false, false, true, false, true, false, false];

    /// <summary>Character payloads used by buffer-until scenarios.</summary>
    private static readonly char[] BufferCharacters = "xx[abc]yy[de]".ToCharArray();

    /// <summary>Integer payloads used by array-backed scenarios.</summary>
    private static readonly int[] Values = CreateValues();

    /// <summary>Nullable string payloads used by null-filtering scenarios.</summary>
    private static readonly string?[] NullableStrings = ["one", null, "two", null, "three"];

    /// <summary>String payloads used by skip-while-null scenarios.</summary>
    private static readonly string[] SkipStrings = ["one", null!, "two", null!, "three"];

    /// <summary>String payloads used by regex filter scenarios.</summary>
    private static readonly string[] StringValues = ["0", "1", "2", "3", "4", "5"];

    /// <summary>The ReactiveUI.Primitives benchmark scenario list.</summary>
    private static readonly ExtensionScenario[] PrimitivesScenarioItems =
        CreateLibraryScenarios(ExtensionsLibrary.Primitives);

    /// <summary>The ReactiveUI.Extensions benchmark scenario list.</summary>
    private static readonly ExtensionScenario[] PackageScenarioItems =
        CreateLibraryScenarios(ExtensionsLibrary.ReactiveUIExtensions);

    /// <summary>The System.Reactive comparison scenario list.</summary>
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
        Scenario("SelectAsync", SystemReactiveSelectAsyncScenario),
        Scenario("SelectConstant", SystemReactiveSelectConstant),
        Scenario("SelectManyThen", SystemReactiveSelectManyThen),
        Scenario("SkipWhileNull", SystemReactiveSkipWhileNull),
        Scenario("TakeUntil", SystemReactiveTakeUntil),
        Scenario("ToHotTask", SystemReactiveToHotTask),
        Scenario("WaitUntil", SystemReactiveWaitUntil),
        Scenario("WhereFalse", SystemReactiveWhereFalse),
        Scenario("WhereIsNotNull", SystemReactiveWhereIsNotNull),
        Scenario("WhereSelect", SystemReactiveWhereSelect),
        Scenario("WhereTrue", SystemReactiveWhereTrue)
    ];

    /// <summary>The R3 comparison scenario list.</summary>
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
        Scenario("WhereTrue", R3WhereTrue)
    ];

    /// <summary>Identifies the library implementation used by paired scenario runners.</summary>
    private enum ExtensionsLibrary
    {
        /// <summary>The ReactiveUI.Primitives.Extensions implementation.</summary>
        Primitives,

        /// <summary>The ReactiveUI.Extensions implementation.</summary>
        ReactiveUIExtensions
    }

    /// <summary>Gets the ReactiveUI.Primitives scenarios.</summary>
    public static IEnumerable<ExtensionScenario> PrimitivesScenarios => PrimitivesScenarioItems;

    /// <summary>Gets the ReactiveUI.Extensions scenarios.</summary>
    public static IEnumerable<ExtensionScenario> ReactiveUIExtensionsScenarios => PackageScenarioItems;

    /// <summary>Gets the System.Reactive comparison scenarios.</summary>
    public static IEnumerable<ExtensionScenario> SystemReactiveScenarios => SystemReactiveScenarioItems;

    /// <summary>Gets the R3 comparison scenarios.</summary>
    public static IEnumerable<ExtensionScenario> R3Scenarios => R3ScenarioItems;

    /// <summary>Runs a ReactiveUI.Primitives scenario.</summary>
    /// <param name="scenario">The scenario to run.</param>
    /// <returns>The benchmark checksum.</returns>
    [Benchmark]
    [ArgumentsSource(nameof(PrimitivesScenarios))]
    public int Primitives(ExtensionScenario scenario) => scenario.Run();

    /// <summary>Runs a ReactiveUI.Extensions scenario.</summary>
    /// <param name="scenario">The scenario to run.</param>
    /// <returns>The benchmark checksum.</returns>
    [Benchmark]
    [ArgumentsSource(nameof(ReactiveUIExtensionsScenarios))]
    public int ReactiveUIExtensions(ExtensionScenario scenario) => scenario.Run();

    /// <summary>Runs a System.Reactive comparison scenario.</summary>
    /// <param name="scenario">The scenario to run.</param>
    /// <returns>The benchmark checksum.</returns>
    [Benchmark]
    [ArgumentsSource(nameof(SystemReactiveScenarios))]
    public int SystemReactive(ExtensionScenario scenario) => scenario.Run();

    /// <summary>Runs an R3 comparison scenario.</summary>
    /// <param name="scenario">The scenario to run.</param>
    /// <returns>The benchmark checksum.</returns>
    [Benchmark]
    [ArgumentsSource(nameof(R3Scenarios))]
    public int R3Library(ExtensionScenario scenario) => scenario.Run();

    /// <summary>Creates the full paired library scenario list.</summary>
    /// <param name="library">The paired library to benchmark.</param>
    /// <returns>The scenario list.</returns>
    private static ExtensionScenario[] CreateLibraryScenarios(ExtensionsLibrary library) =>
    [
        ..CreateCoreLibraryScenarios(library),
        ..CreateSupplementalLibraryScenarios(library)
    ];

    /// <summary>Creates the core paired library scenario list.</summary>
    /// <param name="library">The paired library to benchmark.</param>
    /// <returns>The core scenario list.</returns>
    private static ExtensionScenario[] CreateCoreLibraryScenarios(ExtensionsLibrary library) =>
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
        Scenario("Pairwise", () => RunPairwise(library))
    ];

    /// <summary>Creates the supplemental paired library scenario list.</summary>
    /// <param name="library">The paired library to benchmark.</param>
    /// <returns>The supplemental scenario list.</returns>
    private static ExtensionScenario[] CreateSupplementalLibraryScenarios(ExtensionsLibrary library) =>
    [
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
        Scenario("SelectAsync", () => RunSelectAsyncScenario(library)),
        Scenario("SelectAsyncConcurrent", () => RunSelectAsyncConcurrent(library)),
        Scenario("SelectAsyncSequential", () => RunSelectAsyncSequential(library)),
        Scenario("SelectConstant", () => RunSelectConstant(library)),
        Scenario("SelectLatestAsync", () => RunSelectLatestAsyncScenario(library)),
        Scenario("SelectManyThen", () => RunSelectManyThen(library)),
        Scenario("Shuffle", () => RunShuffle(library)),
        Scenario("SkipWhileNull", () => RunSkipWhileNull(library)),
        Scenario("Start", () => RunStart(library)),
        Scenario("SubscribeAndComplete", () => RunSubscribeAndComplete(library)),
        Scenario("SubscribeAsync", () => RunSubscribeAsyncScenario(library)),
        Scenario("SubscribeGetError", () => RunSubscribeGetError(library)),
        Scenario("SubscribeGetValue", () => RunSubscribeGetValue(library)),
        Scenario("SubscribeSynchronous", () => RunSubscribeSynchronous(library)),
        Scenario("SwitchIfEmpty", () => RunSwitchIfEmpty(library)),
        Scenario("SyncTimer", () => RunSyncTimer(library)),
        Scenario("SynchronizeAsync", () => RunSynchronizeAsyncScenario(library)),
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
        Scenario("WithLimitedConcurrency", () => RunWithLimitedConcurrency(library))
    ];

    /// <summary>Creates a named benchmark scenario.</summary>
    /// <param name="name">The scenario name.</param>
    /// <param name="run">The delegate that runs the scenario.</param>
    /// <returns>The benchmark scenario.</returns>
    private static ExtensionScenario Scenario(string name, Func<int> run) => new(name, run);
}
