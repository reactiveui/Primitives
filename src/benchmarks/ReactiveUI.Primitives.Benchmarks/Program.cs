// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Text;
using BenchmarkDotNet.Running;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Entry point for benchmark execution and smoke-test mode.
/// </summary>
internal static class Program
{
    /// <summary>
    /// The expected Primitives value for the documented SwitchRanges scheduling difference.
    /// </summary>
    private const int SwitchRangesPrimitivesValue = 1856;

    /// <summary>
    /// The expected System.Reactive value for the documented SwitchRanges scheduling difference.
    /// </summary>
    private const int SwitchRangesSystemReactiveValue = 1721;

    /// <summary>
    /// The expected R3 value for the documented SwitchRanges scheduling difference.
    /// </summary>
    private const int SwitchRangesR3Value = 1856;

    /// <summary>
    /// The expected Primitives and R3 value for the documented CombineLatest/WithLatest differences.
    /// </summary>
    private const int CombineWithLatestPrimitivesValue = 536;

    /// <summary>
    /// The expected System.Reactive value for the documented CombineLatestRanges difference.
    /// </summary>
    private const int CombineLatestSystemReactiveValue = 806;

    /// <summary>
    /// The expected System.Reactive value for the documented WithLatestRanges difference.
    /// </summary>
    private const int WithLatestSystemReactiveValue = 416;

    /// <summary>
    /// The benchmark method-name prefix identifying the Primitives library row.
    /// </summary>
    private const string PrimitivesPrefix = "Primitives";

    /// <summary>
    /// The benchmark method-name prefix identifying the System.Reactive library row.
    /// </summary>
    private const string SystemReactivePrefix = "SystemReactive";

    /// <summary>
    /// The benchmark method-name prefix identifying the R3 library row.
    /// </summary>
    private const string R3Prefix = "R3";

    /// <summary>
    /// Maps comparator benchmark method suffixes onto the matching Primitives smoke scenario.
    /// </summary>
    private static readonly Dictionary<string, string> SmokeScenarioAliases =
        new(StringComparer.Ordinal)
        {
            ["ToObservableSubscribe"] = "FromEnumerableSubscribe",
            ["RangeSelectWhere"] = "RangeMapKeep",
            ["SelectManyRange"] = "FlatMapRange",
            ["PrependAppendDefaultIfEmpty"] = "StartWithAppendDefaultIfEmpty",
            ["BehaviorSubject32"] = "StateSignal32",
            ["BehaviorSubject1024"] = "StateSignal1024",
            ["ReplaySubscribe"] = "HistorySubscribe",
            ["CompositeDispose"] = "PocketDispose",
        };

    /// <summary>
    /// Executes benchmarks, or runs a deterministic smoke check with <c>--smoke</c>.
    /// </summary>
    /// <param name="args">BenchmarkDotNet command-line arguments.</param>
    /// <returns>A task that completes when execution is finished.</returns>
    public static async Task Main(string[] args)
    {
        if (args.Contains("--alloc", StringComparer.OrdinalIgnoreCase))
        {
            AllocationProbe.Run();
            return;
        }

        if (args.Contains("--smoke", StringComparer.OrdinalIgnoreCase))
        {
            var originalOutput = Console.Out;
            var capturedOutput = new StringWriter(CultureInfo.InvariantCulture);
            var teeOutput = new SmokeTeeTextWriter(originalOutput, capturedOutput);
            Console.SetOut(teeOutput);
            try
            {
                await RunSmokeBenchmarksAsync();
            }
            finally
            {
                Console.SetOut(originalOutput);
            }

            ValidateSmokeOutput(capturedOutput.ToString());
            return;
        }

        if (args.Contains("--extensions-smoke", StringComparer.OrdinalIgnoreCase))
        {
            RunExtensionComparisonSmoke();
            return;
        }

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }

    private static void RunExtensionComparisonSmoke()
    {
        var benchmarks = new ReactiveExtensionsComparisonBenchmarks();
        RunExtensionScenarioSet(nameof(benchmarks.PrimitivesScenarios), benchmarks.PrimitivesScenarios);
        RunExtensionScenarioSet(nameof(benchmarks.ReactiveUIExtensionsScenarios), benchmarks.ReactiveUIExtensionsScenarios);
        RunExtensionScenarioSet(nameof(benchmarks.SystemReactiveScenarios), benchmarks.SystemReactiveScenarios);
        RunExtensionScenarioSet(nameof(benchmarks.R3Scenarios), benchmarks.R3Scenarios);
        Console.WriteLine("Extensions scenario smoke validation passed.");
    }

    private static void RunExtensionScenarioSet(
        string name,
        IEnumerable<ReactiveExtensionsComparisonBenchmarks.ExtensionScenario> scenarios)
    {
        foreach (var scenario in scenarios)
        {
            Console.WriteLine($"{name}:{scenario}");
            _ = scenario.Run();
        }
    }

    /// <summary>
    /// Runs the deterministic smoke benchmark scenarios and writes their results to the console.
    /// </summary>
    /// <returns>A task that completes when all smoke benchmarks have run.</returns>
    private static async Task RunSmokeBenchmarksAsync()
    {
        var scalar = new ScalarSignalBenchmarks();
        Console.WriteLine($"PrimitivesReturnSubscribe={scalar.PrimitivesReturnSubscribe()}");
        Console.WriteLine($"SystemReactiveReturnSubscribe={scalar.SystemReactiveReturnSubscribe()}");
        Console.WriteLine($"R3ReturnSubscribe={scalar.R3ReturnSubscribe()}");

        var factory = new FactorySignalBenchmarks();
        Console.WriteLine($"PrimitivesEmptySubscribe={factory.PrimitivesEmptySubscribe()}");
        Console.WriteLine($"SystemReactiveEmptySubscribe={factory.SystemReactiveEmptySubscribe()}");
        Console.WriteLine($"R3EmptySubscribe={factory.R3EmptySubscribe()}");
        Console.WriteLine($"PrimitivesRangeSubscribe={factory.PrimitivesRangeSubscribe()}");
        Console.WriteLine($"SystemReactiveRangeSubscribe={factory.SystemReactiveRangeSubscribe()}");
        Console.WriteLine($"R3RangeSubscribe={factory.R3RangeSubscribe()}");
        Console.WriteLine($"PrimitivesRepeatSubscribe={factory.PrimitivesRepeatSubscribe()}");
        Console.WriteLine($"SystemReactiveRepeatSubscribe={factory.SystemReactiveRepeatSubscribe()}");
        Console.WriteLine($"R3RepeatSubscribe={factory.R3RepeatSubscribe()}");
        Console.WriteLine($"PrimitivesThrowSubscribe={factory.PrimitivesThrowSubscribe()}");
        Console.WriteLine($"SystemReactiveThrowSubscribe={factory.SystemReactiveThrowSubscribe()}");
        Console.WriteLine($"R3ThrowSubscribe={factory.R3ThrowSubscribe()}");

        var fromEnumerable = new FactoryFromEnumerableBenchmarks();
        Console.WriteLine($"PrimitivesFromEnumerableSubscribe={fromEnumerable.PrimitivesFromEnumerableSubscribe()}");
        Console.WriteLine($"SystemReactiveToObservableSubscribe={fromEnumerable.SystemReactiveToObservableSubscribe()}");
        Console.WriteLine($"R3ToObservableSubscribe={fromEnumerable.R3ToObservableSubscribe()}");

        var operators = new OperatorMapKeepBenchmarks();
        Console.WriteLine($"PrimitivesRangeMapKeep={operators.PrimitivesRangeMapKeep()}");
        Console.WriteLine($"SystemReactiveRangeSelectWhere={operators.SystemReactiveRangeSelectWhere()}");
        Console.WriteLine($"R3RangeSelectWhere={operators.R3RangeSelectWhere()}");
        Console.WriteLine($"PrimitivesAggregateAnyCount={operators.PrimitivesAggregateAnyCount()}");
        Console.WriteLine($"SystemReactiveAggregateAnyCount={operators.SystemReactiveAggregateAnyCount()}");
        Console.WriteLine($"R3AggregateAnyCount={await operators.R3AggregateAnyCount()}");

        var startWith = new OperatorStartWithAppendDefaultIfEmptyBenchmarks();
        Console.WriteLine(
            $"PrimitivesStartWithAppendDefaultIfEmpty={startWith.PrimitivesStartWithAppendDefaultIfEmpty()}");
        Console.WriteLine(
            $"SystemReactiveStartWithAppendDefaultIfEmpty={startWith.SystemReactiveStartWithAppendDefaultIfEmpty()}");
        Console.WriteLine(
            $"R3PrependAppendDefaultIfEmpty={startWith.R3PrependAppendDefaultIfEmpty()}");
        Console.WriteLine($"PrimitivesDefaultIfEmptyEmpty={startWith.PrimitivesDefaultIfEmptyEmpty()}");
        Console.WriteLine($"SystemReactiveDefaultIfEmptyEmpty={startWith.SystemReactiveDefaultIfEmptyEmpty()}");
        Console.WriteLine($"R3DefaultIfEmptyEmpty={startWith.R3DefaultIfEmptyEmpty()}");

        var flatMap = new OperatorFlatMapRangeBenchmarks();
        Console.WriteLine($"PrimitivesFlatMapRange={flatMap.PrimitivesFlatMapRange()}");
        Console.WriteLine($"SystemReactiveSelectManyRange={flatMap.SystemReactiveSelectManyRange()}");
        Console.WriteLine($"R3SelectManyRange={flatMap.R3SelectManyRange()}");

        var zip = new OperatorZipBenchmarks();
        Console.WriteLine($"PrimitivesZip={zip.PrimitivesZip()}");
        Console.WriteLine($"SystemReactiveZip={zip.SystemReactiveZip()}");
        Console.WriteLine($"R3Zip={zip.R3Zip()}");

        var throughput = new SubjectThroughputBenchmarks();
        Console.WriteLine($"PrimitivesSubjectEmitN32={throughput.PrimitivesSubjectEmit32()}");
        Console.WriteLine($"SystemReactiveSubjectEmitN32={throughput.SystemReactiveSubjectEmit32()}");
        Console.WriteLine($"R3SubjectEmitN32={throughput.R3SubjectEmit32()}");
        Console.WriteLine($"PrimitivesSubjectEmitN1024={throughput.PrimitivesSubjectEmit1024()}");
        Console.WriteLine($"SystemReactiveSubjectEmitN1024={throughput.SystemReactiveSubjectEmit1024()}");
        Console.WriteLine($"R3SubjectEmitN1024={throughput.R3SubjectEmit1024()}");

        var subscriptions = new SubjectSubscriptionBenchmarks();
        Console.WriteLine($"PrimitivesSubjectSubscribeDispose8={subscriptions.PrimitivesSubjectSubscribeDispose8()}");
        Console.WriteLine(
            $"SystemReactiveSubjectSubscribeDispose8={subscriptions.SystemReactiveSubjectSubscribeDispose8()}");
        Console.WriteLine($"R3SubjectSubscribeDispose8={subscriptions.R3SubjectSubscribeDispose8()}");
        Console.WriteLine($"PrimitivesSubjectSubscribeDispose64={subscriptions.PrimitivesSubjectSubscribeDispose64()}");
        Console.WriteLine(
            $"SystemReactiveSubjectSubscribeDispose64={subscriptions.SystemReactiveSubjectSubscribeDispose64()}");
        Console.WriteLine($"R3SubjectSubscribeDispose64={subscriptions.R3SubjectSubscribeDispose64()}");

        var stateful = new StatefulSignalBenchmarks();
        Console.WriteLine($"PrimitivesStateSignal32={stateful.PrimitivesStateSignal32()}");
        Console.WriteLine($"SystemReactiveBehaviorSubject32={stateful.SystemReactiveBehaviorSubject32()}");
        Console.WriteLine($"R3BehaviorSubject32={stateful.R3BehaviorSubject32()}");
        Console.WriteLine($"PrimitivesStateSignal1024={stateful.PrimitivesStateSignal1024()}");
        Console.WriteLine($"SystemReactiveBehaviorSubject1024={stateful.SystemReactiveBehaviorSubject1024()}");
        Console.WriteLine($"R3BehaviorSubject1024={stateful.R3BehaviorSubject1024()}");

        var history = new ReplaySignalBenchmarks();
        Console.WriteLine($"PrimitivesHistorySubscribe={history.PrimitivesHistorySubscribe()}");
        Console.WriteLine($"SystemReactiveReplaySubscribe={history.SystemReactiveReplaySubscribe()}");
        Console.WriteLine($"R3ReplaySubscribe={history.R3ReplaySubscribe()}");

        var taskBridge = new AsyncBridgeBenchmarks();
        Console.WriteLine($"PrimitivesCompletedTaskBridge={taskBridge.PrimitivesCompletedTaskBridge()}");
        Console.WriteLine($"SystemReactiveCompletedTaskBridge={taskBridge.SystemReactiveCompletedTaskBridge()}");
        Console.WriteLine($"R3CompletedTaskBridge={taskBridge.R3CompletedTaskBridge()}");

        await RunExpansionSmokeBenchmarksAsync();

        RunCoreRuntimeSmokeBenchmarks();
    }

    /// <summary>
    /// Runs the expansion-coverage smoke benchmark scenarios and writes their results to the console.
    /// </summary>
    /// <returns>A task that completes when all expansion smoke benchmarks have run.</returns>
    private static async Task RunExpansionSmokeBenchmarksAsync()
    {
        await RunFactoryAdapterExpansionSmokeAsync();
        RunTimeSchedulerSmoke();
        RunHigherOrderSmoke();
        await RunTerminalCollectionSmokeAsync();
        RunConnectableShareSmoke();
        await RunStateTaskCommandSmokeAsync();
    }

    /// <summary>
    /// Runs the factory-adapter expansion smoke benchmarks and writes their results to the console.
    /// </summary>
    /// <returns>A task that completes when the factory-adapter smoke benchmarks have run.</returns>
    private static async Task RunFactoryAdapterExpansionSmokeAsync()
    {
        var factoryAdapters = new FactoryAdapterExpansionBenchmarks();
        Console.WriteLine($"PrimitivesCreateSubscribe={factoryAdapters.PrimitivesCreateSubscribe()}");
        Console.WriteLine($"SystemReactiveCreateSubscribe={factoryAdapters.SystemReactiveCreateSubscribe()}");
        Console.WriteLine($"R3CreateSubscribe={factoryAdapters.R3CreateSubscribe()}");
        Console.WriteLine($"PrimitivesCreateSafeSubscribe={factoryAdapters.PrimitivesCreateSafeSubscribe()}");
        Console.WriteLine($"PrimitivesDeferSubscribe={factoryAdapters.PrimitivesDeferSubscribe()}");
        Console.WriteLine($"SystemReactiveDeferSubscribe={factoryAdapters.SystemReactiveDeferSubscribe()}");
        Console.WriteLine($"R3DeferSubscribe={factoryAdapters.R3DeferSubscribe()}");
        Console.WriteLine($"PrimitivesStartSubscribe={factoryAdapters.PrimitivesStartSubscribe()}");
        Console.WriteLine($"SystemReactiveStartSubscribe={factoryAdapters.SystemReactiveStartSubscribe()}");
        Console.WriteLine($"R3StartSubscribe={factoryAdapters.R3StartSubscribe()}");
        Console.WriteLine($"PrimitivesUnfoldSubscribe={factoryAdapters.PrimitivesUnfoldSubscribe()}");
        Console.WriteLine($"SystemReactiveUnfoldSubscribe={factoryAdapters.SystemReactiveUnfoldSubscribe()}");
        Console.WriteLine($"R3UnfoldSubscribe={factoryAdapters.R3UnfoldSubscribe()}");
        Console.WriteLine($"PrimitivesUseSubscribe={factoryAdapters.PrimitivesUseSubscribe()}");
        Console.WriteLine($"SystemReactiveUseSubscribe={factoryAdapters.SystemReactiveUseSubscribe()}");
        Console.WriteLine($"R3UseSubscribe={factoryAdapters.R3UseSubscribe()}");
        Console.WriteLine(
            $"PrimitivesFromAsyncEnumerableSubscribe={await factoryAdapters.PrimitivesFromAsyncEnumerableSubscribeAsync()}");
        Console.WriteLine(
            $"SystemReactiveFromAsyncEnumerableSubscribe={await factoryAdapters.SystemReactiveFromAsyncEnumerableSubscribeAsync()}");
        Console.WriteLine(
            $"R3FromAsyncEnumerableSubscribe={await factoryAdapters.R3FromAsyncEnumerableSubscribeAsync()}");
        Console.WriteLine($"PrimitivesNeverSubscribeDispose={factoryAdapters.PrimitivesNeverSubscribeDispose()}");
        Console.WriteLine($"SystemReactiveNeverSubscribeDispose={factoryAdapters.SystemReactiveNeverSubscribeDispose()}");
        Console.WriteLine($"R3NeverSubscribeDispose={factoryAdapters.R3NeverSubscribeDispose()}");
    }

    /// <summary>
    /// Runs the time and scheduler operator smoke benchmarks and writes their results to the console.
    /// </summary>
    private static void RunTimeSchedulerSmoke()
    {
        var timeSchedulers = new OperatorTimeSchedulerBenchmarks();
        Console.WriteLine($"PrimitivesDelayRange={timeSchedulers.PrimitivesDelayRange()}");
        Console.WriteLine($"SystemReactiveDelayRange={timeSchedulers.SystemReactiveDelayRange()}");
        Console.WriteLine($"R3DelayRange={timeSchedulers.R3DelayRange()}");
        Console.WriteLine($"PrimitivesDelayStartRange={timeSchedulers.PrimitivesDelayStartRange()}");
        Console.WriteLine($"SystemReactiveDelayStartRange={timeSchedulers.SystemReactiveDelayStartRange()}");
        Console.WriteLine($"R3DelayStartRange={timeSchedulers.R3DelayStartRange()}");
        Console.WriteLine($"PrimitivesThrottleBurst={timeSchedulers.PrimitivesThrottleBurst()}");
        Console.WriteLine($"SystemReactiveThrottleBurst={timeSchedulers.SystemReactiveThrottleBurst()}");
        Console.WriteLine($"R3ThrottleBurst={timeSchedulers.R3ThrottleBurst()}");
        Console.WriteLine($"PrimitivesSampleLatest={timeSchedulers.PrimitivesSampleLatest()}");
        Console.WriteLine($"SystemReactiveSampleLatest={timeSchedulers.SystemReactiveSampleLatest()}");
        Console.WriteLine($"R3SampleLatest={timeSchedulers.R3SampleLatest()}");
        Console.WriteLine($"PrimitivesTimestampRange={timeSchedulers.PrimitivesTimestampRange()}");
        Console.WriteLine($"SystemReactiveTimestampRange={timeSchedulers.SystemReactiveTimestampRange()}");
        Console.WriteLine($"R3TimestampRange={timeSchedulers.R3TimestampRange()}");
        Console.WriteLine($"PrimitivesTimeIntervalRange={timeSchedulers.PrimitivesTimeIntervalRange()}");
        Console.WriteLine($"SystemReactiveTimeIntervalRange={timeSchedulers.SystemReactiveTimeIntervalRange()}");
        Console.WriteLine($"R3TimeIntervalRange={timeSchedulers.R3TimeIntervalRange()}");
        Console.WriteLine($"PrimitivesTimeoutIdle={timeSchedulers.PrimitivesTimeoutIdle()}");
        Console.WriteLine($"SystemReactiveTimeoutIdle={timeSchedulers.SystemReactiveTimeoutIdle()}");
        Console.WriteLine($"R3TimeoutIdle={timeSchedulers.R3TimeoutIdle()}");
        Console.WriteLine($"PrimitivesObserveOnImmediate={timeSchedulers.PrimitivesObserveOnImmediate()}");
        Console.WriteLine($"SystemReactiveObserveOnImmediate={timeSchedulers.SystemReactiveObserveOnImmediate()}");
        Console.WriteLine($"R3ObserveOnImmediate={timeSchedulers.R3ObserveOnImmediate()}");
    }

    /// <summary>
    /// Runs the higher-order operator smoke benchmarks and writes their results to the console.
    /// </summary>
    private static void RunHigherOrderSmoke()
    {
        var higherOrder = new OperatorHigherOrderBenchmarks();
        Console.WriteLine($"PrimitivesConcatRanges={higherOrder.PrimitivesConcatRanges()}");
        Console.WriteLine($"SystemReactiveConcatRanges={higherOrder.SystemReactiveConcatRanges()}");
        Console.WriteLine($"R3ConcatRanges={higherOrder.R3ConcatRanges()}");
        Console.WriteLine($"PrimitivesMergeRanges={higherOrder.PrimitivesMergeRanges()}");
        Console.WriteLine($"SystemReactiveMergeRanges={higherOrder.SystemReactiveMergeRanges()}");
        Console.WriteLine($"R3MergeRanges={higherOrder.R3MergeRanges()}");
        Console.WriteLine($"PrimitivesRaceRanges={higherOrder.PrimitivesRaceRanges()}");
        Console.WriteLine($"SystemReactiveRaceRanges={higherOrder.SystemReactiveRaceRanges()}");
        Console.WriteLine($"R3RaceRanges={higherOrder.R3RaceRanges()}");
        Console.WriteLine($"PrimitivesSwitchRanges={higherOrder.PrimitivesSwitchRanges()}");
        Console.WriteLine($"SystemReactiveSwitchRanges={higherOrder.SystemReactiveSwitchRanges()}");
        Console.WriteLine($"R3SwitchRanges={higherOrder.R3SwitchRanges()}");
        Console.WriteLine($"PrimitivesCombineLatestRanges={higherOrder.PrimitivesCombineLatestRanges()}");
        Console.WriteLine($"SystemReactiveCombineLatestRanges={higherOrder.SystemReactiveCombineLatestRanges()}");
        Console.WriteLine($"R3CombineLatestRanges={higherOrder.R3CombineLatestRanges()}");
        Console.WriteLine($"PrimitivesWithLatestRanges={higherOrder.PrimitivesWithLatestRanges()}");
        Console.WriteLine($"SystemReactiveWithLatestRanges={higherOrder.SystemReactiveWithLatestRanges()}");
        Console.WriteLine($"R3WithLatestRanges={higherOrder.R3WithLatestRanges()}");
        Console.WriteLine($"PrimitivesForkJoinRanges={higherOrder.PrimitivesForkJoinRanges()}");
        Console.WriteLine($"SystemReactiveForkJoinRanges={higherOrder.SystemReactiveForkJoinRanges()}");
        Console.WriteLine($"R3ForkJoinRanges={higherOrder.R3ForkJoinRanges()}");
    }

    /// <summary>
    /// Runs the terminal-collection smoke benchmarks and writes their results to the console.
    /// </summary>
    /// <returns>A task that completes when the terminal-collection smoke benchmarks have run.</returns>
    private static async Task RunTerminalCollectionSmokeAsync()
    {
        var terminalCollections = new TerminalCollectionBenchmarks();
        Console.WriteLine($"PrimitivesCollectList={terminalCollections.PrimitivesCollectList()}");
        Console.WriteLine($"SystemReactiveCollectList={terminalCollections.SystemReactiveCollectList()}");
        Console.WriteLine($"R3CollectList={await terminalCollections.R3CollectList()}");
        WriteSynchronousCollectArrayResults(terminalCollections);
        Console.WriteLine($"R3CollectArray={await terminalCollections.R3CollectArray()}");
        Console.WriteLine($"PrimitivesCollectArrayAsync={await terminalCollections.PrimitivesCollectArrayAsync()}");
        Console.WriteLine($"SystemReactiveCollectArrayAsync={await terminalCollections.SystemReactiveCollectArrayAsync()}");
        Console.WriteLine($"R3CollectArrayAsync={await terminalCollections.R3CollectArrayAsync()}");
        Console.WriteLine($"PrimitivesFirstAsync={await terminalCollections.PrimitivesFirstAsync()}");
        Console.WriteLine($"SystemReactiveFirstAsync={await terminalCollections.SystemReactiveFirstAsync()}");
        Console.WriteLine($"R3FirstAsync={await terminalCollections.R3FirstAsync()}");
        Console.WriteLine($"PrimitivesToTask={await terminalCollections.PrimitivesToTask()}");
        Console.WriteLine($"SystemReactiveToTask={await terminalCollections.SystemReactiveToTask()}");
        Console.WriteLine($"R3ToTask={await terminalCollections.R3ToTask()}");
        Console.WriteLine($"PrimitivesCountPredicate={terminalCollections.PrimitivesCountPredicate()}");
        Console.WriteLine($"SystemReactiveCountPredicate={terminalCollections.SystemReactiveCountPredicate()}");
        Console.WriteLine($"R3CountPredicate={await terminalCollections.R3CountPredicate()}");
        Console.WriteLine($"PrimitivesLongCountPredicate={terminalCollections.PrimitivesLongCountPredicate()}");
        Console.WriteLine($"SystemReactiveLongCountPredicate={terminalCollections.SystemReactiveLongCountPredicate()}");
        Console.WriteLine($"R3LongCountPredicate={await terminalCollections.R3LongCountPredicate()}");
        Console.WriteLine($"PrimitivesAllRange={terminalCollections.PrimitivesAllRange()}");
        Console.WriteLine($"SystemReactiveAllRange={terminalCollections.SystemReactiveAllRange()}");
        Console.WriteLine($"R3AllRange={await terminalCollections.R3AllRange()}");
        Console.WriteLine($"PrimitivesContainsRange={terminalCollections.PrimitivesContainsRange()}");
        Console.WriteLine($"SystemReactiveContainsRange={terminalCollections.SystemReactiveContainsRange()}");
        Console.WriteLine($"R3ContainsRange={await terminalCollections.R3ContainsRange()}");
        Console.WriteLine($"PrimitivesAllContains={terminalCollections.PrimitivesAllContains()}");
        Console.WriteLine($"SystemReactiveAllContains={terminalCollections.SystemReactiveAllContains()}");
        Console.WriteLine($"R3AllContains={await terminalCollections.R3AllContains()}");
    }

    /// <summary>
    /// Writes the synchronous array-collection smoke results from a non-async method so the
    /// synchronous CollectArray benchmarks are measured without awaiting their async overloads.
    /// </summary>
    /// <param name="terminalCollections">The terminal-collection benchmarks instance.</param>
    private static void WriteSynchronousCollectArrayResults(TerminalCollectionBenchmarks terminalCollections)
    {
        Console.WriteLine($"PrimitivesCollectArray={terminalCollections.PrimitivesCollectArray()}");
        Console.WriteLine($"SystemReactiveCollectArray={terminalCollections.SystemReactiveCollectArray()}");
    }

    /// <summary>
    /// Runs the connectable/share smoke benchmarks and writes their results to the console.
    /// </summary>
    private static void RunConnectableShareSmoke()
    {
        var connectableShare = new ConnectableShareBenchmarks();
        Console.WriteLine($"PrimitivesPublishLiveConnect={connectableShare.PrimitivesPublishLiveConnect()}");
        Console.WriteLine($"SystemReactivePublishLiveConnect={connectableShare.SystemReactivePublishLiveConnect()}");
        Console.WriteLine($"R3PublishLiveConnect={connectableShare.R3PublishLiveConnect()}");
        Console.WriteLine($"PrimitivesShareLiveSubscribe={connectableShare.PrimitivesShareLiveSubscribe()}");
        Console.WriteLine($"SystemReactiveShareLiveSubscribe={connectableShare.SystemReactiveShareLiveSubscribe()}");
        Console.WriteLine($"R3ShareLiveSubscribe={connectableShare.R3ShareLiveSubscribe()}");
        Console.WriteLine($"PrimitivesReplayLiveLateSubscribe={connectableShare.PrimitivesReplayLiveLateSubscribe()}");
        Console.WriteLine($"SystemReactiveReplayLiveLateSubscribe={connectableShare.SystemReactiveReplayLiveLateSubscribe()}");
        Console.WriteLine($"R3ReplayLiveLateSubscribe={connectableShare.R3ReplayLiveLateSubscribe()}");
        Console.WriteLine($"PrimitivesRefCountSubscribe={connectableShare.PrimitivesRefCountSubscribe()}");
        Console.WriteLine($"R3RefCountSubscribe={connectableShare.R3RefCountSubscribe()}");
        Console.WriteLine($"PrimitivesAutoConnectSubscribe={connectableShare.PrimitivesAutoConnectSubscribe()}");
        Console.WriteLine($"SystemReactiveAutoConnectSubscribe={connectableShare.SystemReactiveAutoConnectSubscribe()}");
    }

    /// <summary>
    /// Runs the state, task, and command smoke benchmarks and writes their results to the console.
    /// </summary>
    /// <returns>A task that completes when the state/task/command smoke benchmarks have run.</returns>
    private static async Task RunStateTaskCommandSmokeAsync()
    {
        var stateTaskCommand = new StateTaskCommandBenchmarks();
        Console.WriteLine($"PrimitivesStateSignalUpdates={stateTaskCommand.PrimitivesStateSignalUpdates()}");
        Console.WriteLine($"SystemReactiveStateSignalUpdates={stateTaskCommand.SystemReactiveStateSignalUpdates()}");
        Console.WriteLine($"R3StateSignalUpdates={stateTaskCommand.R3StateSignalUpdates()}");
        Console.WriteLine($"PrimitivesReadOnlyStateProjection={stateTaskCommand.PrimitivesReadOnlyStateProjection()}");
        Console.WriteLine(
            $"SystemReactiveReadOnlyStateProjection={stateTaskCommand.SystemReactiveReadOnlyStateProjection()}");
        Console.WriteLine($"R3ReadOnlyStateProjection={stateTaskCommand.R3ReadOnlyStateProjection()}");
        Console.WriteLine($"PrimitivesTaskSignalSubscribe={stateTaskCommand.PrimitivesTaskSignalSubscribe()}");
        Console.WriteLine($"SystemReactiveTaskSignalSubscribe={stateTaskCommand.SystemReactiveTaskSignalSubscribe()}");
        Console.WriteLine($"R3TaskSignalSubscribe={stateTaskCommand.R3TaskSignalSubscribe()}");
        Console.WriteLine($"PrimitivesCommandExecute={await stateTaskCommand.PrimitivesCommandExecuteAsync()}");
        Console.WriteLine($"SystemReactiveCommandExecute={await stateTaskCommand.SystemReactiveCommandExecuteAsync()}");
        Console.WriteLine($"R3CommandExecute={stateTaskCommand.R3CommandExecute()}");
        Console.WriteLine(
            $"PrimitivesCommandResultSubscribe={await stateTaskCommand.PrimitivesCommandResultSubscribeAsync()}");
        Console.WriteLine($"SystemReactiveCommandResultSubscribe={stateTaskCommand.SystemReactiveCommandResultSubscribe()}");
        Console.WriteLine($"R3CommandResultSubscribe={stateTaskCommand.R3CommandResultSubscribe()}");
    }

    /// <summary>
    /// Runs the core-runtime smoke benchmark scenarios and writes their results to the console.
    /// </summary>
    private static void RunCoreRuntimeSmokeBenchmarks()
    {
        var coreRuntime = new CoreRuntimeBenchmarks();
        Console.WriteLine($"PrimitivesPocketDispose={coreRuntime.PrimitivesPocketDispose()}");
        Console.WriteLine($"SystemReactiveCompositeDispose={coreRuntime.SystemReactiveCompositeDispose()}");
        Console.WriteLine($"R3CompositeDispose={coreRuntime.R3CompositeDispose()}");
        Console.WriteLine($"PrimitivesCurrentThreadSchedule={coreRuntime.PrimitivesCurrentThreadSchedule()}");
        Console.WriteLine(
            $"SystemReactiveCurrentThreadSchedule={coreRuntime.SystemReactiveCurrentThreadSchedule()}");
        Console.WriteLine($"R3CurrentThreadSchedule={coreRuntime.R3CurrentThreadSchedule()}");
        Console.WriteLine($"PrimitivesSafeWitness={coreRuntime.PrimitivesSafeWitness()}");
        Console.WriteLine($"SystemReactiveSafeWitness={coreRuntime.SystemReactiveSafeWitness()}");
        Console.WriteLine($"R3SafeWitness={coreRuntime.R3SafeWitness()}");
        Console.WriteLine($"PrimitivesCompletedSpark={coreRuntime.PrimitivesCompletedSpark()}");
        Console.WriteLine($"SystemReactiveCompletedSpark={coreRuntime.SystemReactiveCompletedSpark()}");
        Console.WriteLine($"R3CompletedSpark={coreRuntime.R3CompletedSpark()}");
    }

    /// <summary>
    /// Validates the captured smoke output for parity across the participating libraries.
    /// </summary>
    /// <param name="output">The captured smoke benchmark console output.</param>
    private static void ValidateSmokeOutput(string output)
    {
        var results = output.Split([Environment.NewLine], StringSplitOptions.RemoveEmptyEntries);
        var failures = new List<string>();
        var groupCount = 0;
        var index = 0;
        while (index < results.Length)
        {
            var (firstName, firstValue) = ParseSmokeResult(results[index]);
            var scenario = NormalizeSmokeScenarioName(firstName);
            var rows = new List<(string Name, int Value)> { (firstName, firstValue) };
            var next = index + 1;
            while (next < results.Length)
            {
                var (name, value) = ParseSmokeResult(results[next]);
                if (NormalizeSmokeScenarioName(name) != scenario)
                {
                    break;
                }

                rows.Add((name, value));
                next++;
            }

            var failure = ValidateSmokeGroup(index + 1, rows);
            if (failure is not null)
            {
                failures.Add(failure);
            }

            groupCount++;
            index = next;
        }

        if (failures.Count > 0)
        {
            throw new InvalidOperationException(
                "Benchmark smoke parity validation failed:" + Environment.NewLine + string.Join(Environment.NewLine, failures));
        }

        Console.WriteLine($"Smoke parity validation passed for {groupCount} benchmark groups.");
    }

    /// <summary>
    /// Validates a group of consecutive smoke rows for one scenario, one row per participating library.
    /// </summary>
    /// <param name="firstRowNumber">The one-based row number of the first row in the group.</param>
    /// <param name="rows">The library result rows for the scenario, in emission order.</param>
    /// <returns>A failure description, or <see langword="null"/> when the group is valid.</returns>
    private static string? ValidateSmokeGroup(int firstRowNumber, List<(string Name, int Value)> rows)
    {
        string? primitivesName = null;
        var primitivesValue = 0;
        for (var i = 0; i < rows.Count; i++)
        {
            var name = rows[i].Name;
            if (!HasKnownLibraryPrefix(name))
            {
                return $"Row {firstRowNumber} group contains an unrecognized library prefix: {name}.";
            }

            if (primitivesName is null && name.StartsWith(PrimitivesPrefix, StringComparison.Ordinal))
            {
                primitivesName = name;
                primitivesValue = rows[i].Value;
            }
        }

        if (primitivesName is null)
        {
            return $"Row {firstRowNumber} group has no Primitives result.";
        }

        return IsDocumentedSmokeDifference(primitivesName)
            ? ValidateDocumentedSmokeDifference(rows)
            : ValidateSmokeParity(primitivesName, primitivesValue, rows);
    }

    /// <summary>
    /// Determines whether the name carries a recognized library prefix.
    /// </summary>
    /// <param name="name">The benchmark result name.</param>
    /// <returns><see langword="true"/> when the name has a known library prefix.</returns>
    private static bool HasKnownLibraryPrefix(string name) =>
        name.StartsWith(PrimitivesPrefix, StringComparison.Ordinal) ||
        name.StartsWith(SystemReactivePrefix, StringComparison.Ordinal) ||
        name.StartsWith(R3Prefix, StringComparison.Ordinal);

    /// <summary>
    /// Validates that every library row in the group matches the Primitives value.
    /// </summary>
    /// <param name="primitivesName">The Primitives result name.</param>
    /// <param name="primitivesValue">The Primitives result value.</param>
    /// <param name="rows">The library result rows for the scenario.</param>
    /// <returns>A failure description, or <see langword="null"/> when the values match.</returns>
    private static string? ValidateSmokeParity(string primitivesName, int primitivesValue, List<(string Name, int Value)> rows)
    {
        for (var i = 0; i < rows.Count; i++)
        {
            if (rows[i].Value != primitivesValue)
            {
                var parts = new string[rows.Count];
                for (var j = 0; j < rows.Count; j++)
                {
                    parts[j] = $"{rows[j].Name}={rows[j].Value}";
                }

                return $"{primitivesName}: expected parity but got {string.Join(", ", parts)}.";
            }
        }

        return null;
    }

    /// <summary>
    /// Normalizes a benchmark result name to its underlying smoke scenario name.
    /// </summary>
    /// <param name="name">The benchmark result name including its library prefix.</param>
    /// <returns>The normalized smoke scenario name.</returns>
    private static string NormalizeSmokeScenarioName(string name)
    {
        string scenario;
        if (name.StartsWith(SystemReactivePrefix, StringComparison.Ordinal))
        {
            scenario = name[SystemReactivePrefix.Length..];
        }
        else if (name.StartsWith(PrimitivesPrefix, StringComparison.Ordinal))
        {
            scenario = name[PrimitivesPrefix.Length..];
        }
        else
        {
            scenario = name[R3Prefix.Length..];
        }

        return SmokeScenarioAliases.TryGetValue(scenario, out var normalized) ? normalized : scenario;
    }

    /// <summary>
    /// Determines whether the named scenario has a documented, expected parity difference.
    /// </summary>
    /// <param name="primitivesName">The Primitives result name.</param>
    /// <returns><see langword="true"/> when the scenario is a documented difference; otherwise, <see langword="false"/>.</returns>
    private static bool IsDocumentedSmokeDifference(string primitivesName) =>
        primitivesName is "PrimitivesSwitchRanges" or
            "PrimitivesCombineLatestRanges" or
            "PrimitivesWithLatestRanges";

    /// <summary>
    /// Splits the scenario rows into the Primitives name and the per-library values.
    /// </summary>
    /// <param name="rows">The library result rows for the scenario.</param>
    /// <returns>The Primitives name and the Primitives, System.Reactive, and R3 values.</returns>
    private static (string? PrimitivesName, int PrimitivesValue, int SystemReactiveValue, int R3Value) SplitLibraryValues(
        List<(string Name, int Value)> rows)
    {
        string? primitivesName = null;
        var primitivesValue = 0;
        var systemReactiveValue = 0;
        var r3Value = 0;
        for (var i = 0; i < rows.Count; i++)
        {
            var (name, value) = rows[i];
            if (name.StartsWith(SystemReactivePrefix, StringComparison.Ordinal))
            {
                systemReactiveValue = value;
            }
            else if (name.StartsWith(PrimitivesPrefix, StringComparison.Ordinal))
            {
                primitivesName = name;
                primitivesValue = value;
            }
            else
            {
                r3Value = value;
            }
        }

        return (primitivesName, primitivesValue, systemReactiveValue, r3Value);
    }

    /// <summary>
    /// Validates a scenario with a documented, expected parity difference against its known values.
    /// </summary>
    /// <param name="rows">The library result rows for the scenario.</param>
    /// <returns>A failure description, or <see langword="null"/> when the values match the documented difference.</returns>
    private static string? ValidateDocumentedSmokeDifference(List<(string Name, int Value)> rows)
    {
        var (primitivesName, primitivesValue, systemReactiveValue, r3Value) = SplitLibraryValues(rows);

        var expected = primitivesName switch
        {
            "PrimitivesSwitchRanges" => (Primitives: SwitchRangesPrimitivesValue, SystemReactive: SwitchRangesSystemReactiveValue, R3: SwitchRangesR3Value),
            "PrimitivesCombineLatestRanges" => (Primitives: CombineWithLatestPrimitivesValue, SystemReactive: CombineLatestSystemReactiveValue, R3: CombineWithLatestPrimitivesValue),
            "PrimitivesWithLatestRanges" => (Primitives: CombineWithLatestPrimitivesValue, SystemReactive: WithLatestSystemReactiveValue, R3: CombineWithLatestPrimitivesValue),
            _ => default,
        };

        if (expected == default)
        {
            return null;
        }

        return primitivesValue == expected.Primitives &&
               systemReactiveValue == expected.SystemReactive &&
               r3Value == expected.R3
            ? null
            : $"{primitivesName}: documented scheduling difference changed; expected " +
              $"Primitives={expected.Primitives}, System.Reactive={expected.SystemReactive}, R3={expected.R3}, " +
              $"but got Primitives={primitivesValue}, System.Reactive={systemReactiveValue}, R3={r3Value}.";
    }

    /// <summary>
    /// Parses a single <c>key=value</c> smoke output row into its name and integer value.
    /// </summary>
    /// <param name="line">The smoke output row to parse.</param>
    /// <returns>A tuple containing the result name and its integer value.</returns>
    private static (string Name, int Value) ParseSmokeResult(string line)
    {
        var separator = line.IndexOf('=', StringComparison.Ordinal);
        if (separator <= 0 || separator == line.Length - 1)
        {
            throw new InvalidOperationException($"Smoke output row is not key=value: {line}");
        }

        var value = int.Parse(line[(separator + 1)..], CultureInfo.InvariantCulture);
        return (line[..separator], value);
    }

    /// <summary>
    /// A <see cref="TextWriter"/> that mirrors every write to a primary and a secondary writer.
    /// </summary>
    /// <param name="primary">The primary writer to forward writes to.</param>
    /// <param name="secondary">The secondary writer to forward writes to.</param>
    private sealed class SmokeTeeTextWriter(TextWriter primary, TextWriter secondary) : TextWriter
    {
        /// <summary>
        /// Gets the character encoding of the primary writer.
        /// </summary>
        public override Encoding Encoding => primary.Encoding;

        /// <summary>
        /// Writes a character to both the primary and secondary writers.
        /// </summary>
        /// <param name="value">The character to write.</param>
        public override void Write(char value)
        {
            primary.Write(value);
            secondary.Write(value);
        }

        /// <summary>
        /// Writes a string to both the primary and secondary writers.
        /// </summary>
        /// <param name="value">The string to write.</param>
        public override void Write(string? value)
        {
            primary.Write(value);
            secondary.Write(value);
        }

        /// <summary>
        /// Writes a string followed by a line terminator to both the primary and secondary writers.
        /// </summary>
        /// <param name="value">The string to write.</param>
        public override void WriteLine(string? value)
        {
            primary.WriteLine(value);
            secondary.WriteLine(value);
        }
    }
}
