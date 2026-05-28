// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Globalization;
using System.IO;
using System.Text;
using BenchmarkDotNet.Running;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Entry point for benchmark execution and smoke-test mode.
/// </summary>
internal static class Program
{
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

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }

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

        var history = new HistorySignalBenchmarks();
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

    private static async Task RunExpansionSmokeBenchmarksAsync()
    {
        var factoryAdapters = new FactoryAdapterExpansionBenchmarks();
        Console.WriteLine($"PrimitivesCreateSubscribe={factoryAdapters.PrimitivesCreateSubscribe()}");
        Console.WriteLine($"SystemReactiveCreateSubscribe={factoryAdapters.SystemReactiveCreateSubscribe()}");
        Console.WriteLine($"R3CreateSubscribe={factoryAdapters.R3CreateSubscribe()}");
        Console.WriteLine($"PrimitivesCreateSafeSubscribe={factoryAdapters.PrimitivesCreateSafeSubscribe()}");
        Console.WriteLine($"SystemReactiveCreateSafeSubscribe={factoryAdapters.SystemReactiveCreateSafeSubscribe()}");
        Console.WriteLine($"R3CreateSafeSubscribe={factoryAdapters.R3CreateSafeSubscribe()}");
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

        var terminalCollections = new TerminalCollectionBenchmarks();
        Console.WriteLine($"PrimitivesCollectList={terminalCollections.PrimitivesCollectList()}");
        Console.WriteLine($"SystemReactiveCollectList={terminalCollections.SystemReactiveCollectList()}");
        Console.WriteLine($"R3CollectList={await terminalCollections.R3CollectList()}");
        Console.WriteLine($"PrimitivesCollectArray={terminalCollections.PrimitivesCollectArray()}");
        Console.WriteLine($"SystemReactiveCollectArray={terminalCollections.SystemReactiveCollectArray()}");
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
        Console.WriteLine($"SystemReactiveRefCountSubscribe={connectableShare.SystemReactiveRefCountSubscribe()}");
        Console.WriteLine($"R3RefCountSubscribe={connectableShare.R3RefCountSubscribe()}");
        Console.WriteLine($"PrimitivesAutoConnectSubscribe={connectableShare.PrimitivesAutoConnectSubscribe()}");
        Console.WriteLine($"SystemReactiveAutoConnectSubscribe={connectableShare.SystemReactiveAutoConnectSubscribe()}");
        Console.WriteLine($"R3AutoConnectSubscribe={connectableShare.R3AutoConnectSubscribe()}");

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

    private static void ValidateSmokeOutput(string output)
    {
        var results = output.Split([Environment.NewLine], StringSplitOptions.RemoveEmptyEntries);
        if (results.Length % 3 != 0)
        {
            throw new InvalidOperationException(
                $"Smoke rows must be emitted in Primitives/System.Reactive/R3 triples; found {results.Length} rows.");
        }

        var failures = new List<string>();
        for (var i = 0; i < results.Length; i += 3)
        {
            var (primitivesName, primitivesValue) = ParseSmokeResult(results[i]);
            var (systemReactiveName, systemReactiveValue) = ParseSmokeResult(results[i + 1]);
            var (r3Name, r3Value) = ParseSmokeResult(results[i + 2]);

            var failure = ValidateSmokeTriple(
                i + 1,
                primitivesName,
                primitivesValue,
                systemReactiveName,
                systemReactiveValue,
                r3Name,
                r3Value);
            if (failure is not null)
            {
                failures.Add(failure);
            }
        }

        if (failures.Count > 0)
        {
            throw new InvalidOperationException(
                "Benchmark smoke parity validation failed:" + Environment.NewLine + string.Join(Environment.NewLine, failures));
        }

        Console.WriteLine($"Smoke parity validation passed for {results.Length / 3} benchmark groups.");
    }

    private static string? ValidateSmokeTriple(
        int firstRowNumber,
        string primitivesName,
        int primitivesValue,
        string systemReactiveName,
        int systemReactiveValue,
        string r3Name,
        int r3Value)
    {
        if (!primitivesName.StartsWith("Primitives", StringComparison.Ordinal) ||
            !systemReactiveName.StartsWith("SystemReactive", StringComparison.Ordinal) ||
            !r3Name.StartsWith("R3", StringComparison.Ordinal))
        {
            return $"Rows {firstRowNumber}-{firstRowNumber + 2} are not ordered as Primitives/System.Reactive/R3.";
        }

        var primitivesScenario = NormalizeSmokeScenarioName(primitivesName);
        var systemReactiveScenario = NormalizeSmokeScenarioName(systemReactiveName);
        var r3Scenario = NormalizeSmokeScenarioName(r3Name);
        if (primitivesScenario != systemReactiveScenario || primitivesScenario != r3Scenario)
        {
            return $"Rows {firstRowNumber}-{firstRowNumber + 2} are not the same smoke scenario: " +
                   $"{primitivesName}, {systemReactiveName}, {r3Name}.";
        }

        if (IsDocumentedSmokeDifference(primitivesName))
        {
            return ValidateDocumentedSmokeDifference(primitivesName, primitivesValue, systemReactiveValue, r3Value);
        }

        return ValidateExpectedSmokeParity(primitivesName, primitivesValue, systemReactiveValue, r3Value);
    }

    private static string NormalizeSmokeScenarioName(string name)
    {
        string scenario;
        if (name.StartsWith("SystemReactive", StringComparison.Ordinal))
        {
            scenario = name["SystemReactive".Length..];
        }
        else if (name.StartsWith("Primitives", StringComparison.Ordinal))
        {
            scenario = name["Primitives".Length..];
        }
        else
        {
            scenario = name["R3".Length..];
        }

        return SmokeScenarioAliases.TryGetValue(scenario, out var normalized) ? normalized : scenario;
    }

    private static string? ValidateExpectedSmokeParity(
        string primitivesName,
        int primitivesValue,
        int systemReactiveValue,
        int r3Value)
    {
        if (primitivesValue == systemReactiveValue && primitivesValue == r3Value)
        {
            return null;
        }

        return $"{primitivesName}: expected parity but got Primitives={primitivesValue}, " +
               $"System.Reactive={systemReactiveValue}, R3={r3Value}.";
    }

    private static bool IsDocumentedSmokeDifference(string primitivesName)
    {
        return primitivesName is "PrimitivesSwitchRanges" or
            "PrimitivesCombineLatestRanges" or
            "PrimitivesWithLatestRanges";
    }

    private static string? ValidateDocumentedSmokeDifference(
        string primitivesName,
        int primitivesValue,
        int systemReactiveValue,
        int r3Value)
    {
        var expected = primitivesName switch
        {
            "PrimitivesSwitchRanges" => (Primitives: 1856, SystemReactive: 1721, R3: 1856),
            "PrimitivesCombineLatestRanges" => (Primitives: 536, SystemReactive: 806, R3: 536),
            "PrimitivesWithLatestRanges" => (Primitives: 536, SystemReactive: 416, R3: 536),
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

    private sealed class SmokeTeeTextWriter(TextWriter primary, TextWriter secondary) : TextWriter
    {
        public override Encoding Encoding => primary.Encoding;

        public override void Write(char value)
        {
            primary.Write(value);
            secondary.Write(value);
        }

        public override void Write(string? value)
        {
            primary.Write(value);
            secondary.Write(value);
        }

        public override void WriteLine(string? value)
        {
            primary.WriteLine(value);
            secondary.WriteLine(value);
        }
    }
}
