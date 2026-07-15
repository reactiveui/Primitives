// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Runs every benchmark scenario once and writes each result as a <c>key=value</c> row, so the
/// libraries can be compared for parity by <see cref="SmokeParityValidator"/>. Row order is
/// significant: the validator groups consecutive rows that share a scenario name.
/// </summary>
internal static class SmokeBenchmarkRunner
{
    /// <summary>Runs the deterministic smoke benchmark scenarios and writes their results to the console.</summary>
    /// <returns>A task that completes when all smoke benchmarks have run.</returns>
    public static async Task RunAsync()
    {
        RunSignalFactorySmoke();
        await RunOperatorSmokeAsync();
        RunSubjectSmoke();
        RunAsyncBridgeSmoke();
        await RunExpansionSmokeBenchmarksAsync();
        RunCoreRuntimeSmokeBenchmarks();
    }

    /// <summary>Runs the scalar, factory, and enumerable-source smoke benchmarks.</summary>
    private static void RunSignalFactorySmoke()
    {
        ScalarSignalBenchmarks scalar = new();
        Console.WriteLine($"PrimitivesReturnSubscribe={scalar.PrimitivesReturnSubscribe()}");
        Console.WriteLine($"SystemReactiveReturnSubscribe={scalar.SystemReactiveReturnSubscribe()}");
        Console.WriteLine($"R3ReturnSubscribe={scalar.R3ReturnSubscribe()}");

        FactorySignalBenchmarks factory = new();
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

        FactoryFromEnumerableBenchmarks fromEnumerable = new();
        Console.WriteLine($"PrimitivesFromEnumerableSubscribe={fromEnumerable.PrimitivesFromEnumerableSubscribe()}");
        Console.WriteLine(
            $"SystemReactiveToObservableSubscribe={fromEnumerable.SystemReactiveToObservableSubscribe()}");
        Console.WriteLine($"R3ToObservableSubscribe={fromEnumerable.R3ToObservableSubscribe()}");
    }

    /// <summary>Runs the projection, start-with, flat-map, and zip operator smoke benchmarks.</summary>
    /// <returns>A task that completes when the operator smoke benchmarks have run.</returns>
    private static async Task RunOperatorSmokeAsync()
    {
        OperatorMapKeepBenchmarks operators = new();
        Console.WriteLine($"PrimitivesRangeMapKeep={operators.PrimitivesRangeMapKeep()}");
        Console.WriteLine($"SystemReactiveRangeSelectWhere={operators.SystemReactiveRangeSelectWhere()}");
        Console.WriteLine($"R3RangeSelectWhere={operators.R3RangeSelectWhere()}");
        Console.WriteLine($"PrimitivesAggregateAnyCount={operators.PrimitivesAggregateAnyCount()}");
        Console.WriteLine($"SystemReactiveAggregateAnyCount={operators.SystemReactiveAggregateAnyCount()}");
        Console.WriteLine($"R3AggregateAnyCount={await operators.R3AggregateAnyCount()}");

        OperatorStartWithAppendDefaultIfEmptyBenchmarks startWith = new();
        Console.WriteLine(
            $"PrimitivesStartWithAppendDefaultIfEmpty={startWith.PrimitivesStartWithAppendDefaultIfEmpty()}");
        Console.WriteLine(
            $"SystemReactiveStartWithAppendDefaultIfEmpty={startWith.SystemReactiveStartWithAppendDefaultIfEmpty()}");
        Console.WriteLine(
            $"R3PrependAppendDefaultIfEmpty={startWith.R3PrependAppendDefaultIfEmpty()}");
        Console.WriteLine($"PrimitivesDefaultIfEmptyEmpty={startWith.PrimitivesDefaultIfEmptyEmpty()}");
        Console.WriteLine($"SystemReactiveDefaultIfEmptyEmpty={startWith.SystemReactiveDefaultIfEmptyEmpty()}");
        Console.WriteLine($"R3DefaultIfEmptyEmpty={startWith.R3DefaultIfEmptyEmpty()}");

        OperatorFlatMapRangeBenchmarks flatMap = new();
        Console.WriteLine($"PrimitivesFlatMapRange={flatMap.PrimitivesFlatMapRange()}");
        Console.WriteLine($"SystemReactiveSelectManyRange={flatMap.SystemReactiveSelectManyRange()}");
        Console.WriteLine($"R3SelectManyRange={flatMap.R3SelectManyRange()}");

        OperatorZipBenchmarks zip = new();
        Console.WriteLine($"PrimitivesZip={zip.PrimitivesZip()}");
        Console.WriteLine($"SystemReactiveZip={zip.SystemReactiveZip()}");
        Console.WriteLine($"R3Zip={zip.R3Zip()}");
    }

    /// <summary>Runs the subject throughput, subscription, state, and replay smoke benchmarks.</summary>
    private static void RunSubjectSmoke()
    {
        SubjectThroughputBenchmarks throughput = new();
        Console.WriteLine($"PrimitivesSubjectEmitN32={throughput.PrimitivesSubjectEmit32()}");
        Console.WriteLine($"SystemReactiveSubjectEmitN32={throughput.SystemReactiveSubjectEmit32()}");
        Console.WriteLine($"R3SubjectEmitN32={throughput.R3SubjectEmit32()}");
        Console.WriteLine($"PrimitivesSubjectEmitN1024={throughput.PrimitivesSubjectEmit1024()}");
        Console.WriteLine($"SystemReactiveSubjectEmitN1024={throughput.SystemReactiveSubjectEmit1024()}");
        Console.WriteLine($"R3SubjectEmitN1024={throughput.R3SubjectEmit1024()}");

        SubjectSubscriptionBenchmarks subscriptions = new();
        Console.WriteLine($"PrimitivesSubjectSubscribeDispose8={subscriptions.PrimitivesSubjectSubscribeDispose8()}");
        Console.WriteLine(
            $"SystemReactiveSubjectSubscribeDispose8={subscriptions.SystemReactiveSubjectSubscribeDispose8()}");
        Console.WriteLine($"R3SubjectSubscribeDispose8={subscriptions.R3SubjectSubscribeDispose8()}");
        Console.WriteLine($"PrimitivesSubjectSubscribeDispose64={subscriptions.PrimitivesSubjectSubscribeDispose64()}");
        Console.WriteLine(
            $"SystemReactiveSubjectSubscribeDispose64={subscriptions.SystemReactiveSubjectSubscribeDispose64()}");
        Console.WriteLine($"R3SubjectSubscribeDispose64={subscriptions.R3SubjectSubscribeDispose64()}");

        StatefulSignalBenchmarks stateful = new();
        Console.WriteLine($"PrimitivesStateSignal32={stateful.PrimitivesStateSignal32()}");
        Console.WriteLine($"SystemReactiveBehaviorSubject32={stateful.SystemReactiveBehaviorSubject32()}");
        Console.WriteLine($"R3BehaviorSubject32={stateful.R3BehaviorSubject32()}");
        Console.WriteLine($"PrimitivesStateSignal1024={stateful.PrimitivesStateSignal1024()}");
        Console.WriteLine($"SystemReactiveBehaviorSubject1024={stateful.SystemReactiveBehaviorSubject1024()}");
        Console.WriteLine($"R3BehaviorSubject1024={stateful.R3BehaviorSubject1024()}");

        ReplaySignalBenchmarks history = new();
        Console.WriteLine($"PrimitivesHistorySubscribe={history.PrimitivesHistorySubscribe()}");
        Console.WriteLine($"SystemReactiveReplaySubscribe={history.SystemReactiveReplaySubscribe()}");
        Console.WriteLine($"R3ReplaySubscribe={history.R3ReplaySubscribe()}");
    }

    /// <summary>Runs the completed-task bridge smoke benchmarks.</summary>
    private static void RunAsyncBridgeSmoke()
    {
        AsyncBridgeBenchmarks taskBridge = new();
        Console.WriteLine($"PrimitivesCompletedTaskBridge={taskBridge.PrimitivesCompletedTaskBridge()}");
        Console.WriteLine($"SystemReactiveCompletedTaskBridge={taskBridge.SystemReactiveCompletedTaskBridge()}");
        Console.WriteLine($"R3CompletedTaskBridge={taskBridge.R3CompletedTaskBridge()}");
    }

    /// <summary>Runs the expansion-coverage smoke benchmark scenarios and writes their results to the console.</summary>
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

    /// <summary>Runs the factory-adapter expansion smoke benchmarks and writes their results to the console.</summary>
    /// <returns>A task that completes when the factory-adapter smoke benchmarks have run.</returns>
    private static async Task RunFactoryAdapterExpansionSmokeAsync()
    {
        FactoryAdapterExpansionBenchmarks factoryAdapters = new();
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
        Console.WriteLine(
            $"SystemReactiveNeverSubscribeDispose={factoryAdapters.SystemReactiveNeverSubscribeDispose()}");
        Console.WriteLine($"R3NeverSubscribeDispose={factoryAdapters.R3NeverSubscribeDispose()}");
    }

    /// <summary>Runs the time and scheduler operator smoke benchmarks and writes their results to the console.</summary>
    private static void RunTimeSchedulerSmoke()
    {
        OperatorTimeSchedulerBenchmarks timeSchedulers = new();
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

    /// <summary>Runs the higher-order operator smoke benchmarks and writes their results to the console.</summary>
    private static void RunHigherOrderSmoke()
    {
        OperatorHigherOrderBenchmarks higherOrder = new();
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

    /// <summary>Runs the terminal-collection smoke benchmarks and writes their results to the console.</summary>
    /// <returns>A task that completes when the terminal-collection smoke benchmarks have run.</returns>
    private static async Task RunTerminalCollectionSmokeAsync()
    {
        TerminalCollectionBenchmarks terminalCollections = new();
        Console.WriteLine($"PrimitivesCollectList={terminalCollections.PrimitivesCollectList()}");
        Console.WriteLine($"SystemReactiveCollectList={terminalCollections.SystemReactiveCollectList()}");
        Console.WriteLine($"R3CollectList={await terminalCollections.R3CollectList()}");
        WriteSynchronousCollectArrayResults(terminalCollections);
        Console.WriteLine($"R3CollectArray={await terminalCollections.R3CollectArray()}");
        Console.WriteLine($"PrimitivesCollectArrayAsync={await terminalCollections.PrimitivesCollectArrayAsync()}");
        Console.WriteLine(
            $"SystemReactiveCollectArrayAsync={await terminalCollections.SystemReactiveCollectArrayAsync()}");
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

    /// <summary>Runs the connectable/share smoke benchmarks and writes their results to the console.</summary>
    private static void RunConnectableShareSmoke()
    {
        ConnectableShareBenchmarks connectableShare = new();
        Console.WriteLine($"PrimitivesPublishLiveConnect={connectableShare.PrimitivesPublishLiveConnect()}");
        Console.WriteLine($"SystemReactivePublishLiveConnect={connectableShare.SystemReactivePublishLiveConnect()}");
        Console.WriteLine($"R3PublishLiveConnect={connectableShare.R3PublishLiveConnect()}");
        Console.WriteLine($"PrimitivesShareLiveSubscribe={connectableShare.PrimitivesShareLiveSubscribe()}");
        Console.WriteLine($"SystemReactiveShareLiveSubscribe={connectableShare.SystemReactiveShareLiveSubscribe()}");
        Console.WriteLine($"R3ShareLiveSubscribe={connectableShare.R3ShareLiveSubscribe()}");
        Console.WriteLine($"PrimitivesReplayLiveLateSubscribe={connectableShare.PrimitivesReplayLiveLateSubscribe()}");
        Console.WriteLine(
            $"SystemReactiveReplayLiveLateSubscribe={connectableShare.SystemReactiveReplayLiveLateSubscribe()}");
        Console.WriteLine($"R3ReplayLiveLateSubscribe={connectableShare.R3ReplayLiveLateSubscribe()}");
        Console.WriteLine($"PrimitivesRefCountSubscribe={connectableShare.PrimitivesRefCountSubscribe()}");
        Console.WriteLine($"R3RefCountSubscribe={connectableShare.R3RefCountSubscribe()}");
        Console.WriteLine($"PrimitivesAutoConnectSubscribe={connectableShare.PrimitivesAutoConnectSubscribe()}");
        Console.WriteLine(
            $"SystemReactiveAutoConnectSubscribe={connectableShare.SystemReactiveAutoConnectSubscribe()}");
    }

    /// <summary>Runs the state, task, and command smoke benchmarks and writes their results to the console.</summary>
    /// <returns>A task that completes when the state/task/command smoke benchmarks have run.</returns>
    private static async Task RunStateTaskCommandSmokeAsync()
    {
        StateTaskCommandBenchmarks stateTaskCommand = new();
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
        Console.WriteLine(
            $"SystemReactiveCommandResultSubscribe={stateTaskCommand.SystemReactiveCommandResultSubscribe()}");
        Console.WriteLine($"R3CommandResultSubscribe={stateTaskCommand.R3CommandResultSubscribe()}");
    }

    /// <summary>Runs the core-runtime smoke benchmark scenarios and writes their results to the console.</summary>
    private static void RunCoreRuntimeSmokeBenchmarks()
    {
        CoreRuntimeBenchmarks coreRuntime = new();
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
}
