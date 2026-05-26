// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using BenchmarkDotNet.Running;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Entry point for benchmark execution and smoke-test mode.
/// </summary>
internal static class Program
{
    /// <summary>
    /// Executes benchmarks, or runs a deterministic smoke check with <c>--smoke</c>.
    /// </summary>
    /// <param name="args">BenchmarkDotNet command-line arguments.</param>
    /// <returns>A task that completes when execution is finished.</returns>
    public static async Task Main(string[] args)
    {
        if (args.Contains("--smoke", StringComparer.OrdinalIgnoreCase))
        {
            await RunSmokeBenchmarksAsync();
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

        var selectMany = new OperatorSelectManyRangeBenchmarks();
        Console.WriteLine($"PrimitivesSelectManyRange={selectMany.PrimitivesSelectManyRange()}");
        Console.WriteLine($"SystemReactiveSelectManyRange={selectMany.SystemReactiveSelectManyRange()}");
        Console.WriteLine($"R3SelectManyRange={selectMany.R3SelectManyRange()}");

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
        Console.WriteLine($"PrimitivesBehaviourSignal32={stateful.PrimitivesBehaviourSignal32()}");
        Console.WriteLine($"SystemReactiveBehaviorSubject32={stateful.SystemReactiveBehaviorSubject32()}");
        Console.WriteLine($"R3BehaviorSubject32={stateful.R3BehaviorSubject32()}");
        Console.WriteLine($"PrimitivesBehaviourSignal1024={stateful.PrimitivesBehaviourSignal1024()}");
        Console.WriteLine($"SystemReactiveBehaviorSubject1024={stateful.SystemReactiveBehaviorSubject1024()}");
        Console.WriteLine($"R3BehaviorSubject1024={stateful.R3BehaviorSubject1024()}");

        var replay = new ReplaySignalBenchmarks();
        Console.WriteLine($"PrimitivesReplaySubscribe={replay.PrimitivesReplaySubscribe()}");
        Console.WriteLine($"SystemReactiveReplaySubscribe={replay.SystemReactiveReplaySubscribe()}");

        var taskBridge = new AsyncBridgeBenchmarks();
        Console.WriteLine($"PrimitivesCompletedTaskBridge={taskBridge.PrimitivesCompletedTaskBridge()}");
        Console.WriteLine($"SystemReactiveCompletedTaskBridge={taskBridge.SystemReactiveCompletedTaskBridge()}");

        await RunExpansionSmokeBenchmarksAsync();

        RunCoreRuntimeSmokeBenchmarks();
    }

    private static async Task RunExpansionSmokeBenchmarksAsync()
    {
        var factoryAdapters = new FactoryAdapterExpansionBenchmarks();
        Console.WriteLine($"PrimitivesCreateSubscribe={factoryAdapters.PrimitivesCreateSubscribe()}");
        Console.WriteLine($"PrimitivesDeferSubscribe={factoryAdapters.PrimitivesDeferSubscribe()}");
        Console.WriteLine(
            $"PrimitivesFromAsyncEnumerableSubscribe={await factoryAdapters.PrimitivesFromAsyncEnumerableSubscribeAsync()}");

        var timeSchedulers = new OperatorTimeSchedulerBenchmarks();
        Console.WriteLine($"PrimitivesDelayRange={timeSchedulers.PrimitivesDelayRange()}");
        Console.WriteLine($"PrimitivesThrottleBurst={timeSchedulers.PrimitivesThrottleBurst()}");
        Console.WriteLine($"PrimitivesTimeoutNever={timeSchedulers.PrimitivesTimeoutNever()}");

        var higherOrder = new OperatorHigherOrderBenchmarks();
        Console.WriteLine($"PrimitivesConcatRanges={higherOrder.PrimitivesConcatRanges()}");
        Console.WriteLine($"PrimitivesCombineLatestRanges={higherOrder.PrimitivesCombineLatestRanges()}");
        Console.WriteLine($"PrimitivesForkJoinRanges={higherOrder.PrimitivesForkJoinRanges()}");

        var terminalCollections = new TerminalCollectionBenchmarks();
        Console.WriteLine($"PrimitivesCollectList={terminalCollections.PrimitivesCollectList()}");
        Console.WriteLine($"PrimitivesFirstAsync={await terminalCollections.PrimitivesFirstAsync()}");
        Console.WriteLine($"PrimitivesAllContains={terminalCollections.PrimitivesAllContains()}");

        var connectableShare = new ConnectableShareBenchmarks();
        Console.WriteLine($"PrimitivesPublishLiveConnect={connectableShare.PrimitivesPublishLiveConnect()}");
        Console.WriteLine($"PrimitivesShareLiveSubscribe={connectableShare.PrimitivesShareLiveSubscribe()}");
        Console.WriteLine($"PrimitivesReplayLiveLateSubscribe={connectableShare.PrimitivesReplayLiveLateSubscribe()}");

        var stateTaskCommand = new StateTaskCommandBenchmarks();
        Console.WriteLine($"PrimitivesStateSignalUpdates={stateTaskCommand.PrimitivesStateSignalUpdates()}");
        Console.WriteLine($"PrimitivesTaskSignalSubscribe={stateTaskCommand.PrimitivesTaskSignalSubscribe()}");
        Console.WriteLine($"PrimitivesCommandExecute={await stateTaskCommand.PrimitivesCommandExecuteAsync()}");
    }

    private static void RunCoreRuntimeSmokeBenchmarks()
    {
        var coreRuntime = new CoreRuntimeBenchmarks();
        Console.WriteLine($"PrimitivesPocketDispose={coreRuntime.PrimitivesPocketDispose()}");
        Console.WriteLine($"SystemReactiveCompositeDispose={coreRuntime.SystemReactiveCompositeDispose()}");
        Console.WriteLine($"PrimitivesCurrentThreadSchedule={coreRuntime.PrimitivesCurrentThreadSchedule()}");
        Console.WriteLine(
            $"SystemReactiveCurrentThreadSchedule={coreRuntime.SystemReactiveCurrentThreadSchedule()}");
        Console.WriteLine($"PrimitivesSafeWitness={coreRuntime.PrimitivesSafeWitness()}");
        Console.WriteLine($"PrimitivesCompletedSpark={coreRuntime.PrimitivesCompletedSpark()}");
    }
}
