// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Concurrency;
using RxDisposable = System.Reactive.Disposables.Disposable;
using RxSynchronizationContextScheduler = System.Reactive.Concurrency.SynchronizationContextScheduler;
using RxTaskPoolScheduler = System.Reactive.Concurrency.TaskPoolScheduler;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures sequencers that hand work to a synchronization context or a task factory, both running it inline.</summary>
[MemoryDiagnoser]
public class ContextSequencerBenchmarks
{
    /// <summary>The number of work items scheduled per case.</summary>
    private const int Count = 1000;

    /// <summary>Benchmarks scheduling stateful work through a synchronization context.</summary>
    /// <returns>The number of work items executed.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesSynchronizationContextSchedule()
    {
        StrongBox<int> executed = new();
        SynchronizationContextSequencer sequencer = new(new InlineSynchronizationContext());
        for (var i = 0; i < Count; i++)
        {
            _ = sequencer.Schedule(executed, static box => box.Value++);
        }

        return executed.Value + (sequencer.Context is null ? 0 : 1) + (sequencer.Timestamp > 0 ? 1 : 0);
    }

    /// <summary>Benchmarks scheduling stateful work through a synchronization context using System.Reactive.</summary>
    /// <returns>The number of work items executed.</returns>
    [Benchmark]
    public int SystemReactiveSynchronizationContextSchedule()
    {
        StrongBox<int> executed = new();
        RxSynchronizationContextScheduler scheduler = new(new InlineSynchronizationContext());
        for (var i = 0; i < Count; i++)
        {
            _ = scheduler.Schedule(executed, static (_, box) =>
            {
                box.Value++;
                return RxDisposable.Empty;
            });
        }

        return executed.Value + 1 + (scheduler.Now > DateTimeOffset.MinValue ? 1 : 0);
    }

    /// <summary>Benchmarks scheduling stateful work through a task factory.</summary>
    /// <returns>The number of work items executed.</returns>
    [Benchmark]
    public int PrimitivesTaskPoolSchedule()
    {
        StrongBox<int> executed = new();
        TaskPoolSequencer sequencer = new(new TaskFactory(new InlineTaskScheduler())) { UnhandledExceptionHandler = static _ => { } };
        for (var i = 0; i < Count; i++)
        {
            _ = sequencer.Schedule(executed, static box => box.Value++);
        }

        return executed.Value + (sequencer.Now > DateTimeOffset.MinValue ? 1 : 0);
    }

    /// <summary>Benchmarks scheduling stateful work through a task factory using System.Reactive.</summary>
    /// <returns>The number of work items executed.</returns>
    [Benchmark]
    public int SystemReactiveTaskPoolSchedule()
    {
        StrongBox<int> executed = new();
        RxTaskPoolScheduler scheduler = new(new TaskFactory(new InlineTaskScheduler()));
        for (var i = 0; i < Count; i++)
        {
            _ = scheduler.Schedule(executed, static (_, box) =>
            {
                box.Value++;
                return RxDisposable.Empty;
            });
        }

        return executed.Value + (scheduler.Now > DateTimeOffset.MinValue ? 1 : 0);
    }

    /// <summary>Synchronization context that runs posted callbacks on the posting thread.</summary>
    private sealed class InlineSynchronizationContext : SynchronizationContext
    {
        /// <inheritdoc/>
        public override void Post(SendOrPostCallback d, object? state) => d(state);
    }

    /// <summary>Task scheduler that runs queued tasks on the queuing thread.</summary>
    private sealed class InlineTaskScheduler : TaskScheduler
    {
        /// <inheritdoc/>
        protected override IEnumerable<Task> GetScheduledTasks() => [];

        /// <inheritdoc/>
        protected override void QueueTask(Task task) => TryExecuteTask(task);

        /// <inheritdoc/>
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => TryExecuteTask(task);
    }
}
