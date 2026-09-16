// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;
using RxTaskObservable = System.Reactive.Threading.Tasks.TaskObservableExtensions;
using RxTaskSubject = System.Reactive.Subjects.Subject<System.Threading.Tasks.Task<int>>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures bridging tasks into signals: async factories, pending task instances, async defer and task concatenation.</summary>
[MemoryDiagnoser]
public class TaskSignalBridgeBenchmarks
{
    /// <summary>The number of tasks each case bridges.</summary>
    private const int Count = 16;

    /// <summary>Benchmarks an async factory whose task completes after the subscription is made.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesFromAsyncPendingTask()
    {
        var total = 0;
        for (var i = 0; i < Count; i++)
        {
            TaskCompletionSource<int> source = new(TaskCreationOptions.RunContinuationsAsynchronously);
            CompletionWitness observer = new();
            using var subscription = Signal.FromAsync(() => source.Task).Subscribe(observer);
            source.SetResult(i);
            total += await observer.Completion.ConfigureAwait(false);
        }

        return total;
    }

    /// <summary>Benchmarks an async factory whose task completes after subscription using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public async Task<int> SystemReactiveFromAsyncPendingTask()
    {
        var total = 0;
        for (var i = 0; i < Count; i++)
        {
            TaskCompletionSource<int> source = new(TaskCreationOptions.RunContinuationsAsynchronously);
            CompletionWitness observer = new();
            using var subscription = RxObservable.FromAsync(() => source.Task, ImmediateScheduler.Instance).Subscribe(observer);
            source.SetResult(i);
            total += await observer.Completion.ConfigureAwait(false);
        }

        return total;
    }

    /// <summary>Benchmarks converting a pending task instance into a signal and completing it.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesPendingTaskToSignal()
    {
        var total = 0;
        for (var i = 0; i < Count; i++)
        {
            TaskCompletionSource<int> source = new(TaskCreationOptions.RunContinuationsAsynchronously);
            CompletionWitness observer = new();
            using var subscription = source.Task.ToSignal().Subscribe(observer);
            source.SetResult(i);
            total += await observer.Completion.ConfigureAwait(false);
        }

        return total;
    }

    /// <summary>Benchmarks converting a pending task instance into an observable using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public async Task<int> SystemReactivePendingTaskToObservable()
    {
        var total = 0;
        for (var i = 0; i < Count; i++)
        {
            TaskCompletionSource<int> source = new(TaskCreationOptions.RunContinuationsAsynchronously);
            CompletionWitness observer = new();
            using var subscription = RxTaskObservable.ToObservable(source.Task, ImmediateScheduler.Instance).Subscribe(observer);
            source.SetResult(i);
            total += await observer.Completion.ConfigureAwait(false);
        }

        return total;
    }

    /// <summary>Benchmarks a signal whose source is produced by an async factory for each subscription.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesDeferAsyncFactory()
    {
        var total = 0;
        for (var i = 0; i < Count; i++)
        {
            var inner = Signal.Emit(i);
            CompletionWitness observer = new();
            using var subscription = Signal.Defer(() => Task.FromResult(inner)).Subscribe(observer);
            total += await observer.Completion.ConfigureAwait(false);
        }

        return total;
    }

    /// <summary>Benchmarks concatenating task results in source order while the tasks are still pending.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesChainPendingTasks()
    {
        CompletionWitness observer = new();
        using Signal<Task<int>> tasks = new();
        using var subscription = tasks.Chain().Subscribe(observer);
        DriveTasks(tasks);
        return await observer.Completion.ConfigureAwait(false);
    }

    /// <summary>Benchmarks concatenating pending task results using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public async Task<int> SystemReactiveConcatPendingTasks()
    {
        CompletionWitness observer = new();
        using RxTaskSubject tasks = new();
        using var subscription = RxObservable.Concat(tasks).Subscribe(observer);
        DriveTasks(tasks);
        return await observer.Completion.ConfigureAwait(false);
    }

    /// <summary>Pushes pending tasks through the outer source, completes it, then completes the tasks in order.</summary>
    /// <param name="tasks">The outer task source.</param>
    private static void DriveTasks(IObserver<Task<int>> tasks)
    {
        var pending = new TaskCompletionSource<int>[Count];
        for (var i = 0; i < pending.Length; i++)
        {
            pending[i] = new(TaskCreationOptions.RunContinuationsAsynchronously);
            tasks.OnNext(pending[i].Task);
        }

        tasks.OnCompleted();
        for (var i = 0; i < pending.Length; i++)
        {
            pending[i].SetResult(i);
        }
    }

    /// <summary>Observer that totals values and exposes the total as a task once the sequence terminates.</summary>
    private sealed class CompletionWitness : IObserver<int>
    {
        /// <summary>Completes with the total when the sequence terminates.</summary>
        private readonly TaskCompletionSource<int> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The total of received values.</summary>
        private int _total;

        /// <summary>Gets the task that completes with the total once the sequence terminates.</summary>
        internal Task<int> Completion => _completion.Task;

        /// <inheritdoc/>
        public void OnNext(int value) => _total += value;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => _completion.TrySetException(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => _completion.TrySetResult(_total);
    }
}
