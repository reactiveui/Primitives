// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Threading.Tasks;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using PackageContinuation = ReactiveUI.Extensions.Continuation;
using PackageExtensions = ReactiveUI.Extensions.ReactiveExtensions;
using PackageObservables = ReactiveUI.Extensions.Observables;
using PrimitivesContinuation = ReactiveUI.Primitives.Extensions.Continuation;
using PrimitivesExtensions = ReactiveUI.Primitives.Extensions.ReactiveExtensions;
using PrimitivesObservables = ReactiveUI.Primitives.Extensions.Observables;
using RxIntSubject = System.Reactive.Subjects.Subject<int>;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures task-backed projection, subscription, handoff and first-value bridges with synchronously completed work.</summary>
[MemoryDiagnoser]
public class ExtensionAsyncProjectionBenchmarks
{
    /// <summary>The number of values or operations in each case.</summary>
    private const int Count = 256;

    /// <summary>The concurrency cap used by the bounded cases.</summary>
    private const int MaxConcurrency = 4;

    /// <summary>Projects every value through a task, preserving source order.</summary>
    /// <returns>The sum of the projected values.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesSelectAsyncSequential()
    {
        CompletionWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.SelectAsyncSequential(source, Increment).Subscribe(observer);
        PushRangeAndComplete(source);
        return await observer.Completion.ConfigureAwait(false);
    }

    /// <summary>Projects every value through a task, preserving source order, using System.Reactive.</summary>
    /// <returns>The sum of the projected values.</returns>
    [Benchmark]
    public async Task<int> SystemReactiveSelectAsyncSequential()
    {
        CompletionWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = RxObservable.Concat(RxObservable.Select(source, static value => RxObservable.FromAsync(() => Increment(value))))
            .Subscribe(observer);
        PushRangeAndComplete(source);
        return await observer.Completion.ConfigureAwait(false);
    }

    /// <summary>Projects every value through a task, preserving source order, using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the projected values.</returns>
    [Benchmark]
    public async Task<int> ReactiveUIExtensionsSelectAsyncSequential()
    {
        CompletionWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.SelectAsyncSequential(source, Increment).Subscribe(observer);
        PushRangeAndComplete(source);
        return await observer.Completion.ConfigureAwait(false);
    }

    /// <summary>Projects every value through a task under a concurrency cap.</summary>
    /// <returns>The sum of the projected values.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSelectAsyncConcurrent()
    {
        CompletionWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.SelectAsyncConcurrent(source, Increment, MaxConcurrency).Subscribe(observer);
        PushRangeAndComplete(source);
        return await observer.Completion.ConfigureAwait(false);
    }

    /// <summary>Projects every value through a task under a concurrency cap using System.Reactive.</summary>
    /// <returns>The sum of the projected values.</returns>
    [Benchmark]
    public async Task<int> SystemReactiveSelectAsyncConcurrent()
    {
        CompletionWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = RxObservable.Merge(
                RxObservable.Select(source, static value => RxObservable.FromAsync(() => Increment(value))),
                MaxConcurrency)
            .Subscribe(observer);
        PushRangeAndComplete(source);
        return await observer.Completion.ConfigureAwait(false);
    }

    /// <summary>Projects every value through a task under a concurrency cap using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the projected values.</returns>
    [Benchmark]
    public async Task<int> ReactiveUIExtensionsSelectAsyncConcurrent()
    {
        CompletionWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.SelectAsyncConcurrent(source, Increment, MaxConcurrency).Subscribe(observer);
        PushRangeAndComplete(source);
        return await observer.Completion.ConfigureAwait(false);
    }

    /// <summary>Projects every value through a task, keeping only the latest projection.</summary>
    /// <returns>The sum of the emitted projections.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSelectLatestAsync()
    {
        CompletionWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.SelectLatestAsync(source, Increment).Subscribe(observer);
        PushRangeAndComplete(source);
        return await observer.Completion.ConfigureAwait(false);
    }

    /// <summary>Projects every value through a task, keeping only the latest projection, using System.Reactive.</summary>
    /// <returns>The sum of the emitted projections.</returns>
    [Benchmark]
    public async Task<int> SystemReactiveSelectLatestAsync()
    {
        CompletionWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = RxObservable.Switch(RxObservable.Select(source, static value => RxObservable.FromAsync(() => Increment(value))))
            .Subscribe(observer);
        PushRangeAndComplete(source);
        return await observer.Completion.ConfigureAwait(false);
    }

    /// <summary>Projects every value through a task, keeping only the latest projection, using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the emitted projections.</returns>
    [Benchmark]
    public async Task<int> ReactiveUIExtensionsSelectLatestAsync()
    {
        CompletionWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.SelectLatestAsync(source, Increment).Subscribe(observer);
        PushRangeAndComplete(source);
        return await observer.Completion.ConfigureAwait(false);
    }

    /// <summary>Runs a value-task action per value, dropping values that arrive while it is busy.</summary>
    /// <returns>The sum of the forwarded values.</returns>
    [Benchmark]
    public async Task<int> PrimitivesDropIfBusy()
    {
        CompletionWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.DropIfBusy(source, static _ => default).Subscribe(observer);
        PushRangeAndComplete(source);
        return await observer.Completion.ConfigureAwait(false);
    }

    /// <summary>Runs a value-task action per value, dropping values that arrive while it is busy, using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the forwarded values.</returns>
    [Benchmark]
    public async Task<int> ReactiveUIExtensionsDropIfBusy()
    {
        CompletionWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.DropIfBusy(source, static _ => default).Subscribe(observer);
        PushRangeAndComplete(source);
        return await observer.Completion.ConfigureAwait(false);
    }

    /// <summary>Runs a queued value-task handler per value and waits for the completion callback.</summary>
    /// <returns>The sum of the handled values.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSubscribeAsync()
    {
        AsyncHandler handler = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.SubscribeAsync(source, handler.HandleAsync, handler.OnError, handler.OnCompleted);
        PushRangeAndComplete(source);
        return await handler.Completion.ConfigureAwait(false);
    }

    /// <summary>Runs a queued value-task handler per value and waits for the completion callback using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the handled values.</returns>
    [Benchmark]
    public async Task<int> ReactiveUIExtensionsSubscribeAsync()
    {
        AsyncHandler handler = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.SubscribeAsync(source, handler.HandleAsync, handler.OnError, handler.OnCompleted);
        PushRangeAndComplete(source);
        return await handler.Completion.ConfigureAwait(false);
    }

    /// <summary>Pairs each value with an acknowledgement handle that the observer releases.</summary>
    /// <returns>The sum of the acknowledged values.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSynchronizeAsync()
    {
        AcknowledgingWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.SynchronizeAsync(source).Subscribe(observer);
        PushRangeAndComplete(source);
        return await observer.Completion.ConfigureAwait(false);
    }

    /// <summary>Pairs each value with an acknowledgement handle that the observer releases, using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the acknowledged values.</returns>
    [Benchmark]
    public async Task<int> ReactiveUIExtensionsSynchronizeAsync()
    {
        AcknowledgingWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.SynchronizeAsync(source).Subscribe(observer);
        PushRangeAndComplete(source);
        return await observer.Completion.ConfigureAwait(false);
    }

    /// <summary>Hands one item per continuation to an observer that releases it, awaiting each handoff.</summary>
    /// <returns>The number of completed handoffs.</returns>
    [Benchmark]
    public async Task<int> PrimitivesContinuationHandoff()
    {
        AcknowledgingWitness observer = new();
        var completed = 0;
        for (var i = 0; i < Count; i++)
        {
            using PrimitivesContinuation continuation = new();
            if ((i & 1) == 0)
            {
                await continuation.Lock(i, observer).ConfigureAwait(false);
            }
            else
            {
                await continuation.LockValueTask(i, observer).ConfigureAwait(false);
            }

            completed += (int)continuation.CompletedPhases;
        }

        return completed + observer.Total;
    }

    /// <summary>Hands one item per continuation to an observer that releases it, awaiting each handoff, using ReactiveUI.Extensions.</summary>
    /// <returns>The number of completed handoffs.</returns>
    [Benchmark]
    public async Task<int> ReactiveUIExtensionsContinuationHandoff()
    {
        AcknowledgingWitness observer = new();
        var completed = 0;
        for (var i = 0; i < Count; i++)
        {
            using PackageContinuation continuation = new();
            if ((i & 1) == 0)
            {
                await continuation.Lock(i, observer).ConfigureAwait(false);
            }
            else
            {
                await continuation.LockValueTask(i, observer).ConfigureAwait(false);
            }

            completed += (int)continuation.CompletedPhases;
        }

        return completed + observer.Total;
    }

    /// <summary>Drains a sequence of completed tasks under a concurrency cap.</summary>
    /// <returns>The sum of the task results.</returns>
    [Benchmark]
    public async Task<int> PrimitivesWithLimitedConcurrency()
    {
        CompletionWitness observer = new();
        using var subscription = PrimitivesExtensions.WithLimitedConcurrency(CompletedTasks(), MaxConcurrency).Subscribe(observer);
        return await observer.Completion.ConfigureAwait(false);
    }

    /// <summary>Drains a sequence of completed tasks under a concurrency cap using System.Reactive.</summary>
    /// <returns>The sum of the task results.</returns>
    [Benchmark]
    public async Task<int> SystemReactiveWithLimitedConcurrency()
    {
        CompletionWitness observer = new();
        using var subscription = RxObservable.Merge(
                RxObservable.Select(RxObservable.ToObservable(CompletedTasks()), TaskObservableExtensions.ToObservable),
                MaxConcurrency)
            .Subscribe(observer);
        return await observer.Completion.ConfigureAwait(false);
    }

    /// <summary>Drains a sequence of completed tasks under a concurrency cap using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the task results.</returns>
    [Benchmark]
    public async Task<int> ReactiveUIExtensionsWithLimitedConcurrency()
    {
        CompletionWitness observer = new();
        using var subscription = PackageExtensions.WithLimitedConcurrency(CompletedTasks(), MaxConcurrency).Subscribe(observer);
        return await observer.Completion.ConfigureAwait(false);
    }

    /// <summary>Bridges the first value of single-value sources to tasks and awaits each.</summary>
    /// <returns>The sum of the first values.</returns>
    [Benchmark]
    public async Task<int> PrimitivesToHotTask()
    {
        var total = 0;
        for (var i = 0; i < Count; i++)
        {
            total += await PrimitivesExtensions.ToHotTask(PrimitivesObservables.Return(i)).ConfigureAwait(false);
        }

        return total;
    }

    /// <summary>Bridges the first value of single-value sources to tasks and awaits each using System.Reactive.</summary>
    /// <returns>The sum of the first values.</returns>
    [Benchmark]
    public async Task<int> SystemReactiveToHotTask()
    {
        var total = 0;
        for (var i = 0; i < Count; i++)
        {
            total += await TaskObservableExtensions.ToTask(RxObservable.FirstAsync(RxObservable.Return(i))).ConfigureAwait(false);
        }

        return total;
    }

    /// <summary>Bridges the first value of single-value sources to tasks and awaits each using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the first values.</returns>
    [Benchmark]
    public async Task<int> ReactiveUIExtensionsToHotTask()
    {
        var total = 0;
        for (var i = 0; i < Count; i++)
        {
            total += await PackageExtensions.ToHotTask(PackageObservables.Return(i)).ConfigureAwait(false);
        }

        return total;
    }

    /// <summary>Bridges the first value of single-value sources to pooled value tasks and awaits each.</summary>
    /// <returns>The sum of the first values.</returns>
    [Benchmark]
    public async Task<int> PrimitivesToHotValueTask()
    {
        var total = 0;
        for (var i = 0; i < Count; i++)
        {
            total += await PrimitivesExtensions.ToHotValueTask(PrimitivesObservables.Return(i)).ConfigureAwait(false);
        }

        return total;
    }

    /// <summary>Bridges the first value of single-value sources to value tasks and awaits each using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the first values.</returns>
    [Benchmark]
    public async Task<int> ReactiveUIExtensionsToHotValueTask()
    {
        var total = 0;
        for (var i = 0; i < Count; i++)
        {
            total += await PackageExtensions.ToHotValueTask(PackageObservables.Return(i)).ConfigureAwait(false);
        }

        return total;
    }

    /// <summary>Projects a value to its successor through a completed task.</summary>
    /// <param name="value">The value to project.</param>
    /// <returns>A completed task carrying the successor.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<int> Increment(int value) => Task.FromResult(value + 1);

    /// <summary>Pushes the ascending range into the subject and completes it.</summary>
    /// <param name="source">The subject to push into.</param>
    private static void PushRangeAndComplete(IObserver<int> source)
    {
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        source.OnCompleted();
    }

    /// <summary>Yields completed tasks carrying ascending values.</summary>
    /// <returns>A lazy sequence of completed tasks.</returns>
    private static IEnumerable<Task<int>> CompletedTasks()
    {
        for (var i = 0; i < Count; i++)
        {
            yield return Task.FromResult(i);
        }
    }

    /// <summary>Observer that sums values and completes a task with the sum when the sequence ends.</summary>
    private sealed class CompletionWitness : IObserver<int>
    {
        /// <summary>The completion source settled by the terminal notification.</summary>
        private readonly TaskCompletionSource<int> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The running sum of observed values.</summary>
        private int _total;

        /// <summary>Gets the task that completes with the sum when the sequence completes.</summary>
        public Task<int> Completion => _completion.Task;

        /// <inheritdoc/>
        public void OnNext(int value) => _total += value;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => _completion.TrySetException(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => _completion.TrySetResult(_total);
    }

    /// <summary>Observer that sums values, releases each acknowledgement handle and completes a task on termination.</summary>
    private sealed class AcknowledgingWitness : IObserver<(int Value, IDisposable Sync)>
    {
        /// <summary>The completion source settled by the terminal notification.</summary>
        private readonly TaskCompletionSource<int> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the running sum of observed values.</summary>
        public int Total { get; private set; }

        /// <summary>Gets the task that completes with the sum when the sequence completes.</summary>
        public Task<int> Completion => _completion.Task;

        /// <inheritdoc/>
        public void OnNext((int Value, IDisposable Sync) value)
        {
            Total += value.Value;
            value.Sync.Dispose();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => _completion.TrySetException(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => _completion.TrySetResult(Total);
    }

    /// <summary>Asynchronous subscription callbacks that sum values and complete a task on termination.</summary>
    private sealed class AsyncHandler
    {
        /// <summary>The completion source settled by the terminal callback.</summary>
        private readonly TaskCompletionSource<int> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The running sum of handled values.</summary>
        private int _total;

        /// <summary>Gets the task that completes with the sum when the sequence completes.</summary>
        public Task<int> Completion => _completion.Task;

        /// <summary>Adds the value to the running sum.</summary>
        /// <param name="value">The value to handle.</param>
        /// <returns>A completed value task.</returns>
        public ValueTask HandleAsync(int value)
        {
            _total += value;
            return default;
        }

        /// <summary>Faults the completion task.</summary>
        /// <param name="error">The error.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => _completion.TrySetException(error);

        /// <summary>Completes the completion task with the sum.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => _completion.TrySetResult(_total);
    }
}
