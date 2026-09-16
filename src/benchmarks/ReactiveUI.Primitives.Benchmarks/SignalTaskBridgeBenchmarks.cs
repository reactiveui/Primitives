// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures the bridges between signals and tasks or enumerables: awaiting, blocking enumeration, cancellation handling and task-backed signals.</summary>
[MemoryDiagnoser]
public class SignalTaskBridgeBenchmarks
{
    /// <summary>The number of values in each source range.</summary>
    private const int Count = 64;

    /// <summary>A token that is already cancelled.</summary>
    private static readonly CancellationToken Cancelled = new(true);

    /// <summary>Awaits the last value of a range, with and without a cancellation token.</summary>
    /// <returns>The sum of the awaited values.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesAwaitLastValue()
    {
        using CancellationTokenSource cts = new();
        var first = await Signal.Range(1, Count);
        var second = await Signal.Range(1, Count).GetAwaiter(cts.Token);
        return first + second;
    }

    /// <summary>Awaits the last value of a range, with and without a cancellation token, using System.Reactive.</summary>
    /// <returns>The sum of the awaited values.</returns>
    [Benchmark]
    public async Task<int> SystemReactiveAwaitLastValue()
    {
        using CancellationTokenSource cts = new();
        var first = await RxObservable.GetAwaiter(RxObservable.Range(1, Count, ImmediateScheduler.Instance));
        var second = await RxObservable.RunAsync(RxObservable.Range(1, Count, ImmediateScheduler.Instance), cts.Token);
        return first + second;
    }

    /// <summary>Enumerates a completed range through the blocking enumerable bridge.</summary>
    /// <returns>The total enumerated.</returns>
    [Benchmark]
    public int PrimitivesToEnumerable()
    {
        var total = 0;
        foreach (var value in Signal.Range(1, Count).ToEnumerable())
        {
            total += value;
        }

        return total;
    }

    /// <summary>Enumerates a completed range through the blocking enumerable bridge using System.Reactive.</summary>
    /// <returns>The total enumerated.</returns>
    [Benchmark]
    public int SystemReactiveToEnumerable()
    {
        var total = 0;
        foreach (var value in RxObservable.ToEnumerable(RxObservable.Range(1, Count, ImmediateScheduler.Instance)))
        {
            total += value;
        }

        return total;
    }

    /// <summary>Awaits completed and cancelled tasks and a cancelled signal wait through the cancellation-handling bridge.</summary>
    /// <returns>The sum of the results plus the number of cancellation callbacks.</returns>
    [Benchmark]
    public async Task<int> PrimitivesHandleCancellation()
    {
        var cancellations = 0;
        var result = await Task.FromResult(Count).HandleCancellation();
        result += await Task.FromCanceled<int>(Cancelled).HandleCancellation(() => cancellations++);
        await Task.CompletedTask.HandleCancellation();
        await Task.FromCanceled(Cancelled).HandleCancellation(() => cancellations++);
        result += await Signal.Range(1, Count).HandleCancellation(() => cancellations++, Cancelled);
        result += await Signal.Range(1, Count).HandleCancellation(Cancelled);
        return result + cancellations;
    }

    /// <summary>Creates task-backed signals over a range and subscribes on the current-thread and immediate sequencers.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int PrimitivesTaskSignalSubscribe()
    {
        IntSignalWitness observer = new();
        using CancellationTokenSource cts = new();
        using var trampolined = TaskSignal.Create<int>(static _ => Signal.Range(1, Count));
        using var immediate = TaskSignal.Create<int>(static _ => Signal.Range(1, Count), Sequencer.Immediate);
        using var shared = TaskSignal.Create<int>(static _ => Signal.Range(1, Count), Sequencer.Immediate, cts);
        _ = trampolined.Subscribe(observer);
        _ = immediate.Subscribe(observer);
        _ = shared.Subscribe(observer);
        return observer.Total;
    }
}
