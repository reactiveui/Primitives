// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks for state, task, and command surfaces.
/// </summary>
[MemoryDiagnoser]
public class StateTaskCommandBenchmarks
{
    private const int Count = 32;
    private const int Value = 42;

    /// <summary>
    /// Benchmarks state signal updates.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesStateSignalUpdates()
    {
        var observer = new IntSignalObserver();
        using var state = new StateSignal<int>(0);
        using var subscription = state.Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            state.Value = i;
        }

        return observer.Total;
    }

    /// <summary>
    /// Benchmarks read-only state projection.
    /// </summary>
    /// <returns>The current projected value.</returns>
    [Benchmark]
    public int PrimitivesReadOnlyStateProjection()
    {
        using var state = new StateSignal<int>(Value);
        using var projected = state.ToReadOnlyState(static value => value + 1);
        state.Value = Value + 1;
        return projected.Value;
    }

    /// <summary>
    /// Benchmarks task signal subscription.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesTaskSignalSubscribe()
    {
        var observer = new IntSignalObserver();
        using var signal = Signal.FromTask(static _ => Task.FromResult(Value), Sequencer.Immediate);
        using var subscription = signal.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks command execution.
    /// </summary>
    /// <returns>The command result.</returns>
    [Benchmark]
    public async Task<int> PrimitivesCommandExecuteAsync()
    {
        using var command = new CommandSignal<int>(static () => Value);
        return await command.ExecuteAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Benchmarks command result publication.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesCommandResultSubscribeAsync()
    {
        var observer = new IntSignalObserver();
        using var command = new CommandSignal<int>(static () => Value);
        using var subscription = command.Results.Subscribe(observer);
        await command.ExecuteAsync().ConfigureAwait(false);
        return observer.Total;
    }
}
