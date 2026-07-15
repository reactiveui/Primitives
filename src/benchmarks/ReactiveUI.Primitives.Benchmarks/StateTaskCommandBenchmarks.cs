// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using BenchmarkDotNet.Attributes;
using R3;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks for state, task, and command surfaces.</summary>
[MemoryDiagnoser]
public class StateTaskCommandBenchmarks
{
    /// <summary>The number of state updates performed by each benchmarked sequence.</summary>
    private const int Count = 32;

    /// <summary>The scalar value used by the single-value state, task, and command benchmarks.</summary>
    private const int Value = 42;

    /// <summary>Benchmarks state signal updates.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesStateSignalUpdates()
    {
        IntSignalWitness observer = new();
        using StateSignal<int> state = new(0);
        using var subscription = state.Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            state.Value = i;
        }

        return observer.Total;
    }

    /// <summary>Benchmarks state updates using System.Reactive behavior subject.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveStateSignalUpdates()
    {
        IntSignalWitness observer = new();
        using System.Reactive.Subjects.BehaviorSubject<int> state = new(0);
        using var subscription = state.Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            state.OnNext(i);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks state updates using R3 behavior subject.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3StateSignalUpdates()
    {
        IntR3Witness observer = new();
        using BehaviorSubject<int> state = new(0);
        using var subscription = state.Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            state.OnNext(i);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks read-only state projection.</summary>
    /// <returns>The current projected value.</returns>
    [Benchmark]
    public int PrimitivesReadOnlyStateProjection()
    {
        using StateSignal<int> state = new(Value);
        using var projected = state.ToReadOnlyState(static value => value + 1);
        state.Value = Value + 1;
        return projected.Value;
    }

    /// <summary>Benchmarks read-only state projection using System.Reactive.</summary>
    /// <returns>The current projected value.</returns>
    [Benchmark]
    public int SystemReactiveReadOnlyStateProjection()
    {
        var current = 0;
        using System.Reactive.Subjects.BehaviorSubject<int> state = new(Value);
        using var subscription = state.Map(static value => value + 1).Subscribe(value => current = value);
        state.OnNext(Value + 1);
        return current;
    }

    /// <summary>Benchmarks read-only state projection using R3.</summary>
    /// <returns>The current projected value.</returns>
    [Benchmark]
    public int R3ReadOnlyStateProjection()
    {
        using BehaviorSubject<int> state = new(Value);
        using var projected = ReactivePropertyExtensions.ToReadOnlyReactiveProperty(
            R3.ObservableExtensions.Select(state, static value => value + 1),
            Value + 1);
        state.OnNext(Value + 1);
        return projected.CurrentValue;
    }

    /// <summary>Benchmarks task signal subscription.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesTaskSignalSubscribe()
    {
        IntSignalWitness observer = new();
        using var signal = Signal.FromTask(static _ => Task.FromResult(Value), Sequencer.Immediate);
        using var subscription = signal.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks task-backed observable subscription using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveTaskSignalSubscribe()
    {
        IntSignalWitness observer = new();
        using var subscription = System.Reactive.Linq.Observable.FromAsync(
                static () => Task.FromResult(Value),
                ImmediateScheduler.Instance)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks task-backed observable subscription using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3TaskSignalSubscribe()
    {
        IntR3Witness observer = new();
        using var subscription = Observable.ToObservable(Task.FromResult(Value), false)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks command execution.</summary>
    /// <returns>The command result.</returns>
    [Benchmark]
    public async Task<int> PrimitivesCommandExecuteAsync()
    {
        using CommandSignal<int> command = new(static () => Value);
        return await command.ExecuteAsync().ConfigureAwait(false);
    }

    /// <summary>Benchmarks command-like execution using System.Reactive async factory semantics.</summary>
    /// <returns>The command result.</returns>
    [Benchmark]
    public Task<int> SystemReactiveCommandExecuteAsync() =>
        System.Reactive.Linq.Observable.Start(static () => Value, ImmediateScheduler.Instance).FirstAsync().ToTask();

    /// <summary>Benchmarks command execution using R3.</summary>
    /// <returns>The command result.</returns>
    [Benchmark]
    public int R3CommandExecute()
    {
        var result = 0;
        using ReactiveCommand<int> command = new(value => result = value);
        command.Execute(Value);
        return result;
    }

    /// <summary>Benchmarks command result publication.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesCommandResultSubscribeAsync()
    {
        IntSignalWitness observer = new();
        using CommandSignal<int> command = new(static () => Value);
        using var subscription = command.Results.Subscribe(observer);
        await command.ExecuteAsync().ConfigureAwait(false);
        return observer.Total;
    }

    /// <summary>Benchmarks command-result publication using System.Reactive subject semantics.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveCommandResultSubscribe()
    {
        IntSignalWitness observer = new();
        using System.Reactive.Subjects.Subject<int> results = new();
        using var subscription = results.Subscribe(observer);
        results.OnNext(Value);
        return observer.Total;
    }

    /// <summary>Benchmarks command-result publication using R3 subject semantics.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3CommandResultSubscribe()
    {
        IntR3Witness observer = new();
        using Subject<int> results = new();
        using var subscription = results.Subscribe(observer);
        results.OnNext(Value);
        return observer.Total;
    }
}
