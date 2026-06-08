// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using BenchmarkDotNet.Attributes;
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
        var observer = new IntSignalObserver();
        using var state = new StateSignal<int>(0);
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
        var observer = new IntSignalObserver();
        using var state = new System.Reactive.Subjects.BehaviorSubject<int>(0);
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
        var observer = new IntR3Observer();
        using var state = new R3.BehaviorSubject<int>(0);
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
        using var state = new StateSignal<int>(Value);
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
        using var state = new System.Reactive.Subjects.BehaviorSubject<int>(Value);
        using var subscription = state.Map(static value => value + 1).Subscribe(value => current = value);
        state.OnNext(Value + 1);
        return current;
    }

    /// <summary>Benchmarks read-only state projection using R3.</summary>
    /// <returns>The current projected value.</returns>
    [Benchmark]
    public int R3ReadOnlyStateProjection()
    {
        using var state = new R3.BehaviorSubject<int>(Value);
        using var projected = R3.ReactivePropertyExtensions.ToReadOnlyReactiveProperty(
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
        var observer = new IntSignalObserver();
        using var signal = Signal.FromTask(static _ => Task.FromResult(Value), Sequencer.Immediate);
        using var subscription = signal.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks task-backed observable subscription using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveTaskSignalSubscribe()
    {
        var observer = new IntSignalObserver();
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
        var observer = new IntR3Observer();
        using var subscription = R3.Observable.ToObservable(Task.FromResult(Value), configureAwait: false)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks command execution.</summary>
    /// <returns>The command result.</returns>
    [Benchmark]
    public async Task<int> PrimitivesCommandExecuteAsync()
    {
        using var command = new CommandSignal<int>(static () => Value);
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
        using var command = new R3.ReactiveCommand<int>(value => result = value);
        command.Execute(Value);
        return result;
    }

    /// <summary>Benchmarks command result publication.</summary>
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

    /// <summary>Benchmarks command-result publication using System.Reactive subject semantics.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveCommandResultSubscribe()
    {
        var observer = new IntSignalObserver();
        using var results = new System.Reactive.Subjects.Subject<int>();
        using var subscription = results.Subscribe(observer);
        results.OnNext(Value);
        return observer.Total;
    }

    /// <summary>Benchmarks command-result publication using R3 subject semantics.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3CommandResultSubscribe()
    {
        var observer = new IntR3Observer();
        using var results = new R3.Subject<int>();
        using var subscription = results.Subscribe(observer);
        results.OnNext(Value);
        return observer.Total;
    }
}
