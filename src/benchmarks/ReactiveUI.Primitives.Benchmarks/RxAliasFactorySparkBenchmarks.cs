// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures the System.Reactive-named factory aliases and notification materialization with formatting.</summary>
[MemoryDiagnoser]
public class RxAliasFactorySparkBenchmarks
{
    /// <summary>The number of subscriptions or values per case.</summary>
    private const int Count = 64;

    /// <summary>The value returned by the first source.</summary>
    private const int FirstValue = 1;

    /// <summary>The value returned by the second source.</summary>
    private const int SecondValue = 2;

    /// <summary>The value returned by the then-only conditional source.</summary>
    private const int ThenValue = 3;

    /// <summary>The failure materialized by the error cases.</summary>
    private static readonly InvalidOperationException Failure = new("failure");

    /// <summary>Subscribes deferred, conditional and keyed factories once per iteration.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesDeferIfCase()
    {
        IntSignalWitness observer = new();
        Dictionary<int, IObservable<int>> sources = new() { [0] = Signal.Return(FirstValue), [1] = Signal.Return(SecondValue) };
        for (var i = 0; i < Count; i++)
        {
            var key = i & 1;
            var flag = key == 0;
            using var deferred = Signal.Defer(() => Signal.Return(key)).Subscribe(observer);
            using var conditional = Signal.If(() => flag, Signal.Return(FirstValue), Signal.Return(SecondValue)).Subscribe(observer);
            using var thenOnly = Signal.If(() => flag, Signal.Return(ThenValue)).Subscribe(observer);
            using var keyed = Signal.Case(() => key, sources).Subscribe(observer);
        }

        return observer.Total;
    }

    /// <summary>Subscribes deferred, conditional and keyed factories once per iteration using System.Reactive.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int SystemReactiveDeferIfCase()
    {
        IntSignalWitness observer = new();
        Dictionary<int, IObservable<int>> sources = new() { [0] = RxObservable.Return(FirstValue), [1] = RxObservable.Return(SecondValue) };
        for (var i = 0; i < Count; i++)
        {
            var key = i & 1;
            var flag = key == 0;
            using var deferred = RxObservable.Defer(() => RxObservable.Return(key)).Subscribe(observer);
            using var conditional = RxObservable.If(() => flag, RxObservable.Return(FirstValue), RxObservable.Return(SecondValue)).Subscribe(observer);
            using var thenOnly = RxObservable.If(() => flag, RxObservable.Return(ThenValue)).Subscribe(observer);
            using var keyed = RxObservable.Case(() => key, sources).Subscribe(observer);
        }

        return observer.Total;
    }

    /// <summary>Materializes a range and a failure into sparks and formats each one.</summary>
    /// <returns>The total formatted length.</returns>
    [Benchmark]
    public int PrimitivesSparkFormatting()
    {
        TextLengthWitness<Core.Spark<int>> observer = new();
        using var values = Signal.Range(1, Count).Spark().Subscribe(observer);
        using var failure = Signal.Throw<int>(Failure).Spark().Subscribe(observer);
        return observer.Length;
    }

    /// <summary>Materializes a range and a failure into notifications and formats each one using System.Reactive.</summary>
    /// <returns>The total formatted length.</returns>
    [Benchmark]
    public int SystemReactiveMaterializeFormatting()
    {
        TextLengthWitness<System.Reactive.Notification<int>> observer = new();
        using var values = RxObservable.Materialize(RxObservable.Range(1, Count)).Subscribe(observer);
        using var failure = RxObservable.Materialize(RxObservable.Throw<int>(Failure)).Subscribe(observer);
        return observer.Length;
    }

    /// <summary>Accumulates the formatted length of every observed value.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class TextLengthWitness<T> : IObserver<T>
    {
        /// <summary>Gets the total formatted length.</summary>
        internal int Length { get; private set; }

        /// <inheritdoc/>
        public void OnNext(T value) => Length += value?.ToString()?.Length ?? 0;

        /// <inheritdoc/>
        public void OnError(Exception error) => Length--;

        /// <inheritdoc/>
        public void OnCompleted() => Length++;
    }
}
