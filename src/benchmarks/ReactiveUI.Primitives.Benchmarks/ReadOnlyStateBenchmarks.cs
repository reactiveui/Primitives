// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using R3;
using ReactiveUI.Primitives.Signals;
using R3Subject = R3.Subject<int>;
using RxBehaviorSubject = System.Reactive.Subjects.BehaviorSubject<int>;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures read-only latest-value holders that mirror a source stream.</summary>
[MemoryDiagnoser]
public class ReadOnlyStateBenchmarks
{
    /// <summary>The number of source values mirrored per case.</summary>
    private const int Count = 1000;

    /// <summary>The value held before the source emits.</summary>
    private const int InitialValue = -1;

    /// <summary>Benchmarks mirroring a source into a read-only state with one subscriber.</summary>
    /// <returns>The sum observed by the subscriber plus the final value.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesReadOnlyStateMirror()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using ReadOnlyState<int> state = new(source, InitialValue);
        using var subscription = state.Changed.Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        return observer.Total + state.Value;
    }

    /// <summary>Benchmarks mirroring a source into a behavior subject using System.Reactive.</summary>
    /// <returns>The sum observed by the subscriber plus the final value.</returns>
    [Benchmark]
    public int SystemReactiveBehaviorSubjectMirror()
    {
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using RxBehaviorSubject state = new(InitialValue);
        using var link = source.Subscribe(state);
        using var subscription = state.Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        return observer.Total + state.Value;
    }

    /// <summary>Benchmarks mirroring a source into a read-only reactive property using R3.</summary>
    /// <returns>The sum observed by the subscriber plus the final value.</returns>
    [Benchmark]
    public int R3ReadOnlyReactivePropertyMirror()
    {
        IntR3Witness observer = new();
        using R3Subject source = new();
        using var state = source.ToReadOnlyReactiveProperty(InitialValue);
        using var subscription = state.Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        return observer.Total + state.CurrentValue;
    }
}
