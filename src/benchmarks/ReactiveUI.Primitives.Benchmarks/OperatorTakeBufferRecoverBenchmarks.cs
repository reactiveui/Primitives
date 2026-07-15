// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks for the common, previously-uncovered operators: prefix truncation (Take), batching
/// (Buffer / Chunk), and the error-handling path (Recover / Resume vs Catch).
/// </summary>
[MemoryDiagnoser]
public class OperatorTakeBufferRecoverBenchmarks
{
    /// <summary>The number of values produced by each benchmarked sequence.</summary>
    private const int Count = 16;

    /// <summary>The number of leading values taken by the prefix-truncation benchmarks.</summary>
    private const int TakeCount = 8;

    /// <summary>The number of values per batch used by the buffering benchmarks.</summary>
    private const int BufferSize = 4;

    /// <summary>The error used to trigger the error-handling benchmarks.</summary>
    private static readonly InvalidOperationException Boom = new("boom");

    /// <summary>Benchmarks taking a prefix of a range.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesTakeRange()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(1, Count).Take(TakeCount).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks taking a prefix of a range using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveTakeRange()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Range(1, Count).Take(TakeCount).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks taking a prefix of a range using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3TakeRange()
    {
        IntR3Witness observer = new();
        using var subscription =
            R3.ObservableExtensions.Take(R3.Observable.Range(1, Count), TakeCount).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks batching a range into fixed-size lists.</summary>
    /// <returns>The number of batches observed.</returns>
    [Benchmark]
    public int PrimitivesBufferRange()
    {
        CountingSignalWitness<IList<int>> observer = new();
        using var subscription = Signal.Sequence(1, Count).Buffer(BufferSize).Subscribe(observer);
        return observer.Count;
    }

    /// <summary>Benchmarks batching a range into fixed-size lists using System.Reactive.</summary>
    /// <returns>The number of batches observed.</returns>
    [Benchmark]
    public int SystemReactiveBufferRange()
    {
        CountingSignalWitness<IList<int>> observer = new();
        using var subscription = RxObservable.Range(1, Count).Buffer(BufferSize).Subscribe(observer);
        return observer.Count;
    }

    /// <summary>Benchmarks batching a range into fixed-size arrays using R3.</summary>
    /// <returns>The number of batches observed.</returns>
    [Benchmark]
    public int R3BufferRange()
    {
        CountingR3Witness<int[]> observer = new();
        using var subscription = R3.ObservableExtensions.Chunk(R3.Observable.Range(1, Count), BufferSize)
            .Subscribe(observer);
        return observer.Count;
    }

    /// <summary>Benchmarks recovering from an error with a handler-selected fallback sequence.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesRecover()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Fail<int>(Boom, Sequencer.Immediate)
            .Recover<int, Exception>(static _ => Signal.Sequence(1, Count))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks recovering from an error with a handler-selected fallback sequence using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveRecover()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Throw<int>(Boom)
            .Catch<int, Exception>(static _ => RxObservable.Range(1, Count))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks recovering from an error with a handler-selected fallback sequence using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3Recover()
    {
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions
            .Catch<int, Exception>(R3.Observable.Throw<int>(Boom), static _ => R3.Observable.Range(1, Count))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks resuming with a fallback sequence on error.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesResume()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Fail<int>(Boom, Sequencer.Immediate)
            .Resume(Signal.Sequence(1, Count))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks resuming with a fallback sequence on error using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveResume()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Throw<int>(Boom)
            .Catch(RxObservable.Range(1, Count))
            .Subscribe(observer);
        return observer.Total;
    }
}
