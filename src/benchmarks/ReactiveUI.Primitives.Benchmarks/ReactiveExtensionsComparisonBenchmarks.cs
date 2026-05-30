// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using PackageExtensions = ReactiveUI.Extensions.ReactiveExtensions;
using PrimitivesExtensions = ReactiveUI.Primitives.Extensions.ReactiveExtensions;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks the synchronous extension-operator package against ReactiveUI.Extensions 4.0.0.
/// </summary>
[MemoryDiagnoser]
public class ReactiveExtensionsComparisonBenchmarks
{
    /// <summary>
    /// The number of values produced by range-based benchmarks.
    /// </summary>
    private const int Count = 32;

    /// <summary>
    /// Source array used by FromArray/FastForEach style benchmarks.
    /// </summary>
    private static readonly int[] Values = CreateValues();

    /// <summary>
    /// Source characters used by delimiter-buffer benchmarks.
    /// </summary>
    private static readonly char[] BufferCharacters = "xx[abc]yy[de]".ToCharArray();

    /// <summary>
    /// Source booleans used by boolean extension benchmarks.
    /// </summary>
    private static readonly bool[] BooleanValues = [true, false, false, true, false, true, false, false];

    /// <summary>
    /// Compares the fused filter/projection operator over a finite range.
    /// </summary>
    /// <returns>The observed value total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesWhereSelectRange()
    {
        var observer = new IntSignalObserver();
        using var subscription = PrimitivesExtensions.WhereSelect(
                Signal.Sequence(0, Count),
                static value => (value & 1) == 0,
                static value => value * 3)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Compares the ReactiveUI.Extensions 4.0.0 fused filter/projection operator over a finite range.
    /// </summary>
    /// <returns>The observed value total.</returns>
    [Benchmark]
    public int PackageWhereSelectRange()
    {
        var observer = new IntSignalObserver();
        using var subscription = PackageExtensions.WhereSelect(
                RxObservable.Range(0, Count),
                static value => (value & 1) == 0,
                static value => value * 3)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Compares the array-to-observable helper over an integer array.
    /// </summary>
    /// <returns>The observed value total.</returns>
    [Benchmark]
    public int PrimitivesFromArraySubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = PrimitivesExtensions.FromArray(Values).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Compares the ReactiveUI.Extensions 4.0.0 array-to-observable helper.
    /// </summary>
    /// <returns>The observed value total.</returns>
    [Benchmark]
    public int PackageFromArraySubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = PackageExtensions.FromArray(Values).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Compares the pairwise operator over a finite range.
    /// </summary>
    /// <returns>The aggregate of observed pair values.</returns>
    [Benchmark]
    public int PrimitivesPairwiseRange()
    {
        var observer = new PairObserver();
        using var subscription = PrimitivesExtensions.Pairwise(Signal.Sequence(0, Count)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Compares the ReactiveUI.Extensions 4.0.0 pairwise operator over a finite range.
    /// </summary>
    /// <returns>The aggregate of observed pair values.</returns>
    [Benchmark]
    public int PackagePairwiseRange()
    {
        var observer = new PairObserver();
        using var subscription = PackageExtensions.Pairwise(RxObservable.Range(0, Count)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Compares the delimiter-buffer operator over a finite character stream.
    /// </summary>
    /// <returns>The total length of emitted buffers.</returns>
    [Benchmark]
    public int PrimitivesBufferUntil()
    {
        var observer = new StringLengthObserver();
        using var subscription = PrimitivesExtensions.BufferUntil(
                PrimitivesExtensions.FromArray(BufferCharacters),
                '[',
                ']')
            .Subscribe(observer);
        return observer.TotalLength;
    }

    /// <summary>
    /// Compares the ReactiveUI.Extensions 4.0.0 delimiter-buffer operator.
    /// </summary>
    /// <returns>The total length of emitted buffers.</returns>
    [Benchmark]
    public int PackageBufferUntil()
    {
        var observer = new StringLengthObserver();
        using var subscription = PackageExtensions.BufferUntil(
                PackageExtensions.FromArray(BufferCharacters),
                '[',
                ']')
            .Subscribe(observer);
        return observer.TotalLength;
    }

    /// <summary>
    /// Compares the boolean negation operator over a finite stream.
    /// </summary>
    /// <returns>The number of true values emitted after negation.</returns>
    [Benchmark]
    public int PrimitivesNotWhereTrue()
    {
        var observer = new BoolSignalObserver();
        using var subscription = PrimitivesExtensions.WhereTrue(
                PrimitivesExtensions.Not(
                    PrimitivesExtensions.FromArray(BooleanValues)))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Compares the ReactiveUI.Extensions 4.0.0 boolean negation and true-filter operators.
    /// </summary>
    /// <returns>The number of true values emitted after negation.</returns>
    [Benchmark]
    public int PackageNotWhereTrue()
    {
        var observer = new BoolSignalObserver();
        using var subscription = PackageExtensions.WhereTrue(
                PackageExtensions.Not(
                    PackageExtensions.FromArray(BooleanValues)))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Creates the shared source array.
    /// </summary>
    /// <returns>An array containing values 0..31.</returns>
    private static int[] CreateValues()
    {
        var values = new int[Count];
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = i;
        }

        return values;
    }

    /// <summary>
    /// Observer that aggregates pairwise tuple values.
    /// </summary>
    private sealed class PairObserver : IObserver<(int Previous, int Current)>
    {
        /// <summary>
        /// Gets the aggregate total.
        /// </summary>
        public int Total { get; private set; }

        /// <inheritdoc/>
        public void OnNext((int Previous, int Current) value) => Total += value.Previous + value.Current;

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }
    }

    /// <summary>
    /// Observer that aggregates emitted string lengths.
    /// </summary>
    private sealed class StringLengthObserver : IObserver<string>
    {
        /// <summary>
        /// Gets the combined string length.
        /// </summary>
        public int TotalLength { get; private set; }

        /// <inheritdoc/>
        public void OnNext(string value) => TotalLength += value.Length;

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }
    }
}
