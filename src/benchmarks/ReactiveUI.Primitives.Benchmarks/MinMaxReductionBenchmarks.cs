// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using BenchmarkDotNet.Attributes;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Compares the latest-value reduction used by asynchronous minimum operators.</summary>
[MemoryDiagnoser]
[System.Diagnostics.DebuggerDisplay("MinMaxReductionBenchmarks: Sources = {SourceCount}")]
public class MinMaxReductionBenchmarks
{
    /// <summary>A stride coprime to every source count produces a repeatable permutation.</summary>
    private const int ValueStride = 17;

    /// <summary>Offsets the permutation so its first value need not be the minimum.</summary>
    private const int ValueOffset = 42;

    /// <summary>The coordinator exposes its reusable array through this interface.</summary>
    [SuppressMessage(
        "Performance",
        "CA1859:Use concrete types when possible for improved performance",
        Justification = "The baseline must measure the coordinator's IReadOnlyList<int> dispatch; an array field would change the measured code.")]
    private IReadOnlyList<int> _values = [];

    /// <summary>Gets or sets the number of latest values in the snapshot.</summary>
    [Params(3, 8, 32, 128, 1024)]
    public int SourceCount { get; set; }

    /// <summary>Creates one snapshot outside the measured operations.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var values = new int[SourceCount];
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = ((i * ValueStride) + ValueOffset) % values.Length;
        }

        _values = values;
    }

    /// <summary>Reduces the snapshot through the existing scalar comparison loop.</summary>
    /// <returns>The minimum value.</returns>
    [Benchmark(Baseline = true)]
    public int ScalarMinimum()
    {
        var minimum = _values[0];
        for (var i = 1; i < _values.Count; i++)
        {
            if (Comparer<int>.Default.Compare(_values[i], minimum) < 0)
            {
                minimum = _values[i];
            }
        }

        return minimum;
    }

#if NET11_0_OR_GREATER
    /// <summary>Reduces the same snapshot with the .NET 11 span implementation.</summary>
    /// <returns>The minimum value.</returns>
    [Benchmark]
    public int SpanMinimum() => ((ReadOnlySpan<int>)(int[])_values).Min();
#endif
}
