// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace ReactiveUI.Primitives.ObservableEvents.Benchmarks;

/// <summary>
/// Allocation baselines for the generator's cold and incremental runs, with a GC-verbose trace naming the frames
/// the allocations come from. Opt in with <c>--filter "*GcProfile*"</c>.
/// </summary>
/// <remarks>
/// <para>
/// Only the largest corpus is profiled: it is where an allocation per event or per file actually shows up against
/// the compiler's own overhead.
/// </para>
/// <para>
/// The trace is the measurement, not a summary column. An inclusive per-operation total is nearly all Roslyn here
/// - parsing, symbols, and the driver's own state - so it moves with the compiler rather than with this generator.
/// What is actionable is which frames allocated, which is what the GC-verbose trace carries.
/// </para>
/// </remarks>
[ShortRunJob]
[EventPipeProfiler(EventPipeProfile.GcVerbose)]
[System.Diagnostics.DebuggerDisplay("EventGeneratorGcProfileBenchmarks: {nameof(EventGeneratorGcProfileBenchmarks),nq}")]
public class EventGeneratorGcProfileBenchmarks
{
    /// <summary>The corpus size profiled here.</summary>
    private const CorpusSize ProfiledSize = CorpusSize.Large;

    /// <summary>The compilation the cold driver runs against.</summary>
    private Compilation _coldCompilation = null!;

    /// <summary>The driver that has generated nothing yet.</summary>
    private CSharpGeneratorDriver _coldDriver = null!;

    /// <summary>The compilation carrying an edit no request depends on.</summary>
    private Compilation _unrelatedCompilation = null!;

    /// <summary>The primed driver for the unrelated edit.</summary>
    private CSharpGeneratorDriver _unrelatedDriver = null!;

    /// <summary>Builds the cold and primed driver states for the profiled corpus size.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var cold = GeneratorHarness.CreateColdState(ProfiledSize);
        _coldCompilation = cold.Compilation;
        _coldDriver = cold.Driver;

        var unrelated = GeneratorHarness.CreateUnrelatedEditState(ProfiledSize);
        _unrelatedCompilation = unrelated.Compilation;
        _unrelatedDriver = unrelated.Driver;
    }

    /// <summary>Generates the whole corpus from a driver with nothing cached.</summary>
    /// <returns>The updated driver.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark(Baseline = true)]
    public GeneratorDriver Cold() => _coldDriver.RunGenerators(_coldCompilation);

    /// <summary>Re-runs a primed driver after an edit in a file no request depends on.</summary>
    /// <returns>The updated driver.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public GeneratorDriver UnrelatedEdit() => _unrelatedDriver.RunGenerators(_unrelatedCompilation);
}
