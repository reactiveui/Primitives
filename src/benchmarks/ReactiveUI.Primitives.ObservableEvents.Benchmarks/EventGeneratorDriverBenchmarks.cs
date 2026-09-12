// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace ReactiveUI.Primitives.ObservableEvents.Benchmarks;

/// <summary>What the observable-event generator itself costs on a build and on a keystroke.</summary>
/// <remarks>Measures generator execution for cold, unchanged, unrelated-edit, and event-edit inputs, excluding compilation updates.</remarks>
[System.Diagnostics.DebuggerDisplay("EventGeneratorDriverBenchmarks: {Size}")]
[SimpleJob(warmupCount: 5, iterationCount: 15)]
[EventPipeProfiler(EventPipeProfile.CpuSampling)]
public class EventGeneratorDriverBenchmarks
{
    /// <summary>The compilation the cold driver runs against.</summary>
    private Compilation _coldCompilation = null!;

    /// <summary>The driver that has generated nothing yet.</summary>
    private CSharpGeneratorDriver _coldDriver = null!;

    /// <summary>The compilation carrying an edit no request depends on.</summary>
    private Compilation _unrelatedCompilation = null!;

    /// <summary>The primed driver for the unrelated edit.</summary>
    private CSharpGeneratorDriver _unrelatedDriver = null!;

    /// <summary>The compilation whose first host gained an event.</summary>
    private Compilation _eventEditCompilation = null!;

    /// <summary>The primed driver for the event edit.</summary>
    private CSharpGeneratorDriver _eventEditDriver = null!;

    /// <summary>The compilation a primed driver is re-run against with nothing changed.</summary>
    private Compilation _unchangedCompilation = null!;

    /// <summary>The primed driver for the unchanged control.</summary>
    private CSharpGeneratorDriver _unchangedDriver = null!;

    /// <summary>Gets or sets the corpus size under benchmark.</summary>
    [ParamsAllValues]
    public CorpusSize Size { get; set; }

    /// <summary>Builds the cold and primed driver states for the current corpus size.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var cold = GeneratorHarness.CreateColdState(Size);
        _coldCompilation = cold.Compilation;
        _coldDriver = cold.Driver;

        var unrelated = GeneratorHarness.CreateUnrelatedEditState(Size);
        _unrelatedCompilation = unrelated.Compilation;
        _unrelatedDriver = unrelated.Driver;

        var eventEdit = GeneratorHarness.CreateEventEditState(Size);
        _eventEditCompilation = eventEdit.Compilation;
        _eventEditDriver = eventEdit.Driver;

        var unchanged = GeneratorHarness.CreateUnchangedState(Size);
        _unchangedCompilation = unchanged.Compilation;
        _unchangedDriver = unchanged.Driver;
    }

    /// <summary>Generates the whole corpus from a driver with nothing cached.</summary>
    /// <returns>The updated driver.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark(Baseline = true)]
    public GeneratorDriver Cold() => _coldDriver.RunGenerators(_coldCompilation);

    /// <summary>Re-runs a primed driver against the compilation it was primed on, with nothing changed.</summary>
    /// <returns>The updated driver.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public GeneratorDriver Unchanged() => _unchangedDriver.RunGenerators(_unchangedCompilation);

    /// <summary>Re-runs a primed driver after an edit in a file no request depends on.</summary>
    /// <returns>The updated driver.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public GeneratorDriver UnrelatedEdit() => _unrelatedDriver.RunGenerators(_unrelatedCompilation);

    /// <summary>Re-runs a primed driver after one host gained an event.</summary>
    /// <returns>The updated driver.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public GeneratorDriver EventEdit() => _eventEditDriver.RunGenerators(_eventEditCompilation);
}
