// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace ReactiveUI.Primitives.ObservableEvents.Benchmarks;

/// <summary>Generator execution when requests cannot be served, so diagnostics are reported instead of wrappers.</summary>
[System.Diagnostics.DebuggerDisplay("EventGeneratorDiagnosticBenchmarks: {Size}")]
[SimpleJob(warmupCount: 5, iterationCount: 15)]
public class EventGeneratorDiagnosticBenchmarks
{
    /// <summary>The compilation that references a provider and requests unsupported static hosts.</summary>
    private Compilation _unsupportedCompilation = null!;

    /// <summary>The fresh driver for the unsupported hosts.</summary>
    private CSharpGeneratorDriver _unsupportedDriver = null!;

    /// <summary>The compilation that references no observable provider.</summary>
    private Compilation _missingProviderCompilation = null!;

    /// <summary>The fresh driver for the missing provider.</summary>
    private CSharpGeneratorDriver _missingProviderDriver = null!;

    /// <summary>Gets or sets the corpus size under benchmark.</summary>
    [ParamsAllValues]
    public CorpusSize Size { get; set; }

    /// <summary>Builds the unsupported-host and missing-provider states for the current corpus size.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var unsupported = GeneratorHarness.CreateUnservableState(Size, true);
        _unsupportedCompilation = unsupported.Compilation;
        _unsupportedDriver = unsupported.Driver;

        var missingProvider = GeneratorHarness.CreateUnservableState(Size, false);
        _missingProviderCompilation = missingProvider.Compilation;
        _missingProviderDriver = missingProvider.Driver;
    }

    /// <summary>Generates the corpus alongside requests for an eventless and a generic static host, which are reported.</summary>
    /// <returns>The updated driver.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark(Baseline = true)]
    public GeneratorDriver UnsupportedHosts() => _unsupportedDriver.RunGenerators(_unsupportedCompilation);

    /// <summary>Runs the generator where no observable provider is referenced, reporting every request.</summary>
    /// <returns>The updated driver.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public GeneratorDriver MissingProvider() => _missingProviderDriver.RunGenerators(_missingProviderCompilation);
}
