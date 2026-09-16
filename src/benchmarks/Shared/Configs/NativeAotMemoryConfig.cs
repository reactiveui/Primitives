// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Filters;

namespace ReactiveUI.Primitives.Benchmarks.Configs;

/// <summary>Runs the NativeAOT jobs with the memory diagnoser.</summary>
/// <remarks>
/// EventPipe cannot attach to a NativeAOT process built with the default settings, and a run keeps one set of
/// diagnosers, so these jobs run apart from <see cref="BenchmarkConfig"/>. The filter keeps only the NativeAOT jobs
/// a benchmark class adds through <see cref="NativeAotBenchmarkConfig"/>.
/// </remarks>
public class NativeAotMemoryConfig : ManualConfig
{
    /// <summary>Initializes a new instance of the <see cref="NativeAotMemoryConfig"/> class.</summary>
    public NativeAotMemoryConfig()
    {
        Add(DefaultConfig.Instance);
        _ = AddDiagnoser(MemoryDiagnoser.Default);
        _ = AddExporter(MarkdownExporter.GitHub);
        _ = AddFilter(new SimpleFilter(static benchmark => NativeAotBenchmarkConfig.IsNativeAotJob(benchmark.Job)));
        _ = WithOption(ConfigOptions.DontOverwriteResults, true);
    }
}
