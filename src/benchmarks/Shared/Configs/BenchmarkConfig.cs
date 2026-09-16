// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Filters;
using BenchmarkDotNet.Jobs;

namespace ReactiveUI.Primitives.Benchmarks.Configs;

/// <summary>Runs benchmarks on .NET 8, 10 and 11, traced with EventPipe.</summary>
/// <remarks>The filter drops the NativeAOT jobs a benchmark class adds, which <see cref="NativeAotMemoryConfig"/> runs.</remarks>
public class BenchmarkConfig : ProfilerConfig
{
    /// <summary>Initializes a new instance of the <see cref="BenchmarkConfig"/> class.</summary>
    public BenchmarkConfig()
    {
        Add(DefaultConfig.Instance);
        _ = AddJob(new Job().WithRuntime(CoreRuntime.Core80));
        _ = AddJob(new Job().WithRuntime(CoreRuntime.Core10_0));
        _ = AddJob(new Job().WithRuntime(CoreRuntime.Core11_0));
        _ = AddExporter(MarkdownExporter.GitHub);
        _ = AddFilter(new SimpleFilter(static benchmark => !NativeAotBenchmarkConfig.IsNativeAotJob(benchmark.Job)));
    }
}
