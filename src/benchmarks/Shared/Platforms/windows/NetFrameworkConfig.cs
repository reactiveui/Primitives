// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnostics.Windows;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Filters;
using BenchmarkDotNet.Jobs;

namespace ReactiveUI.Primitives.Benchmarks.Configs;

/// <summary>Runs benchmarks on .NET Framework 4.6.2, traced with ETW.</summary>
/// <remarks>
/// EventPipe cannot attach to .NET Framework, and a run keeps one set of profilers, so this runs apart from
/// <see cref="BenchmarkConfig"/>. The filter keeps only the .NET Framework job, dropping the NativeAOT jobs a
/// benchmark class adds for the other run.
/// </remarks>
public class NetFrameworkConfig : ManualConfig
{
    /// <summary>The id of the .NET Framework job, which the filter keeps.</summary>
    private const string NetFrameworkJobId = "Net462";

    /// <summary>Initializes a new instance of the <see cref="NetFrameworkConfig"/> class.</summary>
    public NetFrameworkConfig()
    {
        Add(DefaultConfig.Instance);
        _ = AddJob(new Job(NetFrameworkJobId).WithRuntime(ClrRuntime.Net462));
        _ = AddExporter(MarkdownExporter.GitHub);
        _ = AddFilter(new SimpleFilter(static benchmark => benchmark.Job.Id == NetFrameworkJobId));
        _ = WithOption(ConfigOptions.DontOverwriteResults, true);

        if (BenchmarkProfiling.Enabled)
        {
            _ = AddDiagnoser(new EtwProfiler(new EtwProfilerConfig()));
        }
    }
}
