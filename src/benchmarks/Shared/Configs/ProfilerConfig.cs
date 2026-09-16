// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.Tracing;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using Microsoft.Diagnostics.NETCore.Client;
using Microsoft.Diagnostics.Tracing.Parsers;

namespace ReactiveUI.Primitives.Benchmarks.Configs;

/// <summary>Traces a benchmark with EventPipe, recording CPU samples and verbose GC events in one trace.</summary>
/// <remarks>
/// A config keeps one EventPipe profiler, and the CPU and GC profiles both enable the runtime provider. So the
/// sample profiler is added beside the GC profile, and the runtime provider carries the keywords of both.
/// </remarks>
public class ProfilerConfig : ManualConfig
{
    /// <summary>The provider that samples CPU stacks.</summary>
    private const string SampleProfilerProviderName = "Microsoft-DotNETCore-SampleProfiler";

    /// <summary>The runtime keywords of both profiles: the CPU profile's defaults, which include GC and exceptions, plus GC handles.</summary>
    private const ClrTraceEventParser.Keywords RuntimeKeywords =
        ClrTraceEventParser.Keywords.Default | ClrTraceEventParser.Keywords.GCHandle;

    /// <summary>Initializes a new instance of the <see cref="ProfilerConfig"/> class.</summary>
    public ProfilerConfig()
    {
        if (!BenchmarkProfiling.Enabled)
        {
            return;
        }

        EventPipeProvider[] providers =
        [
            new(SampleProfilerProviderName, EventLevel.Informational),
            new(ClrTraceEventParser.ProviderName, EventLevel.Verbose, (long)RuntimeKeywords),
        ];

        _ = AddDiagnoser(new EventPipeProfiler(EventPipeProfile.GcVerbose, providers));
    }
}
