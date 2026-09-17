// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Jobs;

namespace ReactiveUI.Primitives.Benchmarks.Configs;

/// <summary>Adds the NativeAOT runtimes, for benchmarks whose code publishes ahead of time.</summary>
public class NativeAotBenchmarkConfig : ManualConfig
{
    /// <summary>The id of the NativeAOT 10 job.</summary>
    private const string NativeAot10JobId = "NativeAot10_0";

    /// <summary>The id of the NativeAOT 11 job.</summary>
    private const string NativeAot11JobId = "NativeAot11_0";

    /// <summary>Initializes a new instance of the <see cref="NativeAotBenchmarkConfig"/> class.</summary>
    public NativeAotBenchmarkConfig()
    {
        _ = AddJob(new Job(NativeAot10JobId).WithRuntime(NativeAotRuntime.Net10_0));
        _ = AddJob(new Job(NativeAot11JobId).WithRuntime(NativeAotRuntime.Net11_0));
    }

    /// <summary>Gets a value indicating whether a job is one of the NativeAOT jobs this config adds.</summary>
    /// <param name="job">The job to check.</param>
    /// <returns><see langword="true"/> for a NativeAOT job; otherwise, <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsNativeAotJob(Job job) => job.Id is NativeAot10JobId or NativeAot11JobId;
}
