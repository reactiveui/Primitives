// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reflection;
using BenchmarkDotNet.Running;

namespace ReactiveUI.Primitives.Benchmarks.Configs;

/// <summary>Runs the benchmarks on .NET with EventPipe, on .NET Framework with ETW, and on NativeAOT with the memory diagnoser.</summary>
public static class BenchmarkHost
{
    /// <summary>Runs the benchmarks the command line selects.</summary>
    /// <param name="assembly">The assembly holding the benchmarks.</param>
    /// <param name="args">The command line arguments passed to the benchmark switcher.</param>
    public static void Run(Assembly assembly, string[] args)
    {
        var switcher = BenchmarkSwitcher.FromAssembly(assembly);
        _ = switcher.Run(args, new BenchmarkConfig());
        _ = switcher.Run(args, new NetFrameworkConfig());
        _ = switcher.Run(args, new NativeAotMemoryConfig());
    }
}
