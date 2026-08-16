// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Running;

namespace ReactiveUI.Primitives.ObservableEvents.Benchmarks;

/// <summary>Entry point for the observable-event generator benchmarks.</summary>
internal static class Program
{
    /// <summary>Runs a benchmark suite, or checks the corpus with <c>--smoke</c>.</summary>
    /// <param name="args">BenchmarkDotNet command-line arguments.</param>
    internal static void Main(string[] args)
    {
        if (args.Contains("--smoke", StringComparer.OrdinalIgnoreCase))
        {
            GeneratorHarness.ValidateCorpus();
            return;
        }

        _ = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}
