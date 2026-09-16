// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Benchmarks.Configs;

namespace ReactiveUI.Primitives.ObservableEvents.Benchmarks;

/// <summary>Entry point for the observable-event generator benchmarks.</summary>
internal static class Program
{
    /// <summary>Runs a benchmark suite, or checks the corpus with <c>--smoke</c>.</summary>
    /// <param name="args">BenchmarkDotNet command-line arguments.</param>
    internal static void Main(string[] args)
    {
        if (HasSwitch(args, "--smoke"))
        {
            GeneratorHarness.ValidateCorpus();
            return;
        }

        BenchmarkHost.Run(typeof(Program).Assembly, args);
    }

    /// <summary>Reports whether the command line carries a switch, ignoring case.</summary>
    /// <param name="args">The command line arguments.</param>
    /// <param name="name">The switch to look for.</param>
    /// <returns><see langword="true"/> when the switch is present; otherwise, <see langword="false"/>.</returns>
    private static bool HasSwitch(string[] args, string name)
    {
        foreach (var arg in args)
        {
            if (string.Equals(arg, name, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }
}
