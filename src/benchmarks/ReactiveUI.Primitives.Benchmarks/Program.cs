// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Text;
using BenchmarkDotNet.Running;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Entry point for benchmark execution and smoke-test mode.</summary>
internal static class Program
{
    /// <summary>Executes benchmarks, or runs a deterministic smoke check with <c>--smoke</c>.</summary>
    /// <param name="args">BenchmarkDotNet command-line arguments.</param>
    /// <returns>A task that completes when execution is finished.</returns>
    public static async Task Main(string[] args)
    {
        if (args.Contains("--alloc", StringComparer.OrdinalIgnoreCase))
        {
            AllocationProbe.Run();
            return;
        }

        if (args.Contains("--smoke", StringComparer.OrdinalIgnoreCase))
        {
            var originalOutput = Console.Out;
            StringWriter capturedOutput = new(CultureInfo.InvariantCulture);
            SmokeTeeTextWriter teeOutput = new(originalOutput, capturedOutput);
            Console.SetOut(teeOutput);
            try
            {
                await SmokeBenchmarkRunner.RunAsync();
            }
            finally
            {
                Console.SetOut(originalOutput);
            }

            SmokeParityValidator.Validate(capturedOutput.ToString());
            return;
        }

        if (args.Contains("--extensions-smoke", StringComparer.OrdinalIgnoreCase))
        {
            RunExtensionComparisonSmoke();
            return;
        }

        _ = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }

    /// <summary>Runs the extension comparison scenarios once to validate benchmark delegates.</summary>
    private static void RunExtensionComparisonSmoke()
    {
        RunExtensionScenarioSet(
            nameof(ReactiveExtensionsComparisonBenchmarks.PrimitivesScenarios),
            ReactiveExtensionsComparisonBenchmarks.PrimitivesScenarios);
        RunExtensionScenarioSet(
            nameof(ReactiveExtensionsComparisonBenchmarks.ReactiveUIExtensionsScenarios),
            ReactiveExtensionsComparisonBenchmarks.ReactiveUIExtensionsScenarios);
        RunExtensionScenarioSet(
            nameof(ReactiveExtensionsComparisonBenchmarks.SystemReactiveScenarios),
            ReactiveExtensionsComparisonBenchmarks.SystemReactiveScenarios);
        RunExtensionScenarioSet(
            nameof(ReactiveExtensionsComparisonBenchmarks.R3Scenarios),
            ReactiveExtensionsComparisonBenchmarks.R3Scenarios);
        Console.WriteLine("Extensions scenario smoke validation passed.");
    }

    /// <summary>Runs a named extension scenario set.</summary>
    /// <param name="name">The scenario set name.</param>
    /// <param name="scenarios">The scenarios to run.</param>
    private static void RunExtensionScenarioSet(
        string name,
        IEnumerable<ReactiveExtensionsComparisonBenchmarks.ExtensionScenario> scenarios)
    {
        foreach (var scenario in scenarios)
        {
            Console.WriteLine($"{name}:{scenario}");
            _ = scenario.Run();
        }
    }

    /// <summary>A <see cref="TextWriter"/> that mirrors every write to a primary and a secondary writer.</summary>
    /// <param name="primary">The primary writer to forward writes to.</param>
    /// <param name="secondary">The secondary writer to forward writes to.</param>
    private sealed class SmokeTeeTextWriter(TextWriter primary, TextWriter secondary) : TextWriter
    {
        /// <summary>Gets the character encoding of the primary writer.</summary>
        public override Encoding Encoding => primary.Encoding;

        /// <summary>Writes a character to both the primary and secondary writers.</summary>
        /// <param name="value">The character to write.</param>
        public override void Write(char value)
        {
            primary.Write(value);
            secondary.Write(value);
        }

        /// <summary>Writes a string to both the primary and secondary writers.</summary>
        /// <param name="value">The string to write.</param>
        public override void Write(string? value)
        {
            primary.Write(value);
            secondary.Write(value);
        }

        /// <summary>Writes a string followed by a line terminator to both the primary and secondary writers.</summary>
        /// <param name="value">The string to write.</param>
        public override void WriteLine(string? value)
        {
            primary.WriteLine(value);
            secondary.WriteLine(value);
        }
    }
}
