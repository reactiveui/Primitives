// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Benchmarks.Configs;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Entry point for the Avalonia benchmarks.</summary>
internal static class Program
{
    /// <summary>Executes the benchmarks.</summary>
    /// <param name="args">BenchmarkDotNet command-line arguments.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void Main(string[] args) => BenchmarkHost.Run(typeof(Program).Assembly, args);
}
