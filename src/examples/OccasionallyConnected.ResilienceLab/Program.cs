// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Console entry point for the occasionally connected resilience lab.</summary>
public static class Program
{
    /// <summary>Runs the requested lab scenario.</summary>
    /// <param name="args">The command-line arguments.</param>
    /// <returns>The process exit code.</returns>
    public static async Task<int> Main(string[] args)
    {
        var options = ResilienceLabOptions.Parse(args);
        var result = await ResilienceLabRunner
            .RunAsync(options, Console.Out, CancellationToken.None)
            .ConfigureAwait(false);
        return result.ExitCode;
    }
}
