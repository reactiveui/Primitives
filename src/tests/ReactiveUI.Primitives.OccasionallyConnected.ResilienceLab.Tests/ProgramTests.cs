// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Tests for <see cref="Program"/>.</summary>
public sealed class ProgramTests
{
    /// <summary>The process timeout in seconds.</summary>
    private const int ProcessTimeoutSeconds = 10;

    /// <summary>The unknown scenario argument.</summary>
    private const string UnknownScenario = "unknown";

    /// <summary>The program assembly extension.</summary>
    private const string AssemblyExtension = ".dll";

    /// <summary>Verifies the real process entry point returns a nonzero exit code for unsupported scenarios.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task MainReturnsNonZeroForUnknownScenarioSubprocess()
    {
        var result = await RunProgramAsync("--scenario", UnknownScenario).ConfigureAwait(false);

        await Assert.That(result.ExitedNormally).IsTrue();
        await Assert.That(result.ExitCode).IsEqualTo(1);
        await Assert.That(result.StandardOutput).Contains("scenario: unknown");
        await Assert.That(result.StandardOutput).Contains("expected=crdt-loopback");
        await Assert.That(result.StandardError).IsEmpty();
    }

    /// <summary>Runs the built program assembly in an isolated subprocess.</summary>
    /// <param name="args">The program arguments.</param>
    /// <returns>The process result.</returns>
    /// <exception cref="InvalidOperationException">The process could not be started.</exception>
    private static async Task<ProgramProcessResult> RunProgramAsync(params string[] args)
    {
        var startInfo = new ProcessStartInfo("dotnet") { RedirectStandardError = true, RedirectStandardOutput = true, UseShellExecute = false };
        var programAssembly = Path.Combine(AppContext.BaseDirectory, typeof(Program).Assembly.GetName().Name + AssemblyExtension);
        startInfo.ArgumentList.Add(programAssembly);
        for (var index = 0; index < args.Length; index++)
        {
            startInfo.ArgumentList.Add(args[index]);
        }

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(ProcessTimeoutSeconds));
        using var process = Process.Start(startInfo) ?? throw new InvalidOperationException("The ResilienceLab process could not be started.");
        var outputTask = process.StandardOutput.ReadToEndAsync();
        var errorTask = process.StandardError.ReadToEndAsync();
        try
        {
#if NET11_0_OR_GREATER
            var exitStatus = await process.WaitForExitStatusAsync(timeout.Token).ConfigureAwait(false);
            var output = await outputTask.ConfigureAwait(false);
            var error = await errorTask.ConfigureAwait(false);
            return new(exitStatus.ExitCode, !exitStatus.Canceled && exitStatus.Signal is null, output, error);
#else
            await process.WaitForExitAsync(timeout.Token).ConfigureAwait(false);
            var output = await outputTask.ConfigureAwait(false);
            var error = await errorTask.ConfigureAwait(false);
            return new(process.ExitCode, true, output, error);
#endif
        }
        catch (OperationCanceledException)
        {
            process.Kill(entireProcessTree: true);
            throw;
        }
    }

    /// <summary>Holds an isolated program process result.</summary>
    /// <param name="ExitCode">The process exit code.</param>
    /// <param name="ExitedNormally">A value indicating whether the process exited normally.</param>
    /// <param name="StandardOutput">The captured standard output.</param>
    /// <param name="StandardError">The captured standard error.</param>
    private sealed record ProgramProcessResult(int ExitCode, bool ExitedNormally, string StandardOutput, string StandardError);
}
