// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Isolates ambient tracing checks from test-runner activity listeners.</summary>
public sealed partial class OccasionallyConnectedActivitiesTests
{
    /// <summary>The child-only probe selector.</summary>
    private const string ProbeVariable = "OC_ACTIVITY_TEST_PROBE";

    /// <summary>Selects a listener that rejects activities.</summary>
    private const string RejectProbe = "reject";

    /// <summary>Bounds each child probe.</summary>
    private const int ProbeTimeoutSeconds = 30;

    /// <summary>Runs the selected child probe before the test runner registers process-wide listeners.</summary>
    [ModuleInitializer]
    internal static void RunIsolatedActivityProbe()
    {
        var mode = Environment.GetEnvironmentVariable(ProbeVariable);
        if (mode is not (RejectProbe or "none"))
        {
            return;
        }

        var sampled = 0;
        using var listener = mode == RejectProbe ? new ActivityListener
        {
            ShouldListenTo = static source => source.Name == OccasionallyConnectedActivities.DiagnosticSourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
            {
                sampled++;
                return ActivitySamplingResult.None;
            },
        } : null;
        if (listener is not null)
        {
            ActivitySource.AddActivityListener(listener);
        }

        using var parent = new Activity(ParentOperationName).Start();
        using var recorder = new OccasionallyConnectedActivities();
        using var activity = recorder.Start(OccasionallyConnectedActivityName.SyncPush);
        var result = mode == RejectProbe
            ? $"{activity is null}|{sampled}"
            : $"{activity is null}|{ReferenceEquals(Activity.Current, parent)}";
        using var output = Console.OpenStandardOutput();
        output.Write(Encoding.UTF8.GetBytes(result));
        output.Flush();
        Environment.Exit(0);
    }

    /// <summary>Runs one probe in an owned child without initializing the test host.</summary>
    /// <param name="mode">The probe selector.</param>
    /// <returns>The observations returned by the child.</returns>
    /// <exception cref="InvalidOperationException">The child cannot start or fails.</exception>
    /// <exception cref="OperationCanceledException">The child exceeds its time limit.</exception>
    private static async Task<string> RunActivityProbeAsync(string mode)
    {
        ProcessStartInfo startInfo = new("dotnet") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false, CreateNoWindow = true };
        startInfo.ArgumentList.Add(Path.Combine(AppContext.BaseDirectory, "ReactiveUI.Primitives.OccasionallyConnected.Tests.dll"));
        startInfo.Environment[ProbeVariable] = mode;
        using var process = Process.Start(startInfo) ?? throw new InvalidOperationException("Could not start the activity probe.");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(ProbeTimeoutSeconds));
        var output = process.StandardOutput.ReadToEndAsync();
        var error = process.StandardError.ReadToEndAsync();
        try
        {
#if NET11_0_OR_GREATER
            var exit = await process.WaitForExitStatusAsync(timeout.Token);
            if (exit.Canceled)
            {
                throw new OperationCanceledException(timeout.Token);
            }

            if (exit.Signal is not null || exit.ExitCode != 0)
#else
            await process.WaitForExitAsync(timeout.Token);
            if (process.ExitCode != 0)
#endif
            {
                throw new InvalidOperationException(await error);
            }

            return (await output).Trim();
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }
        }
    }
}
