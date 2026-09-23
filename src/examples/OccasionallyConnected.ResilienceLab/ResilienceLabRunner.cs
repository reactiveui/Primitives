// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Runs bounded occasionally connected resilience lab scenarios.</summary>
public static class ResilienceLabRunner
{
    /// <summary>Runs the selected scenario and writes expected-versus-actual output.</summary>
    /// <param name="options">The scenario options.</param>
    /// <param name="writer">The output writer.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The typed scenario result.</returns>
    public static async ValueTask<ResilienceLabRunResult> RunAsync(
        ResilienceLabOptions options,
        TextWriter writer,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(writer);
        cancellationToken.ThrowIfCancellationRequested();
        if (string.Equals(options.Scenario, CrdtLoopbackScenarioShape.ScenarioName, StringComparison.Ordinal))
        {
            var cases = await CrdtLoopbackScenario.RunAsync(cancellationToken).ConfigureAwait(false);
            await WriteAsync(writer, options.Scenario, cases).ConfigureAwait(false);
            return new(options.Scenario, cases);
        }

        if (string.Equals(options.Scenario, DurableHttpLostAckScenario.ScenarioName, StringComparison.Ordinal))
        {
            var cases = await DurableHttpLostAckScenario.RunAsync(cancellationToken).ConfigureAwait(false);
            await WriteAsync(writer, options.Scenario, cases).ConfigureAwait(false);
            return new(options.Scenario, cases);
        }

        var unknown = ResilienceLabCaseResult.Fail(
            "scenario",
            CrdtLoopbackScenarioShape.ScenarioName,
            options.Scenario);
        await WriteAsync(writer, options.Scenario, [unknown]).ConfigureAwait(false);
        return new(options.Scenario, [unknown]);
    }

    /// <summary>Writes a stable result transcript.</summary>
    /// <param name="writer">The destination writer.</param>
    /// <param name="scenario">The scenario name.</param>
    /// <param name="cases">The case results.</param>
    /// <returns>The write task.</returns>
    private static async ValueTask WriteAsync(
        TextWriter writer,
        string scenario,
        IReadOnlyList<ResilienceLabCaseResult> cases)
    {
        await writer.WriteLineAsync($"scenario: {scenario}").ConfigureAwait(false);
        for (var index = 0; index < cases.Count; index++)
        {
            var item = cases[index];
            var line = $"{item.Name}: expected={item.Expected}; actual={item.Actual}; passed={item.Succeeded}";
            await writer.WriteLineAsync(line).ConfigureAwait(false);
        }
    }
}
