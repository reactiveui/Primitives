// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Tests for <see cref="ResilienceLabRunner"/>.</summary>
public sealed class ResilienceLabRunnerTests
{
    /// <summary>The expected authoritative GCounter value.</summary>
    private const int ExpectedGCounterValue = 8;

    /// <summary>The expected authoritative PNCounter value.</summary>
    private const int ExpectedPNCounterValue = 2;

    /// <summary>The expected receive acknowledgement frontier state.</summary>
    private const string ExpectedAcknowledgedFrontier = "acknowledged";

    /// <summary>The expected shared authoritative frontier state.</summary>
    private const string ExpectedSharedFrontier = "same";

    /// <summary>The expected active OR-set value.</summary>
    private const string ExpectedORSetValue = "blue";

    /// <summary>The expected LWW register value.</summary>
    private const string ExpectedLwwValue = "second";

    /// <summary>Verifies the CRDT loopback scenario completes every corrected convergence invariant.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RunAsyncCrdtLoopbackCompletesExpectedInvariants()
    {
        await using var writer = new StringWriter();

        var result = await ResilienceLabRunner.RunAsync(
            new(CrdtLoopbackScenarioShape.ScenarioName),
            writer,
            CancellationToken.None);

        await Assert.That(result.Scenario).IsEqualTo(CrdtLoopbackScenarioShape.ScenarioName);
        await Assert.That(result.Succeeded).IsTrue();
        await Assert.That(result.ExitCode).IsEqualTo(0);
        await AssertGCounterCasesAsync(result);
        await AssertPNCounterCasesAsync(result);
        await AssertORSetCasesAsync(result);
        await AssertLwwCasesAsync(result);
        await AssertReceiveCasesAsync(result);
    }

    /// <summary>Verifies unknown scenarios fail with a nonzero exit code.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RunAsyncUnknownScenarioFails()
    {
        await using var writer = new StringWriter();

        var result = await ResilienceLabRunner.RunAsync(
            new("unknown"),
            writer,
            CancellationToken.None);

        await Assert.That(result.Succeeded).IsFalse();
        await Assert.That(result.ExitCode).IsEqualTo(1);
        await Assert.That(writer.ToString()).Contains("expected=crdt-loopback");
    }

    /// <summary>Asserts G-counter case results.</summary>
    /// <param name="result">The scenario result.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertGCounterCasesAsync(ResilienceLabRunResult result)
    {
        await AssertPassedCaseAsync(
            result,
            "gcounter.client-a-receives-authoritative-value",
            ExpectedGCounterValue,
            ExpectedGCounterValue);
        await AssertPassedCaseAsync(
            result,
            "gcounter.client-b-receives-authoritative-value",
            ExpectedGCounterValue,
            ExpectedGCounterValue);
        await AssertPassedCaseAsync(
            result,
            "gcounter.clients-share-authoritative-frontier",
            ExpectedSharedFrontier,
            ExpectedSharedFrontier);
        await AssertPassedCaseAsync(
            result,
            "gcounter.converges-and-replay-is-idempotent",
            ExpectedGCounterValue,
            ExpectedGCounterValue);
        await AssertPassedCaseAsync(
            result,
            "gcounter.received-history-forward-reverse-commutes",
            ExpectedGCounterValue,
            ExpectedGCounterValue);
    }

    /// <summary>Asserts PN-counter case results.</summary>
    /// <param name="result">The scenario result.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertPNCounterCasesAsync(ResilienceLabRunResult result)
    {
        await AssertPassedCaseAsync(
            result,
            "pncounter.client-a-receives-authoritative-value",
            ExpectedPNCounterValue,
            ExpectedPNCounterValue);
        await AssertPassedCaseAsync(
            result,
            "pncounter.client-b-receives-authoritative-value",
            ExpectedPNCounterValue,
            ExpectedPNCounterValue);
        await AssertPassedCaseAsync(
            result,
            "pncounter.clients-share-authoritative-frontier",
            ExpectedSharedFrontier,
            ExpectedSharedFrontier);
        await AssertPassedCaseAsync(
            result,
            "pncounter.converges-after-different-receive-order",
            ExpectedPNCounterValue,
            ExpectedPNCounterValue);
        await AssertPassedCaseAsync(
            result,
            "pncounter.received-history-forward-reverse-commutes",
            ExpectedPNCounterValue,
            ExpectedPNCounterValue);
    }

    /// <summary>Asserts OR-set case results.</summary>
    /// <param name="result">The scenario result.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertORSetCasesAsync(ResilienceLabRunResult result)
    {
        await AssertPassedCaseAsync(
            result,
            "orset.client-a-receives-authoritative-value",
            ExpectedORSetValue,
            ExpectedORSetValue);
        await AssertPassedCaseAsync(
            result,
            "orset.client-b-receives-authoritative-value",
            ExpectedORSetValue,
            ExpectedORSetValue);
        await AssertPassedCaseAsync(
            result,
            "orset.clients-share-authoritative-frontier",
            ExpectedSharedFrontier,
            ExpectedSharedFrontier);
        await AssertPassedCaseAsync(
            result,
            "orset.observed-remove-and-duplicate-add-do-not-resurrect",
            ExpectedORSetValue,
            ExpectedORSetValue);
        await AssertPassedCaseAsync(
            result,
            "orset.received-history-forward-reverse-commutes",
            ExpectedORSetValue,
            ExpectedORSetValue);
    }

    /// <summary>Asserts LWW register case results.</summary>
    /// <param name="result">The scenario result.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertLwwCasesAsync(ResilienceLabRunResult result)
    {
        await AssertPassedCaseAsync(
            result,
            "lww.client-a-receives-authoritative-value",
            ExpectedLwwValue,
            ExpectedLwwValue);
        await AssertPassedCaseAsync(
            result,
            "lww.client-b-receives-authoritative-value",
            ExpectedLwwValue,
            ExpectedLwwValue);
        await AssertPassedCaseAsync(
            result,
            "lww.clients-share-authoritative-frontier",
            ExpectedSharedFrontier,
            ExpectedSharedFrontier);
        await AssertPassedCaseAsync(
            result,
            "lww.later-server-stamp-wins-after-older-exact-retry",
            ExpectedLwwValue,
            ExpectedLwwValue);
        await AssertPassedCaseAsync(
            result,
            "lww.received-history-forward-reverse-commutes",
            ExpectedLwwValue,
            ExpectedLwwValue);
    }

    /// <summary>Asserts receive and duplicate case results.</summary>
    /// <param name="result">The scenario result.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertReceiveCasesAsync(ResilienceLabRunResult result)
    {
        await AssertPassedCaseAsync(
            result,
            "receive.client-a-acknowledges-authoritative-frontier",
            ExpectedAcknowledgedFrontier,
            ExpectedAcknowledgedFrontier);
        await AssertPassedCaseAsync(
            result,
            "receive.client-b-acknowledges-authoritative-frontier",
            ExpectedAcknowledgedFrontier,
            ExpectedAcknowledgedFrontier);
        await AssertPassedCaseAsync(
            result,
            "journal.duplicate-operation-adds-no-effect-group",
            0,
            0);
    }

    /// <summary>Asserts one passed invariant with typed expected and actual values.</summary>
    /// <typeparam name="T">The expected value type.</typeparam>
    /// <param name="result">The scenario result.</param>
    /// <param name="caseName">The case name.</param>
    /// <param name="expected">The expected value.</param>
    /// <param name="actual">The actual value.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertPassedCaseAsync<T>(
        ResilienceLabRunResult result,
        string caseName,
        T expected,
        T actual)
    {
        var item = FindCase(result, caseName);
        await Assert.That(item.Succeeded).IsTrue();
        await Assert.That(item.Expected).IsTypeOf<T>();
        await Assert.That(item.Actual).IsTypeOf<T>();
        await Assert.That((T)item.Expected).IsEqualTo(expected);
        await Assert.That((T)item.Actual).IsEqualTo(actual);
    }

    /// <summary>Finds one invariant case by name.</summary>
    /// <param name="result">The scenario result.</param>
    /// <param name="caseName">The case name.</param>
    /// <returns>The matching case.</returns>
    /// <exception cref="InvalidOperationException">The case was not present.</exception>
    private static ResilienceLabCaseResult FindCase(ResilienceLabRunResult result, string caseName)
    {
        for (var index = 0; index < result.Cases.Count; index++)
        {
            var item = result.Cases[index];
            if (string.Equals(item.Name, caseName, StringComparison.Ordinal))
            {
                return item;
            }
        }

        throw new InvalidOperationException($"Missing case '{caseName}'.");
    }
}
