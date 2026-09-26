// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Tests for <see cref="ResilienceLabRunner"/>.</summary>
public sealed class ResilienceLabRunnerTests
{
    /// <summary>The durable HTTP lost acknowledgement scenario name.</summary>
    private const string DurableHttpLostAckScenarioName = "durable-http-lost-ack";

    /// <summary>The deterministic retry-backoff scenario name.</summary>
    private const string RetryBackoffScenarioName = "retry-backoff";

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

    /// <summary>The expected final operation state after durable retry recovery.</summary>
    private const string ExpectedSynchronizedState = "Synchronized";

    /// <summary>The minimum attempt count proving one lost ACK retry.</summary>
    private const int ExpectedMinimumLostAckAttempts = 2;

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

    /// <summary>Verifies the durable HTTP lost-ACK scenario reports each required recovery proof.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RunAsyncDurableHttpLostAckReportsRequiredRecoveryProofs()
    {
        await using var writer = new StringWriter();

        var result = await ResilienceLabRunner.RunAsync(
            new(DurableHttpLostAckScenarioName),
            writer,
            CancellationToken.None);

        await AssertDurableHttpLostAckRunnerEnvelopeAsync(result);
        await AssertDurableHttpLostAckRetryProofsAsync(result);
        await AssertDurableHttpLostAckRecoveryProofsAsync(result);
        await Assert.That(writer.ToString()).Contains("durable-http-lost-ack.server-effect-count: expected=1; actual=1; passed=True");
    }

    /// <summary>Verifies the retry-backoff scenario proves bounded scheduling and persisted attempts.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RunAsyncRetryBackoffReportsBoundedScheduling()
    {
        await using var writer = new StringWriter();

        var result = await ResilienceLabRunner.RunAsync(
            new(RetryBackoffScenarioName),
            writer,
            CancellationToken.None);

        await Assert.That(result.Scenario).IsEqualTo(RetryBackoffScenarioName);
        await Assert.That(result.Succeeded).IsTrue();
        await Assert.That(result.ExitCode).IsEqualTo(0);
        await Assert.That(writer.ToString()).Contains("retry-backoff.server-hint-applied");
        await Assert.That(writer.ToString()).Contains("retry-backoff.attempts-exhausted");
    }

    /// <summary>Asserts the durable HTTP lost-ACK scenario runner envelope.</summary>
    /// <param name="result">The scenario result.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertDurableHttpLostAckRunnerEnvelopeAsync(ResilienceLabRunResult result)
    {
        var failures = string.Join(
            "; ",
            result.Cases.Where(static item => !item.Succeeded)
                .Select(static item => $"{item.Name}: expected={item.Expected}, actual={item.Actual}"));
        await Assert.That(result.Scenario).IsEqualTo(DurableHttpLostAckScenarioName);
        await Assert.That(result.Succeeded).IsTrue().Because(failures);
        await Assert.That(result.ExitCode).IsEqualTo(0);
    }

    /// <summary>Asserts the durable HTTP lost-ACK retry and server-effect proofs.</summary>
    /// <param name="result">The scenario result.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertDurableHttpLostAckRetryProofsAsync(ResilienceLabRunResult result)
    {
        await AssertPassedCaseAsync(
            result,
            "durable-http-lost-ack.first-ack-lost",
            true,
            true);
        await AssertPassedCaseAsync(
            result,
            "durable-http-lost-ack.server-effect-before-ack-loss",
            1,
            1);
        await AssertDurableHttpLostAckOperationIdentityAsync(result);
        await AssertPassedCaseAsync(
            result,
            "durable-http-lost-ack.client-sequence-persisted",
            1L,
            1L);
        await AssertPassedCaseAsync(
            result,
            "durable-http-lost-ack.persisted-pending-before-reopen",
            "pending",
            "pending");
        await AssertPassedCaseAsync(
            result,
            "durable-http-lost-ack.pending-survives-client-close",
            true,
            true);
        await AssertNumericCaseAtLeastAsync(
            result,
            "durable-http-lost-ack.push-attempts",
            ExpectedMinimumLostAckAttempts);
        await AssertPassedCaseAsync(
            result,
            "durable-http-lost-ack.server-effect-count",
            1,
            1);
        await AssertNumericCaseAtLeastAsync(
            result,
            "durable-http-lost-ack.client-attempt-count",
            ExpectedMinimumLostAckAttempts);
    }

    /// <summary>Asserts the original, persisted and retried operation ids are the same concrete id.</summary>
    /// <param name="result">The scenario result.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertDurableHttpLostAckOperationIdentityAsync(ResilienceLabRunResult result)
    {
        var persistedProof = FindCase(result, "durable-http-lost-ack.operation-id-reused");
        var retriedProof = FindCase(result, "durable-http-lost-ack.retry-uses-persisted-operation-id");

        await Assert.That(persistedProof.Succeeded).IsTrue();
        await Assert.That(retriedProof.Succeeded).IsTrue();
        await Assert.That(persistedProof.Expected).IsTypeOf<string>();
        await Assert.That(persistedProof.Actual).IsTypeOf<string>();
        await Assert.That(retriedProof.Expected).IsTypeOf<string>();
        await Assert.That(retriedProof.Actual).IsTypeOf<string>();

        var originalOperationId = (string)persistedProof.Expected;
        var persistedOperationId = (string)persistedProof.Actual;
        var retrySourceOperationId = (string)retriedProof.Expected;
        var retriedOperationId = (string)retriedProof.Actual;

        await Assert.That(originalOperationId).IsNotEqualTo(string.Empty);
        await Assert.That(persistedOperationId).IsEqualTo(originalOperationId);
        await Assert.That(retrySourceOperationId).IsEqualTo(persistedOperationId);
        await Assert.That(retriedOperationId).IsEqualTo(persistedOperationId);
    }

    /// <summary>Asserts the durable HTTP lost-ACK final recovery and notification proofs.</summary>
    /// <param name="result">The scenario result.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertDurableHttpLostAckRecoveryProofsAsync(ResilienceLabRunResult result)
    {
        await AssertPassedCaseAsync(
            result,
            "durable-http-lost-ack.client-final-status",
            ExpectedSynchronizedState,
            ExpectedSynchronizedState);
        await AssertPassedCaseAsync(
            result,
            "durable-http-lost-ack.writer-pending-after-retry",
            0,
            0);
        await AssertPassedCaseAsync(
            result,
            "durable-http-lost-ack.subscription-id-stable",
            "same",
            "same");
        await AssertPassedCaseAsync(
            result,
            "durable-http-lost-ack.cursor-restored-and-advanced",
            "advanced",
            "advanced");
        await AssertPassedCaseAsync(
            result,
            "durable-http-lost-ack.snapshot-restored",
            "restored",
            "restored");
        await AssertNumericCaseAtLeastAsync(
            result,
            "durable-http-lost-ack.remote-notification-observed",
            1);
        await AssertPassedCaseAsync(
            result,
            "durable-http-lost-ack.observer-inbox-effect-count",
            1,
            1);
        await AssertPassedCaseAsync(
            result,
            "durable-http-lost-ack.observer-final-counter",
            1L,
            1L);
        await AssertPassedCaseAsync(
            result,
            "durable-http-lost-ack.observer-subscription-distinct",
            "distinct",
            "distinct");
        await AssertPassedCaseAsync(
            result,
            "durable-http-lost-ack.no-terminal-faults",
            0,
            0);
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

    /// <summary>Asserts one passed invariant whose actual numeric value must be at least the expected minimum.</summary>
    /// <param name="result">The scenario result.</param>
    /// <param name="caseName">The case name.</param>
    /// <param name="minimum">The expected minimum value.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertNumericCaseAtLeastAsync(
        ResilienceLabRunResult result,
        string caseName,
        int minimum)
    {
        var item = FindCase(result, caseName);
        await Assert.That(item.Succeeded).IsTrue();
        await Assert.That(item.Expected).IsTypeOf<int>();
        await Assert.That(item.Actual).IsTypeOf<int>();
        await Assert.That((int)item.Expected).IsEqualTo(minimum);
        await Assert.That((int)item.Actual).IsGreaterThanOrEqualTo(minimum);
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
