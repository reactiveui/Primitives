// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Tests for <see cref="CrdtLoopbackAckReportBuilder"/>.</summary>
public sealed class CrdtLoopbackAckReportBuilderTests
{
    /// <summary>The expected case count.</summary>
    private const int ExpectedCaseCount = 2;

    /// <summary>Verifies client A failed ACK reporting is independent from client B.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task BuildCasesReportsClientAFailureIndependently()
    {
        var cases = CrdtLoopbackAckReportBuilder.BuildCases(clientAAcknowledged: false, clientBAcknowledged: true);

        await Assert.That(cases.Count).IsEqualTo(ExpectedCaseCount);
        await AssertCaseAsync(
            cases[0],
            CrdtLoopbackAckReportBuilder.ClientACaseName,
            CrdtLoopbackAckReportBuilder.NotAcknowledged,
            failed: true);
        await AssertCaseAsync(
            cases[1],
            CrdtLoopbackAckReportBuilder.ClientBCaseName,
            CrdtLoopbackAckReportBuilder.Acknowledged,
            failed: false);
    }

    /// <summary>Verifies client B failed ACK reporting is independent from client A.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task BuildCasesReportsClientBFailureIndependently()
    {
        var cases = CrdtLoopbackAckReportBuilder.BuildCases(clientAAcknowledged: true, clientBAcknowledged: false);

        await Assert.That(cases.Count).IsEqualTo(ExpectedCaseCount);
        await AssertCaseAsync(
            cases[0],
            CrdtLoopbackAckReportBuilder.ClientACaseName,
            CrdtLoopbackAckReportBuilder.Acknowledged,
            failed: false);
        await AssertCaseAsync(
            cases[1],
            CrdtLoopbackAckReportBuilder.ClientBCaseName,
            CrdtLoopbackAckReportBuilder.NotAcknowledged,
            failed: true);
    }

    /// <summary>Verifies both successful ACK reports preserve the expected and actual values.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task BuildCasesReportsBothAcknowledged()
    {
        var cases = CrdtLoopbackAckReportBuilder.BuildCases(clientAAcknowledged: true, clientBAcknowledged: true);

        await Assert.That(cases.Count).IsEqualTo(ExpectedCaseCount);
        await AssertCaseAsync(
            cases[0],
            CrdtLoopbackAckReportBuilder.ClientACaseName,
            CrdtLoopbackAckReportBuilder.Acknowledged,
            failed: false);
        await AssertCaseAsync(
            cases[1],
            CrdtLoopbackAckReportBuilder.ClientBCaseName,
            CrdtLoopbackAckReportBuilder.Acknowledged,
            failed: false);
    }

    /// <summary>Asserts one ACK case result.</summary>
    /// <param name="result">The case result.</param>
    /// <param name="name">The expected case name.</param>
    /// <param name="actual">The expected actual value.</param>
    /// <param name="failed">Whether the case should fail.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertCaseAsync(
        ResilienceLabCaseResult result,
        string name,
        string actual,
        bool failed)
    {
        await Assert.That(result.Name).IsEqualTo(name);
        await Assert.That(result.Expected).IsEqualTo(CrdtLoopbackAckReportBuilder.Acknowledged);
        await Assert.That(result.Actual).IsEqualTo(actual);
        await Assert.That(result.Succeeded).IsEqualTo(!failed);
    }
}
