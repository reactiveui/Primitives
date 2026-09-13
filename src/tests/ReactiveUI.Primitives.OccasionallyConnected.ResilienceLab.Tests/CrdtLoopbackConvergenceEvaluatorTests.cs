// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Tests for <see cref="CrdtLoopbackConvergenceEvaluator"/>.</summary>
public sealed class CrdtLoopbackConvergenceEvaluatorTests
{
    /// <summary>The expected converged value.</summary>
    private const string ExpectedValue = "blue";

    /// <summary>The client A cursor.</summary>
    private const string ClientACursor = "cursor-a";

    /// <summary>The client B cursor.</summary>
    private const string ClientBCursor = "cursor-b";

    /// <summary>The expected counter value.</summary>
    private const int ExpectedCounter = 8;

    /// <summary>The divergent counter value.</summary>
    private const int DivergentCounter = 7;

    /// <summary>The shared-frontier success text.</summary>
    private const string SameFrontier = "same";

    /// <summary>The divergent-frontier text.</summary>
    private const string DifferentFrontier = "different";

    /// <summary>Verifies equal string values do not hide a divergent authoritative frontier.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EvaluateStringReportsFrontierMismatchWhenValuesMatchExpectedButCursorsDiffer()
    {
        var clientA = new CrdtLoopbackConvergenceParticipant<string>(ExpectedValue, ClientACursor);
        var clientB = new CrdtLoopbackConvergenceParticipant<string>(ExpectedValue, ClientBCursor);

        var actual = CrdtLoopbackConvergenceEvaluator.EvaluateString(clientA, clientB, ExpectedValue);

        await Assert.That(actual).IsNotEqualTo(ExpectedValue);
        await Assert.That(actual).Contains(CrdtLoopbackConvergenceEvaluator.FrontierMismatchDiagnostic);
    }

    /// <summary>Verifies a counter value mismatch fails convergence even when the frontier matches.</summary>
    /// <param name="clientAValue">The client A value.</param>
    /// <param name="clientBValue">The client B value.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(DivergentCounter, ExpectedCounter)]
    [Arguments(ExpectedCounter, DivergentCounter)]
    public async Task EvaluateCounterReportsMismatchWhenEitherClientHasDifferentValue(
        int clientAValue,
        int clientBValue)
    {
        var clientA = new CrdtLoopbackConvergenceParticipant<int>(clientAValue, ClientACursor);
        var clientB = new CrdtLoopbackConvergenceParticipant<int>(clientBValue, ClientACursor);

        var actual = CrdtLoopbackConvergenceEvaluator.EvaluateCounter(clientA, clientB, ExpectedCounter);

        await Assert.That(actual).IsEqualTo(int.MinValue);
    }

    /// <summary>Verifies counter convergence requires a shared authoritative frontier.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EvaluateCounterReportsMismatchWhenCursorsDiffer()
    {
        var clientA = new CrdtLoopbackConvergenceParticipant<int>(ExpectedCounter, ClientACursor);
        var clientB = new CrdtLoopbackConvergenceParticipant<int>(ExpectedCounter, ClientBCursor);

        var actual = CrdtLoopbackConvergenceEvaluator.EvaluateCounter(clientA, clientB, ExpectedCounter);

        await Assert.That(actual).IsEqualTo(int.MinValue);
    }

    /// <summary>Verifies a string value mismatch reports the non-matching client value.</summary>
    /// <param name="clientAValue">The client A value.</param>
    /// <param name="clientBValue">The client B value.</param>
    /// <param name="expectedActual">The expected actual value.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("red", ExpectedValue, "red")]
    [Arguments(ExpectedValue, "red", "red")]
    public async Task EvaluateStringReportsMismatchedClientValue(
        string clientAValue,
        string clientBValue,
        string expectedActual)
    {
        var clientA = new CrdtLoopbackConvergenceParticipant<string>(clientAValue, ClientACursor);
        var clientB = new CrdtLoopbackConvergenceParticipant<string>(clientBValue, ClientACursor);

        var actual = CrdtLoopbackConvergenceEvaluator.EvaluateString(clientA, clientB, ExpectedValue);

        await Assert.That(actual).IsEqualTo(expectedActual);
    }

    /// <summary>Verifies a value mismatch remains visible when the clients also have different frontiers.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EvaluateStringReportsMismatchedClientValueWhenCursorAlsoDiffers()
    {
        var clientA = new CrdtLoopbackConvergenceParticipant<string>("red", ClientACursor);
        var clientB = new CrdtLoopbackConvergenceParticipant<string>(ExpectedValue, ClientBCursor);

        var actual = CrdtLoopbackConvergenceEvaluator.EvaluateString(clientA, clientB, ExpectedValue);

        await Assert.That(actual).IsEqualTo("red");
    }

    /// <summary>Verifies the frontier evaluator reports a divergent cursor.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EvaluateFrontierReportsDifferentCursors()
    {
        var clientA = new CrdtLoopbackConvergenceParticipant<int>(ExpectedCounter, ClientACursor);
        var clientB = new CrdtLoopbackConvergenceParticipant<int>(ExpectedCounter, ClientBCursor);

        var actual = CrdtLoopbackConvergenceEvaluator.EvaluateFrontier(
            clientA,
            clientB,
            SameFrontier,
            DifferentFrontier);

        await Assert.That(actual).IsEqualTo(DifferentFrontier);
    }
}
