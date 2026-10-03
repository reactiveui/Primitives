// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Tests for <see cref="CrdtLoopbackHistoryComparison{T}"/>.</summary>
public sealed class CrdtLoopbackHistoryComparisonTests
{
    /// <summary>The expected projected value.</summary>
    private const string Expected = "blue";

    /// <summary>The client A forward divergent value.</summary>
    private const string ClientAForwardMismatch = "a-forward";

    /// <summary>The client A reverse divergent value.</summary>
    private const string ClientAReverseMismatch = "a-reverse";

    /// <summary>The client B forward divergent value.</summary>
    private const string ClientBForwardMismatch = "b-forward";

    /// <summary>The client B reverse divergent value.</summary>
    private const string ClientBReverseMismatch = "b-reverse";

    /// <summary>Verifies matching recorded values report the expected value.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetActualReturnsExpectedWhenEveryRecordedValueMatches()
    {
        var comparison = new CrdtLoopbackHistoryComparison<string>(Expected, Expected, Expected, Expected, Expected);

        var actual = comparison.GetActual(StringComparer.Ordinal);

        await Assert.That(actual).IsEqualTo(Expected);
    }

    /// <summary>Verifies the first recorded divergent value is reported as the actual diagnostic.</summary>
    /// <param name="clientAForward">The client A forward projected value.</param>
    /// <param name="clientAReverse">The client A reverse projected value.</param>
    /// <param name="clientBForward">The client B forward projected value.</param>
    /// <param name="clientBReverse">The client B reverse projected value.</param>
    /// <param name="expectedActual">The expected diagnostic actual value.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(ClientAForwardMismatch, Expected, Expected, Expected, ClientAForwardMismatch)]
    [Arguments(Expected, ClientAReverseMismatch, Expected, Expected, ClientAReverseMismatch)]
    [Arguments(Expected, Expected, ClientBForwardMismatch, Expected, ClientBForwardMismatch)]
    [Arguments(Expected, Expected, Expected, ClientBReverseMismatch, ClientBReverseMismatch)]
    public async Task GetActualReturnsFirstDivergentRecordedValue(
        string clientAForward,
        string clientAReverse,
        string clientBForward,
        string clientBReverse,
        string expectedActual)
    {
        var comparison = new CrdtLoopbackHistoryComparison<string>(
            Expected,
            clientAForward,
            clientAReverse,
            clientBForward,
            clientBReverse);

        var actual = comparison.GetActual(StringComparer.Ordinal);

        await Assert.That(actual).IsEqualTo(expectedActual);
    }
}
