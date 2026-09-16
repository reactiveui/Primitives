// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpReplayRetainedSizeCalculator"/>.</summary>
public sealed class HttpReplayRetainedSizeCalculatorTests
{
    /// <summary>The first small retained byte count.</summary>
    private const int FirstByteCount = 7;

    /// <summary>The second small retained byte count.</summary>
    private const int SecondByteCount = 11;

    /// <summary>The third small retained byte count.</summary>
    private const int ThirdByteCount = 13;

    /// <summary>The fourth small retained byte count.</summary>
    private const int FourthByteCount = 17;

    /// <summary>A large component count that contributes to wraparound before widening.</summary>
    private const int LargeByteCount = int.MaxValue;

    /// <summary>The first small component that keeps the wrapped total positive.</summary>
    private const int WrappedPositiveFirstByteCount = 100;

    /// <summary>The second small component that keeps the wrapped total positive.</summary>
    private const int WrappedPositiveSecondByteCount = 101;

    /// <summary>The finite budget smaller than the true aggregate component count.</summary>
    private const long FiniteBudget = 200;

    /// <summary>The zero retained byte count.</summary>
    private const long ZeroBytes = 0;

    /// <summary>The expected small retained byte total.</summary>
    private const long ExpectedSmallTotal = FirstByteCount + SecondByteCount + ThirdByteCount + FourthByteCount;

    /// <summary>The expected large retained byte total.</summary>
    private const long ExpectedLargeTotal = (long)int.MaxValue + FirstByteCount + SecondByteCount + ThirdByteCount;

    /// <summary>Verifies ordinary retained byte counts are summed exactly.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SumTextBytesAddsOrdinaryComponentCounts()
    {
        var total = HttpReplayRetainedSizeCalculator.SumTextBytes(FirstByteCount, SecondByteCount, ThirdByteCount, FourthByteCount);

        await Assert.That(total).IsEqualTo(ExpectedSmallTotal);
    }

    /// <summary>Verifies aggregate retained byte counts do not wrap before widening to <see cref="long"/>.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SumTextBytesKeepsAggregateInLongRange()
    {
        var total = HttpReplayRetainedSizeCalculator.SumTextBytes(int.MaxValue, FirstByteCount, SecondByteCount, ThirdByteCount);

        await Assert.That(total).IsEqualTo(ExpectedLargeTotal);
    }

    /// <summary>Verifies wrapped large component sums cannot reserve finite retained-byte capacity.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SumTextBytesReserveRejectsTrueAggregateAboveBudget()
    {
        HttpReplayRetentionBudget budget = new(FiniteBudget);
        var total = HttpReplayRetainedSizeCalculator.SumTextBytes(
            LargeByteCount,
            LargeByteCount,
            WrappedPositiveFirstByteCount,
            WrappedPositiveSecondByteCount);

        await Assert.That(() => budget.Reserve(total)).ThrowsExactly<HttpRemoteTransportException>();
        await Assert.That(budget.RetainedBytes).IsEqualTo(ZeroBytes);
    }
}
