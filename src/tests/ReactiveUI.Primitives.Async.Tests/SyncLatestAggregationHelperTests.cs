// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internal;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for <see cref="SyncLatestAggregationHelper"/>.</summary>
public sealed class SyncLatestAggregationHelperTests
{
    /// <summary>Minimum sample value.</summary>
    private const int MinSample = -4;

    /// <summary>Middle sample value.</summary>
    private const int MidSample = 3;

    /// <summary>Maximum sample value.</summary>
    private const int MaxSample = 12;

    /// <summary>Verifies boolean conjunction and extremum selectors evaluate lists directly.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Selectors_EvaluateConjunctionsAndExtrema()
    {
        await Assert.That(SyncLatestAggregationHelper.AllTrue([true, true])).IsTrue();
        await Assert.That(SyncLatestAggregationHelper.AllTrue([true, false])).IsFalse();
        await Assert.That(SyncLatestAggregationHelper.AllFalse([false, false])).IsTrue();
        await Assert.That(SyncLatestAggregationHelper.AllFalse([false, true])).IsFalse();
        await Assert.That(SyncLatestAggregationHelper.SelectMin([MidSample, MinSample, MaxSample])).IsEqualTo(MinSample);
        await Assert.That(SyncLatestAggregationHelper.SelectMax([MidSample, MaxSample, MinSample])).IsEqualTo(MaxSample);
    }

    /// <summary>
    /// Verifies <see cref="SyncLatestAggregationHelper.CreateAllMatching"/> and
    /// <see cref="SyncLatestAggregationHelper.CreateExtremum{T}"/> handle validation, empty inputs, and multi-source aggregation.
    /// </summary>
    /// <param name="cancellationToken">Token supplied by TUnit.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CreateAllMatchingAndCreateExtremum_ValidateAndAggregate(CancellationToken cancellationToken)
    {
        await Assert.That(static () => SyncLatestAggregationHelper.CreateAllMatching(null!, expected: true)).Throws<ArgumentNullException>();
        await Assert.That(static () => SyncLatestAggregationHelper.CreateExtremum<int>(null!, [], findMaximum: true)).Throws<ArgumentNullException>();
        await Assert.That(static () => SyncLatestAggregationHelper.CreateExtremum(SignalAsync.Return(1), null!, findMaximum: false)).Throws<ArgumentNullException>();

        var emptyAll = await SyncLatestAggregationHelper.CreateAllMatching([], expected: false).FirstAsync(cancellationToken);
        await Assert.That(emptyAll).IsTrue();

        var singleSource = SignalAsync.Return(MidSample);
        await Assert.That(SyncLatestAggregationHelper.CreateExtremum(singleSource, [], findMaximum: true)).IsSameReferenceAs(singleSource);

        var maxVal = await SyncLatestAggregationHelper.CreateExtremum(
            SignalAsync.Return(MidSample),
            [SignalAsync.Return(MinSample), SignalAsync.Return(MaxSample)],
            findMaximum: true).FirstAsync(cancellationToken);
        var minVal = await SyncLatestAggregationHelper.CreateExtremum(
            SignalAsync.Return(MidSample),
            [SignalAsync.Return(MinSample), SignalAsync.Return(MaxSample)],
            findMaximum: false).FirstAsync(cancellationToken);

        await Assert.That(maxVal).IsEqualTo(MaxSample);
        await Assert.That(minVal).IsEqualTo(MinSample);
    }
}
