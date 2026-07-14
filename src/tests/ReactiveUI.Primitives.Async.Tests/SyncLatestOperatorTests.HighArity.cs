// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for the arity-12 through arity-16 <c>SyncLatest</c> overloads.</summary>
public partial class SyncLatestOperatorTests
{
    /// <summary>Verifies the arity-12 <c>SyncLatest</c> projects the latest value of all twelve sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncLatestTwelveSources_ThenCombinesAll()
    {
        var signals = Enumerable.Range(0, ArityTwelve).Select(static _ => Signal.Create<int>()).ToList();

        List<int> results = [];
        await using var sub = await signals[0].Values
            .SyncLatest(
                signals[1].Values,
                signals[SourceIndex2].Values,
                signals[SourceIndex3].Values,
                signals[SourceIndex4].Values,
                signals[SourceIndex5].Values,
                signals[SourceIndex6].Values,
                signals[SourceIndex7].Values,
                signals[SourceIndex8].Values,
                signals[SourceIndex9].Values,
                signals[SourceIndex10].Values,
                signals[SourceIndex11].Values,
                static (a, b, c, d, e, f, g, h, i, j, k, l) => a + b + c + d + e + f + g + h + i + j + k + l)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var index = 0; index < ArityTwelve; index++)
        {
            await signals[index].OnNextAsync(1, CancellationToken.None);
        }

        await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 1, WaitTimeout);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(ArityTwelve);
    }

    /// <summary>Verifies the arity-13 <c>SyncLatest</c> projects the latest value of all thirteen sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncLatestThirteenSources_ThenCombinesAll()
    {
        var signals = Enumerable.Range(0, ArityThirteen).Select(static _ => Signal.Create<int>()).ToList();

        List<int> results = [];
        await using var sub = await signals[0].Values
            .SyncLatest(
                signals[1].Values,
                signals[SourceIndex2].Values,
                signals[SourceIndex3].Values,
                signals[SourceIndex4].Values,
                signals[SourceIndex5].Values,
                signals[SourceIndex6].Values,
                signals[SourceIndex7].Values,
                signals[SourceIndex8].Values,
                signals[SourceIndex9].Values,
                signals[SourceIndex10].Values,
                signals[SourceIndex11].Values,
                signals[SourceIndex12].Values,
                static (a, b, c, d, e, f, g, h, i, j, k, l, m) => a + b + c + d + e + f + g + h + i + j + k + l + m)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var index = 0; index < ArityThirteen; index++)
        {
            await signals[index].OnNextAsync(1, CancellationToken.None);
        }

        await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 1, WaitTimeout);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(ArityThirteen);
    }

    /// <summary>Verifies the arity-14 <c>SyncLatest</c> projects the latest value of all fourteen sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncLatestFourteenSources_ThenCombinesAll()
    {
        var signals = Enumerable.Range(0, ArityFourteen).Select(static _ => Signal.Create<int>()).ToList();

        List<int> results = [];
        await using var sub = await signals[0].Values
            .SyncLatest(
                signals[1].Values,
                signals[SourceIndex2].Values,
                signals[SourceIndex3].Values,
                signals[SourceIndex4].Values,
                signals[SourceIndex5].Values,
                signals[SourceIndex6].Values,
                signals[SourceIndex7].Values,
                signals[SourceIndex8].Values,
                signals[SourceIndex9].Values,
                signals[SourceIndex10].Values,
                signals[SourceIndex11].Values,
                signals[SourceIndex12].Values,
                signals[SourceIndex13].Values,
                static (a, b, c, d, e, f, g, h, i, j, k, l, m, n) =>
                    a + b + c + d + e + f + g + h + i + j + k + l + m + n)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var index = 0; index < ArityFourteen; index++)
        {
            await signals[index].OnNextAsync(1, CancellationToken.None);
        }

        await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 1, WaitTimeout);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(ArityFourteen);
    }

    /// <summary>Verifies the arity-15 <c>SyncLatest</c> projects the latest value of all fifteen sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncLatestFifteenSources_ThenCombinesAll()
    {
        var signals = Enumerable.Range(0, ArityFifteen).Select(static _ => Signal.Create<int>()).ToList();

        List<int> results = [];
        await using var sub = await signals[0].Values
            .SyncLatest(
                signals[1].Values,
                signals[SourceIndex2].Values,
                signals[SourceIndex3].Values,
                signals[SourceIndex4].Values,
                signals[SourceIndex5].Values,
                signals[SourceIndex6].Values,
                signals[SourceIndex7].Values,
                signals[SourceIndex8].Values,
                signals[SourceIndex9].Values,
                signals[SourceIndex10].Values,
                signals[SourceIndex11].Values,
                signals[SourceIndex12].Values,
                signals[SourceIndex13].Values,
                signals[SourceIndex14].Values,
                static (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o) =>
                    a + b + c + d + e + f + g + h + i + j + k + l + m + n + o)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var index = 0; index < ArityFifteen; index++)
        {
            await signals[index].OnNextAsync(1, CancellationToken.None);
        }

        await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 1, WaitTimeout);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(ArityFifteen);
    }

    /// <summary>Verifies the arity-16 <c>SyncLatest</c> projects the latest value of all sixteen sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncLatestSixteenSources_ThenCombinesAll()
    {
        var signals = Enumerable.Range(0, AritySixteen).Select(static _ => Signal.Create<int>()).ToList();

        List<int> results = [];
        await using var sub = await signals[0].Values
            .SyncLatest(
                signals[1].Values,
                signals[SourceIndex2].Values,
                signals[SourceIndex3].Values,
                signals[SourceIndex4].Values,
                signals[SourceIndex5].Values,
                signals[SourceIndex6].Values,
                signals[SourceIndex7].Values,
                signals[SourceIndex8].Values,
                signals[SourceIndex9].Values,
                signals[SourceIndex10].Values,
                signals[SourceIndex11].Values,
                signals[SourceIndex12].Values,
                signals[SourceIndex13].Values,
                signals[SourceIndex14].Values,
                signals[SourceIndex15].Values,
                static (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) =>
                    a + b + c + d + e + f + g + h + i + j + k + l + m + n + o + p)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var index = 0; index < AritySixteen; index++)
        {
            await signals[index].OnNextAsync(1, CancellationToken.None);
        }

        await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 1, WaitTimeout);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(AritySixteen);
    }
}
