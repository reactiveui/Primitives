// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Tests for the arity-3 through arity-7 <c>SyncLatest</c> spelling of the combine-latest operator. The
/// <c>CombineLatest</c> spelling of these overloads is exercised elsewhere; these cover the <c>SyncLatest</c>
/// alias methods, which forward to the same signal. Each source contributes a value of one, so the projected
/// result equals the arity once every source has produced a value.
/// </summary>
public partial class SyncLatestOperatorTests
{
    /// <summary>Number of sources combined by the arity-3 overload.</summary>
    private const int ArityThree = 3;

    /// <summary>Number of sources combined by the arity-4 overload.</summary>
    private const int ArityFour = 4;

    /// <summary>Number of sources combined by the arity-5 overload.</summary>
    private const int ArityFive = 5;

    /// <summary>Number of sources combined by the arity-6 overload.</summary>
    private const int AritySix = 6;

    /// <summary>Number of sources combined by the arity-7 overload.</summary>
    private const int AritySeven = 7;

    /// <summary>Verifies the arity-3 <c>SyncLatest</c> projects the latest value of all three sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncLatestThreeSources_ThenCombinesAll()
    {
        var signals = Enumerable.Range(0, ArityThree).Select(static _ => Signal.Create<int>()).ToList();

        List<int> results = [];
        await using var sub = await signals[0].Values
            .SyncLatest(
                signals[1].Values,
                signals[SourceIndex2].Values,
                static (a, b, c) => a + b + c)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var index = 0; index < ArityThree; index++)
        {
            await signals[index].OnNextAsync(1, CancellationToken.None);
        }

        await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 1, WaitTimeout);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(ArityThree);
    }

    /// <summary>Verifies the arity-4 <c>SyncLatest</c> projects the latest value of all four sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncLatestFourSources_ThenCombinesAll()
    {
        var signals = Enumerable.Range(0, ArityFour).Select(static _ => Signal.Create<int>()).ToList();

        List<int> results = [];
        await using var sub = await signals[0].Values
            .SyncLatest(
                signals[1].Values,
                signals[SourceIndex2].Values,
                signals[SourceIndex3].Values,
                static (a, b, c, d) => a + b + c + d)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var index = 0; index < ArityFour; index++)
        {
            await signals[index].OnNextAsync(1, CancellationToken.None);
        }

        await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 1, WaitTimeout);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(ArityFour);
    }

    /// <summary>Verifies the arity-5 <c>SyncLatest</c> projects the latest value of all five sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncLatestFiveSources_ThenCombinesAll()
    {
        var signals = Enumerable.Range(0, ArityFive).Select(static _ => Signal.Create<int>()).ToList();

        List<int> results = [];
        await using var sub = await signals[0].Values
            .SyncLatest(
                signals[1].Values,
                signals[SourceIndex2].Values,
                signals[SourceIndex3].Values,
                signals[SourceIndex4].Values,
                static (a, b, c, d, e) => a + b + c + d + e)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var index = 0; index < ArityFive; index++)
        {
            await signals[index].OnNextAsync(1, CancellationToken.None);
        }

        await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 1, WaitTimeout);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(ArityFive);
    }

    /// <summary>Verifies the arity-6 <c>SyncLatest</c> projects the latest value of all six sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncLatestSixSources_ThenCombinesAll()
    {
        var signals = Enumerable.Range(0, AritySix).Select(static _ => Signal.Create<int>()).ToList();

        List<int> results = [];
        await using var sub = await signals[0].Values
            .SyncLatest(
                signals[1].Values,
                signals[SourceIndex2].Values,
                signals[SourceIndex3].Values,
                signals[SourceIndex4].Values,
                signals[SourceIndex5].Values,
                static (a, b, c, d, e, f) => a + b + c + d + e + f)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var index = 0; index < AritySix; index++)
        {
            await signals[index].OnNextAsync(1, CancellationToken.None);
        }

        await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 1, WaitTimeout);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(AritySix);
    }

    /// <summary>Verifies the arity-7 <c>SyncLatest</c> projects the latest value of all seven sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncLatestSevenSources_ThenCombinesAll()
    {
        var signals = Enumerable.Range(0, AritySeven).Select(static _ => Signal.Create<int>()).ToList();

        List<int> results = [];
        await using var sub = await signals[0].Values
            .SyncLatest(
                signals[1].Values,
                signals[SourceIndex2].Values,
                signals[SourceIndex3].Values,
                signals[SourceIndex4].Values,
                signals[SourceIndex5].Values,
                signals[SourceIndex6].Values,
                static (a, b, c, d, e, f, g) => a + b + c + d + e + f + g)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var index = 0; index < AritySeven; index++)
        {
            await signals[index].OnNextAsync(1, CancellationToken.None);
        }

        await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 1, WaitTimeout);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(AritySeven);
    }
}
