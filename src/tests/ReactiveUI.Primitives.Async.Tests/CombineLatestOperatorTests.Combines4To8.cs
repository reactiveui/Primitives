// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for CombineLatestOperatorTests.</summary>
public partial class CombineLatestOperatorTests
{
    /// <summary>Tests CombineLatest with 4 sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestFourSources_ThenCombinesAll()
    {
        const int ExpectedSum = 1_111;
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();

        List<int> results = [];
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, s3.Values, s4.Values, static (a, b, c, d) => a + b + c + d)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Step1, CancellationToken.None);
        await s3.OnNextAsync(LargeStep1, CancellationToken.None);
        await s4.OnNextAsync(LargeStep3, CancellationToken.None);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => results.Count >= 1,
            TimeSpan.FromSeconds(WaitTimeoutSeconds));

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(ExpectedSum);
    }

    /// <summary>Tests CombineLatest with 5 sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestFiveSources_ThenCombinesAll()
    {
        const int ValueMultiplier = 10;
        const int ExpectedSum = 150;
        var signals = Enumerable.Range(0, FiveSources).Select(static _ => Signal.Create<int>()).ToList();

        List<int> results = [];
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                signals[Source4Index].Values,
                static (a, b, c, d, e) => a + b + c + d + e)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var i = 0; i < FiveSources; i++)
        {
            await signals[i].OnNextAsync((i + 1) * ValueMultiplier, CancellationToken.None);
        }

        await AsyncTestHelpers.WaitForConditionAsync(
            () => results.Count >= 1,
            TimeSpan.FromSeconds(WaitTimeoutSeconds));

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(ExpectedSum);
    }

    /// <summary>Tests CombineLatest with 6 sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestSixSources_ThenCombinesAll()
    {
        const int ExpectedSum = 21;
        var signals = Enumerable.Range(0, SixSources).Select(static _ => Signal.Create<int>()).ToList();

        List<int> results = [];
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                signals[Source4Index].Values,
                signals[Source5Index].Values,
                static (a, b, c, d, e, f) => a + b + c + d + e + f)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var i = 0; i < SixSources; i++)
        {
            await signals[i].OnNextAsync(i + 1, CancellationToken.None);
        }

        await AsyncTestHelpers.WaitForConditionAsync(
            () => results.Count >= 1,
            TimeSpan.FromSeconds(WaitTimeoutSeconds));

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(ExpectedSum);
    }

    /// <summary>Tests CombineLatest with 7 sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestSevenSources_ThenCombinesAll()
    {
        const int ExpectedSum = 7;
        var signals = Enumerable.Range(0, SevenSources).Select(static _ => Signal.Create<int>()).ToList();

        List<int> results = [];
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                signals[Source4Index].Values,
                signals[Source5Index].Values,
                signals[Source6Index].Values,
                static (a, b, c, d, e, f, g) => a + b + c + d + e + f + g)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var i = 0; i < SevenSources; i++)
        {
            await signals[i].OnNextAsync(1, CancellationToken.None);
        }

        await AsyncTestHelpers.WaitForConditionAsync(
            () => results.Count >= 1,
            TimeSpan.FromSeconds(WaitTimeoutSeconds));

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(ExpectedSum);
    }

    /// <summary>Tests CombineLatest with 8 sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Major Code Smell",
        "S107",
        Justification =
            "Arity-N CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatestEightSources_ThenCombinesAll()
    {
        const int ExpectedSum = 8;
        var signals = Enumerable.Range(0, EightSources).Select(static _ => Signal.Create<int>()).ToList();

        List<int> results = [];
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                signals[Source4Index].Values,
                signals[Source5Index].Values,
                signals[Source6Index].Values,
                signals[Source7Index].Values,
                static (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var i = 0; i < EightSources; i++)
        {
            await signals[i].OnNextAsync(1, CancellationToken.None);
        }

        await AsyncTestHelpers.WaitForConditionAsync(
            () => results.Count >= 1,
            TimeSpan.FromSeconds(WaitTimeoutSeconds));

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(ExpectedSum);
    }
}
