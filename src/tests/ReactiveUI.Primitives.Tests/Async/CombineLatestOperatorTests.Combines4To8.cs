// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.SystemReactiveBridge;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Async.Subjects;

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
        var s1 = SubjectAsync.Create<int>();
        var s2 = SubjectAsync.Create<int>();
        var s3 = SubjectAsync.Create<int>();
        var s4 = SubjectAsync.Create<int>();

        var results = new List<int>();
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, s3.Values, s4.Values, (a, b, c, d) => a + b + c + d)
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
        var subjects = Enumerable.Range(0, FiveSources).Select(_ => SubjectAsync.Create<int>()).ToList();

        var results = new List<int>();
        await using var sub = await subjects[0].Values
            .CombineLatest(
                subjects[1].Values,
                subjects[Source2Index].Values,
                subjects[Source3Index].Values,
                subjects[Source4Index].Values,
                (a, b, c, d, e) => a + b + c + d + e)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var i = 0; i < FiveSources; i++)
        {
            await subjects[i].OnNextAsync((i + 1) * ValueMultiplier, CancellationToken.None);
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
        var subjects = Enumerable.Range(0, SixSources).Select(_ => SubjectAsync.Create<int>()).ToList();

        var results = new List<int>();
        await using var sub = await subjects[0].Values
            .CombineLatest(
                subjects[1].Values,
                subjects[Source2Index].Values,
                subjects[Source3Index].Values,
                subjects[Source4Index].Values,
                subjects[Source5Index].Values,
                (a, b, c, d, e, f) => a + b + c + d + e + f)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var i = 0; i < SixSources; i++)
        {
            await subjects[i].OnNextAsync(i + 1, CancellationToken.None);
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
        var subjects = Enumerable.Range(0, SevenSources).Select(_ => SubjectAsync.Create<int>()).ToList();

        var results = new List<int>();
        await using var sub = await subjects[0].Values
            .CombineLatest(
                subjects[1].Values,
                subjects[Source2Index].Values,
                subjects[Source3Index].Values,
                subjects[Source4Index].Values,
                subjects[Source5Index].Values,
                subjects[Source6Index].Values,
                (a, b, c, d, e, f, g) => a + b + c + d + e + f + g)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var i = 0; i < SevenSources; i++)
        {
            await subjects[i].OnNextAsync(1, CancellationToken.None);
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
    [SuppressMessage("Major Code Smell", "S107", Justification = "Arity-N CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatestEightSources_ThenCombinesAll()
    {
        const int ExpectedSum = 8;
        var subjects = Enumerable.Range(0, EightSources).Select(_ => SubjectAsync.Create<int>()).ToList();

        var results = new List<int>();
        await using var sub = await subjects[0].Values
            .CombineLatest(
                subjects[1].Values,
                subjects[Source2Index].Values,
                subjects[Source3Index].Values,
                subjects[Source4Index].Values,
                subjects[Source5Index].Values,
                subjects[Source6Index].Values,
                subjects[Source7Index].Values,
                (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var i = 0; i < EightSources; i++)
        {
            await subjects[i].OnNextAsync(1, CancellationToken.None);
        }

        await AsyncTestHelpers.WaitForConditionAsync(
            () => results.Count >= 1,
            TimeSpan.FromSeconds(WaitTimeoutSeconds));

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(ExpectedSum);
    }
}
