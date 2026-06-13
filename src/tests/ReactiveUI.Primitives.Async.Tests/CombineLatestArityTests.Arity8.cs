// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using ReactiveUI.Primitives.Async.Signals;
using AsyncObs = ReactiveUI.Primitives.Async.SignalAsync;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for CombineLatestArityTests.</summary>
public partial class CombineLatestArityTests
{
    /// <summary>Verifies that CombineLatest8 disposes on subscription failure (catch block).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107:Methods should not have too many parameters", Justification = "Test Reasons")]
    public async Task WhenCombineLatest8SubscriptionThrows_ThenDisposesAndRethrows()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var throwingSrc = AsyncObs.Create<int>((_, _) => throw new InvalidOperationException("subscribe failed"));
        await Assert.That(async () => await s1.Values.CombineLatest(s2.Values, s3.Values, s4.Values, s5.Values, s6.Values, s7.Values, throwingSrc, (v1, v2, v3, v4, v5, v6, v7, v8) => v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8).SubscribeAsync((_, _) => default, null)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies that CombineLatest8 OnNextCombined guard returns when disposed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107:Methods should not have too many parameters", Justification = "Test Reasons")]
    public async Task WhenCombineLatest8DisposedBeforeCombine_ThenOnNextCombinedIsGuarded()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();
        var results = new List<int>();
        var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, s4.Values, s5.Values, s6.Values, s7.Values, s8.Values, (v1, v2, v3, v4, v5, v6, v7, v8) => v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8).SubscribeAsync(
            (x, _) =>
        {
            results.Add(x);
            return default;
        },
            null);
        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(SeedValue2, CancellationToken.None);
        await s3.OnNextAsync(SeedValue3, CancellationToken.None);
        await s4.OnNextAsync(SeedValue4, CancellationToken.None);
        await s5.OnNextAsync(SeedValue5, CancellationToken.None);
        await s6.OnNextAsync(SeedValue6, CancellationToken.None);
        await s7.OnNextAsync(SeedValue7, CancellationToken.None);
        await s8.OnNextAsync(SeedValue8, CancellationToken.None);
        await sub.DisposeAsync();
        await s1.OnNextAsync(PostDisposeValue, CancellationToken.None);
        await Assert.That(results).Count().IsEqualTo(1);
    }

    /// <summary>Verifies that CombineLatest8 OnErrorResume guard returns when disposed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107:Methods should not have too many parameters", Justification = "Test Reasons")]
    public async Task WhenCombineLatest8DisposedBeforeError_ThenOnErrorResumeIsGuarded()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();
        Exception? receivedError = null;
        var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, s4.Values, s5.Values, s6.Values, s7.Values, s8.Values, (v1, v2, v3, v4, v5, v6, v7, v8) => v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8).SubscribeAsync((_, _) => default, (ex, _) =>
        {
            receivedError = ex;
            return default;
        });
        await sub.DisposeAsync();
        await s1.OnErrorResumeAsync(new InvalidOperationException("post-dispose error"), CancellationToken.None);
        await Assert.That(receivedError).IsNull();
    }

    /// <summary>Verifies that CombineLatest8 OnNext for the last source returns early when not all sources have values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107:Methods should not have too many parameters", Justification = "Test Reasons")]
    public async Task WhenCombineLatest8LastSourceEmitsFirst_ThenNoEmission()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();
        var results = new List<int>();
        await using var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, s4.Values, s5.Values, s6.Values, s7.Values, s8.Values, (v1, v2, v3, v4, v5, v6, v7, v8) => v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8).SubscribeAsync(
            (x, _) =>
        {
            results.Add(x);
            return default;
        },
            null);
        await s8.OnNextAsync(SentinelValue1, CancellationToken.None);
        await Assert.That(results).IsEmpty();
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
        await s8.OnCompletedAsync(Result.Success);
    }

    /// <summary>Verifies that CombineLatest8 OnNext for middle sources returns early when not all sources have values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107:Methods should not have too many parameters", Justification = "Test Reasons")]
    public async Task WhenCombineLatest8MiddleSourcesEmitFirst_ThenNoEmission()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();
        var results = new List<int>();
        await using var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, s4.Values, s5.Values, s6.Values, s7.Values, s8.Values, (v1, v2, v3, v4, v5, v6, v7, v8) => v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8).SubscribeAsync(
            (x, _) =>
        {
            results.Add(x);
            return default;
        },
            null);
        await s2.OnNextAsync(SentinelValue1, CancellationToken.None);
        await s3.OnNextAsync(SentinelValue2, CancellationToken.None);
        await s4.OnNextAsync(SentinelValue3, CancellationToken.None);
        await s5.OnNextAsync(SentinelValue4, CancellationToken.None);
        await s6.OnNextAsync(SentinelValue5, CancellationToken.None);
        await s7.OnNextAsync(SentinelValue6, CancellationToken.None);
        await Assert.That(results).IsEmpty();
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
        await s8.OnCompletedAsync(Result.Success);
    }

    /// <summary>Verifies that CombineLatest8 OnNext_1 calls OnNextCombined when source 1 re-emits after all values are present.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107:Methods should not have too many parameters", Justification = "Test Reasons")]
    public async Task WhenCombineLatest8Source1ReEmits_ThenOnNextCombinedViaOnNext1()
    {
        const int ExpectedSum11111112 = 11_111_112;
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();
        var results = new List<int>();
        await using var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, s4.Values, s5.Values, s6.Values, s7.Values, s8.Values, (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h).SubscribeAsync(
            (x, _) =>
        {
            results.Add(x);
            return default;
        },
            null);
        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(PlaceValue1, CancellationToken.None);
        await s3.OnNextAsync(PlaceValue2, CancellationToken.None);
        await s4.OnNextAsync(PlaceValue3, CancellationToken.None);
        await s5.OnNextAsync(PlaceValue4, CancellationToken.None);
        await s6.OnNextAsync(PlaceValue5, CancellationToken.None);
        await s7.OnNextAsync(PlaceValue6, CancellationToken.None);
        await s8.OnNextAsync(PlaceValue7, CancellationToken.None);
        await s1.OnNextAsync(SeedValue2, CancellationToken.None);
        await Assert.That(results[^1]).IsEqualTo(ExpectedSum11111112);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
        await s8.OnCompletedAsync(Result.Success);
    }

    /// <summary>Verifies that CombineLatest8 OnNext_2 calls OnNextCombined when source 2 re-emits after all values are present.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107:Methods should not have too many parameters", Justification = "Test Reasons")]
    public async Task WhenCombineLatest8Source2ReEmits_ThenOnNextCombinedViaOnNext2()
    {
        const int ExpectedSum11111121 = 11_111_121;
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();
        var results = new List<int>();
        await using var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, s4.Values, s5.Values, s6.Values, s7.Values, s8.Values, (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h).SubscribeAsync(
            (x, _) =>
        {
            results.Add(x);
            return default;
        },
            null);
        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(PlaceValue1, CancellationToken.None);
        await s3.OnNextAsync(PlaceValue2, CancellationToken.None);
        await s4.OnNextAsync(PlaceValue3, CancellationToken.None);
        await s5.OnNextAsync(PlaceValue4, CancellationToken.None);
        await s6.OnNextAsync(PlaceValue5, CancellationToken.None);
        await s7.OnNextAsync(PlaceValue6, CancellationToken.None);
        await s8.OnNextAsync(PlaceValue7, CancellationToken.None);
        await s2.OnNextAsync(ReEmitValue2, CancellationToken.None);
        await Assert.That(results[^1]).IsEqualTo(ExpectedSum11111121);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
        await s8.OnCompletedAsync(Result.Success);
    }

    /// <summary>Verifies that CombineLatest8 OnNext_3 calls OnNextCombined when source 3 re-emits after all values are present.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107:Methods should not have too many parameters", Justification = "Test Reasons")]
    public async Task WhenCombineLatest8Source3ReEmits_ThenOnNextCombinedViaOnNext3()
    {
        const int ExpectedSum11111211 = 11_111_211;
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();
        var results = new List<int>();
        await using var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, s4.Values, s5.Values, s6.Values, s7.Values, s8.Values, (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h).SubscribeAsync(
            (x, _) =>
        {
            results.Add(x);
            return default;
        },
            null);
        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(PlaceValue1, CancellationToken.None);
        await s3.OnNextAsync(PlaceValue2, CancellationToken.None);
        await s4.OnNextAsync(PlaceValue3, CancellationToken.None);
        await s5.OnNextAsync(PlaceValue4, CancellationToken.None);
        await s6.OnNextAsync(PlaceValue5, CancellationToken.None);
        await s7.OnNextAsync(PlaceValue6, CancellationToken.None);
        await s8.OnNextAsync(PlaceValue7, CancellationToken.None);
        await s3.OnNextAsync(ReEmitValue3, CancellationToken.None);
        await Assert.That(results[^1]).IsEqualTo(ExpectedSum11111211);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
        await s8.OnCompletedAsync(Result.Success);
    }

    /// <summary>Verifies that CombineLatest8 OnNext_4 calls OnNextCombined when source 4 re-emits after all values are present.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107:Methods should not have too many parameters", Justification = "Test Reasons")]
    public async Task WhenCombineLatest8Source4ReEmits_ThenOnNextCombinedViaOnNext4()
    {
        const int ExpectedSum11112111 = 11_112_111;
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();
        var results = new List<int>();
        await using var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, s4.Values, s5.Values, s6.Values, s7.Values, s8.Values, (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h).SubscribeAsync(
            (x, _) =>
        {
            results.Add(x);
            return default;
        },
            null);
        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(PlaceValue1, CancellationToken.None);
        await s3.OnNextAsync(PlaceValue2, CancellationToken.None);
        await s4.OnNextAsync(PlaceValue3, CancellationToken.None);
        await s5.OnNextAsync(PlaceValue4, CancellationToken.None);
        await s6.OnNextAsync(PlaceValue5, CancellationToken.None);
        await s7.OnNextAsync(PlaceValue6, CancellationToken.None);
        await s8.OnNextAsync(PlaceValue7, CancellationToken.None);
        await s4.OnNextAsync(ReEmitValue4, CancellationToken.None);
        await Assert.That(results[^1]).IsEqualTo(ExpectedSum11112111);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
        await s8.OnCompletedAsync(Result.Success);
    }

    /// <summary>Verifies that CombineLatest8 OnNext_5 calls OnNextCombined when source 5 re-emits after all values are present.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107:Methods should not have too many parameters", Justification = "Test Reasons")]
    public async Task WhenCombineLatest8Source5ReEmits_ThenOnNextCombinedViaOnNext5()
    {
        const int ExpectedSum11121111 = 11_121_111;
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();
        var results = new List<int>();
        await using var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, s4.Values, s5.Values, s6.Values, s7.Values, s8.Values, (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h).SubscribeAsync(
            (x, _) =>
        {
            results.Add(x);
            return default;
        },
            null);
        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(PlaceValue1, CancellationToken.None);
        await s3.OnNextAsync(PlaceValue2, CancellationToken.None);
        await s4.OnNextAsync(PlaceValue3, CancellationToken.None);
        await s5.OnNextAsync(PlaceValue4, CancellationToken.None);
        await s6.OnNextAsync(PlaceValue5, CancellationToken.None);
        await s7.OnNextAsync(PlaceValue6, CancellationToken.None);
        await s8.OnNextAsync(PlaceValue7, CancellationToken.None);
        await s5.OnNextAsync(ReEmitValue5, CancellationToken.None);
        await Assert.That(results[^1]).IsEqualTo(ExpectedSum11121111);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
        await s8.OnCompletedAsync(Result.Success);
    }

    /// <summary>Verifies that CombineLatest8 OnNext_6 calls OnNextCombined when source 6 re-emits after all values are present.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107:Methods should not have too many parameters", Justification = "Test Reasons")]
    public async Task WhenCombineLatest8Source6ReEmits_ThenOnNextCombinedViaOnNext6()
    {
        const int ExpectedSum11211111 = 11_211_111;
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();
        var results = new List<int>();
        await using var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, s4.Values, s5.Values, s6.Values, s7.Values, s8.Values, (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h).SubscribeAsync(
            (x, _) =>
        {
            results.Add(x);
            return default;
        },
            null);
        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(PlaceValue1, CancellationToken.None);
        await s3.OnNextAsync(PlaceValue2, CancellationToken.None);
        await s4.OnNextAsync(PlaceValue3, CancellationToken.None);
        await s5.OnNextAsync(PlaceValue4, CancellationToken.None);
        await s6.OnNextAsync(PlaceValue5, CancellationToken.None);
        await s7.OnNextAsync(PlaceValue6, CancellationToken.None);
        await s8.OnNextAsync(PlaceValue7, CancellationToken.None);
        await s6.OnNextAsync(ReEmitValue6, CancellationToken.None);
        await Assert.That(results[^1]).IsEqualTo(ExpectedSum11211111);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
        await s8.OnCompletedAsync(Result.Success);
    }

    /// <summary>Verifies that CombineLatest8 OnNext_7 calls OnNextCombined when source 7 re-emits after all values are present.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107:Methods should not have too many parameters", Justification = "Test Reasons")]
    public async Task WhenCombineLatest8Source7ReEmits_ThenOnNextCombinedViaOnNext7()
    {
        const int ExpectedSum12111111 = 12_111_111;
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();
        var results = new List<int>();
        await using var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, s4.Values, s5.Values, s6.Values, s7.Values, s8.Values, (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h).SubscribeAsync(
            (x, _) =>
        {
            results.Add(x);
            return default;
        },
            null);
        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(PlaceValue1, CancellationToken.None);
        await s3.OnNextAsync(PlaceValue2, CancellationToken.None);
        await s4.OnNextAsync(PlaceValue3, CancellationToken.None);
        await s5.OnNextAsync(PlaceValue4, CancellationToken.None);
        await s6.OnNextAsync(PlaceValue5, CancellationToken.None);
        await s7.OnNextAsync(PlaceValue6, CancellationToken.None);
        await s8.OnNextAsync(PlaceValue7, CancellationToken.None);
        await s7.OnNextAsync(ReEmitValue7, CancellationToken.None);
        await Assert.That(results[^1]).IsEqualTo(ExpectedSum12111111);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
        await s8.OnCompletedAsync(Result.Success);
    }
}
