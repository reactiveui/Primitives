// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;
using AsyncObs = ReactiveUI.Primitives.Async.SignalAsync;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for CombineLatestArityTests.</summary>
public partial class CombineLatestArityTests
{
    /// <summary>
    /// Verifies that CombineLatest5 disposes on subscription failure (catch block).
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest5SubscriptionThrows_ThenDisposesAndRethrows()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var throwingSrc = AsyncObs.Create<int>((_, _) => throw new InvalidOperationException("subscribe failed"));

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await s1.Values.CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                throwingSrc,
                (v1, v2, v3, v4, v5) => v1 + v2 + v3 + v4 + v5).SubscribeAsync((_, _) => default, null));
    }

    /// <summary>
    /// Verifies that CombineLatest5 OnNextCombined guard returns when disposed.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest5DisposedBeforeCombine_ThenOnNextCombinedIsGuarded()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var results = new List<int>();

        var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                (v1, v2, v3, v4, v5) => v1 + v2 + v3 + v4 + v5).SubscribeAsync(
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

        await sub.DisposeAsync();

        await s1.OnNextAsync(PostDisposeValue, CancellationToken.None);

        await Assert.That(results).Count().IsEqualTo(1);
    }

    /// <summary>
    /// Verifies that CombineLatest5 OnErrorResume guard returns when disposed.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest5DisposedBeforeError_ThenOnErrorResumeIsGuarded()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        Exception? receivedError = null;

        var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                (v1, v2, v3, v4, v5) => v1 + v2 + v3 + v4 + v5).SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    receivedError = ex;
                    return default;
                });

        await sub.DisposeAsync();

        await s1.OnErrorResumeAsync(new InvalidOperationException("post-dispose error"), CancellationToken.None);

        await Assert.That(receivedError).IsNull();
    }

    /// <summary>
    /// Verifies that CombineLatest5 OnNext for the last source returns early when not all sources have values.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest5LastSourceEmitsFirst_ThenNoEmission()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                (v1, v2, v3, v4, v5) => v1 + v2 + v3 + v4 + v5).SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        await s5.OnNextAsync(SentinelValue1, CancellationToken.None);

        await Assert.That(results).IsEmpty();

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
    }

    /// <summary>
    /// Verifies that CombineLatest5 OnNext for middle sources returns early when not all sources have values.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest5MiddleSourcesEmitFirst_ThenNoEmission()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                (v1, v2, v3, v4, v5) => v1 + v2 + v3 + v4 + v5).SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        await s2.OnNextAsync(SentinelValue1, CancellationToken.None);
        await s3.OnNextAsync(SentinelValue2, CancellationToken.None);
        await s4.OnNextAsync(SentinelValue3, CancellationToken.None);

        await Assert.That(results).IsEmpty();

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
    }

    /// <summary>
    /// Verifies that CombineLatest5 OnNext_1 calls OnNextCombined when source 1 re-emits after all values are present.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest5Source1ReEmits_ThenOnNextCombinedViaOnNext1()
    {
        const int ExpectedSum11112 = 11_112;

        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(s2.Values, s3.Values, s4.Values, s5.Values, (a, b, c, d, e) => a + b + c + d + e)
            .SubscribeAsync(
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

        await s1.OnNextAsync(SeedValue2, CancellationToken.None);

        await Assert.That(results[^1]).IsEqualTo(ExpectedSum11112);

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
    }

    /// <summary>
    /// Verifies that CombineLatest5 OnNext_2 calls OnNextCombined when source 2 re-emits after all values are present.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest5Source2ReEmits_ThenOnNextCombinedViaOnNext2()
    {
        const int ExpectedSum11121 = 11_121;

        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(s2.Values, s3.Values, s4.Values, s5.Values, (a, b, c, d, e) => a + b + c + d + e)
            .SubscribeAsync(
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

        await s2.OnNextAsync(ReEmitValue2, CancellationToken.None);

        await Assert.That(results[^1]).IsEqualTo(ExpectedSum11121);

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
    }

    /// <summary>
    /// Verifies that CombineLatest5 OnNext_3 calls OnNextCombined when source 3 re-emits after all values are present.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest5Source3ReEmits_ThenOnNextCombinedViaOnNext3()
    {
        const int ExpectedSum11211 = 11_211;

        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(s2.Values, s3.Values, s4.Values, s5.Values, (a, b, c, d, e) => a + b + c + d + e)
            .SubscribeAsync(
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

        await s3.OnNextAsync(ReEmitValue3, CancellationToken.None);

        await Assert.That(results[^1]).IsEqualTo(ExpectedSum11211);

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
    }

    /// <summary>
    /// Verifies that CombineLatest5 OnNext_4 calls OnNextCombined when source 4 re-emits after all values are present.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest5Source4ReEmits_ThenOnNextCombinedViaOnNext4()
    {
        const int ExpectedSum12111 = 12_111;

        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(s2.Values, s3.Values, s4.Values, s5.Values, (a, b, c, d, e) => a + b + c + d + e)
            .SubscribeAsync(
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

        await s4.OnNextAsync(ReEmitValue4, CancellationToken.None);

        await Assert.That(results[^1]).IsEqualTo(ExpectedSum12111);

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
    }

    /// <summary>
    /// Verifies that CombineLatest6 disposes on subscription failure (catch block).
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest6SubscriptionThrows_ThenDisposesAndRethrows()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var throwingSrc = AsyncObs.Create<int>((_, _) => throw new InvalidOperationException("subscribe failed"));

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await s1.Values.CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                throwingSrc,
                (v1, v2, v3, v4, v5, v6) => v1 + v2 + v3 + v4 + v5 + v6).SubscribeAsync((_, _) => default, null));
    }

    /// <summary>
    /// Verifies that CombineLatest6 OnNextCombined guard returns when disposed.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest6DisposedBeforeCombine_ThenOnNextCombinedIsGuarded()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var results = new List<int>();

        var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                (v1, v2, v3, v4, v5, v6) => v1 + v2 + v3 + v4 + v5 + v6).SubscribeAsync(
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

        await sub.DisposeAsync();

        await s1.OnNextAsync(PostDisposeValue, CancellationToken.None);

        await Assert.That(results).Count().IsEqualTo(1);
    }

    /// <summary>
    /// Verifies that CombineLatest6 OnErrorResume guard returns when disposed.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest6DisposedBeforeError_ThenOnErrorResumeIsGuarded()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        Exception? receivedError = null;

        var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                (v1, v2, v3, v4, v5, v6) => v1 + v2 + v3 + v4 + v5 + v6).SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    receivedError = ex;
                    return default;
                });

        await sub.DisposeAsync();

        await s1.OnErrorResumeAsync(new InvalidOperationException("post-dispose error"), CancellationToken.None);

        await Assert.That(receivedError).IsNull();
    }

    /// <summary>
    /// Verifies that CombineLatest6 OnNext for the last source returns early when not all sources have values.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest6LastSourceEmitsFirst_ThenNoEmission()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                (v1, v2, v3, v4, v5, v6) => v1 + v2 + v3 + v4 + v5 + v6).SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        await s6.OnNextAsync(SentinelValue1, CancellationToken.None);

        await Assert.That(results).IsEmpty();

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
    }

    /// <summary>
    /// Verifies that CombineLatest6 OnNext for middle sources returns early when not all sources have values.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest6MiddleSourcesEmitFirst_ThenNoEmission()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                (v1, v2, v3, v4, v5, v6) => v1 + v2 + v3 + v4 + v5 + v6).SubscribeAsync(
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

        await Assert.That(results).IsEmpty();

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
    }

    /// <summary>
    /// Verifies that CombineLatest6 OnNext_1 calls OnNextCombined when source 1 re-emits after all values are present.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest6Source1ReEmits_ThenOnNextCombinedViaOnNext1()
    {
        const int ExpectedSum111112 = 111_112;

        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                (a, b, c, d, e, f) => a + b + c + d + e + f).SubscribeAsync(
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

        await s1.OnNextAsync(SeedValue2, CancellationToken.None);

        await Assert.That(results[^1]).IsEqualTo(ExpectedSum111112);

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
    }

    /// <summary>
    /// Verifies that CombineLatest6 OnNext_2 calls OnNextCombined when source 2 re-emits after all values are present.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest6Source2ReEmits_ThenOnNextCombinedViaOnNext2()
    {
        const int ExpectedSum111121 = 111_121;

        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                (a, b, c, d, e, f) => a + b + c + d + e + f).SubscribeAsync(
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

        await s2.OnNextAsync(ReEmitValue2, CancellationToken.None);

        await Assert.That(results[^1]).IsEqualTo(ExpectedSum111121);

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
    }

    /// <summary>
    /// Verifies that CombineLatest6 OnNext_3 calls OnNextCombined when source 3 re-emits after all values are present.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest6Source3ReEmits_ThenOnNextCombinedViaOnNext3()
    {
        const int ExpectedSum111211 = 111_211;

        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                (a, b, c, d, e, f) => a + b + c + d + e + f).SubscribeAsync(
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

        await s3.OnNextAsync(ReEmitValue3, CancellationToken.None);

        await Assert.That(results[^1]).IsEqualTo(ExpectedSum111211);

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
    }

    /// <summary>
    /// Verifies that CombineLatest6 OnNext_4 calls OnNextCombined when source 4 re-emits after all values are present.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest6Source4ReEmits_ThenOnNextCombinedViaOnNext4()
    {
        const int ExpectedSum112111 = 112_111;

        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                (a, b, c, d, e, f) => a + b + c + d + e + f).SubscribeAsync(
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

        await s4.OnNextAsync(ReEmitValue4, CancellationToken.None);

        await Assert.That(results[^1]).IsEqualTo(ExpectedSum112111);

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
    }

    /// <summary>
    /// Verifies that CombineLatest6 OnNext_5 calls OnNextCombined when source 5 re-emits after all values are present.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest6Source5ReEmits_ThenOnNextCombinedViaOnNext5()
    {
        const int ExpectedSum121111 = 121_111;

        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                (a, b, c, d, e, f) => a + b + c + d + e + f).SubscribeAsync(
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

        await s5.OnNextAsync(ReEmitValue5, CancellationToken.None);

        await Assert.That(results[^1]).IsEqualTo(ExpectedSum121111);

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
    }
}
