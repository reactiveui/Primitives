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
using ReactiveUI.Primitives.Async.Signals;
using AsyncObs = ReactiveUI.Primitives.Async.SignalAsync;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for CombineLatestArityTests.</summary>
public partial class CombineLatestArityTests
{
    /// <summary>
    /// Verifies that CombineLatest7 disposes on subscription failure (catch block).
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest7SubscriptionThrows_ThenDisposesAndRethrows()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var throwingSrc = AsyncObs.Create<int>((_, _) => throw new InvalidOperationException("subscribe failed"));

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await s1.Values.CombineLatest(
                    s2.Values,
                    s3.Values,
                    s4.Values,
                    s5.Values,
                    s6.Values,
                    throwingSrc,
                    (v1, v2, v3, v4, v5, v6, v7) => v1 + v2 + v3 + v4 + v5 + v6 + v7)
                .SubscribeAsync((_, _) => default, null));
    }

    /// <summary>
    /// Verifies that CombineLatest7 OnNextCombined guard returns when disposed.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest7DisposedBeforeCombine_ThenOnNextCombinedIsGuarded()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var results = new List<int>();

        var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                s7.Values,
                (v1, v2, v3, v4, v5, v6, v7) => v1 + v2 + v3 + v4 + v5 + v6 + v7).SubscribeAsync(
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

        await sub.DisposeAsync();

        await s1.OnNextAsync(PostDisposeValue, CancellationToken.None);

        await Assert.That(results).Count().IsEqualTo(1);
    }

    /// <summary>
    /// Verifies that CombineLatest7 OnErrorResume guard returns when disposed.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest7DisposedBeforeError_ThenOnErrorResumeIsGuarded()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        Exception? receivedError = null;

        var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                s7.Values,
                (v1, v2, v3, v4, v5, v6, v7) => v1 + v2 + v3 + v4 + v5 + v6 + v7).SubscribeAsync(
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
    /// Verifies that CombineLatest7 OnNext for the last source returns early when not all sources have values.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest7LastSourceEmitsFirst_ThenNoEmission()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                s7.Values,
                (v1, v2, v3, v4, v5, v6, v7) => v1 + v2 + v3 + v4 + v5 + v6 + v7).SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        await s7.OnNextAsync(SentinelValue1, CancellationToken.None);

        await Assert.That(results).IsEmpty();

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
    }

    /// <summary>
    /// Verifies that CombineLatest7 OnNext for middle sources returns early when not all sources have values.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest7MiddleSourcesEmitFirst_ThenNoEmission()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                s7.Values,
                (v1, v2, v3, v4, v5, v6, v7) => v1 + v2 + v3 + v4 + v5 + v6 + v7).SubscribeAsync(
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

        await Assert.That(results).IsEmpty();

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
    }

    /// <summary>
    /// Verifies that CombineLatest7 OnNext_1 calls OnNextCombined when source 1 re-emits after all values are present.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest7Source1ReEmits_ThenOnNextCombinedViaOnNext1()
    {
        const int ExpectedSum1111112 = 1_111_112;

        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                s7.Values,
                (a, b, c, d, e, f, g) => a + b + c + d + e + f + g).SubscribeAsync(
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

        await s1.OnNextAsync(SeedValue2, CancellationToken.None);

        await Assert.That(results[^1]).IsEqualTo(ExpectedSum1111112);

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
    }

    /// <summary>
    /// Verifies that CombineLatest7 OnNext_2 calls OnNextCombined when source 2 re-emits after all values are present.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest7Source2ReEmits_ThenOnNextCombinedViaOnNext2()
    {
        const int ExpectedSum1111121 = 1_111_121;

        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                s7.Values,
                (a, b, c, d, e, f, g) => a + b + c + d + e + f + g).SubscribeAsync(
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

        await s2.OnNextAsync(ReEmitValue2, CancellationToken.None);

        await Assert.That(results[^1]).IsEqualTo(ExpectedSum1111121);

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
    }

    /// <summary>
    /// Verifies that CombineLatest7 OnNext_3 calls OnNextCombined when source 3 re-emits after all values are present.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest7Source3ReEmits_ThenOnNextCombinedViaOnNext3()
    {
        const int ExpectedSum1111211 = 1_111_211;

        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                s7.Values,
                (a, b, c, d, e, f, g) => a + b + c + d + e + f + g).SubscribeAsync(
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

        await s3.OnNextAsync(ReEmitValue3, CancellationToken.None);

        await Assert.That(results[^1]).IsEqualTo(ExpectedSum1111211);

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
    }

    /// <summary>
    /// Verifies that CombineLatest7 OnNext_4 calls OnNextCombined when source 4 re-emits after all values are present.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest7Source4ReEmits_ThenOnNextCombinedViaOnNext4()
    {
        const int ExpectedSum1112111 = 1_112_111;

        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                s7.Values,
                (a, b, c, d, e, f, g) => a + b + c + d + e + f + g).SubscribeAsync(
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

        await s4.OnNextAsync(ReEmitValue4, CancellationToken.None);

        await Assert.That(results[^1]).IsEqualTo(ExpectedSum1112111);

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
    }

    /// <summary>
    /// Verifies that CombineLatest7 OnNext_5 calls OnNextCombined when source 5 re-emits after all values are present.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest7Source5ReEmits_ThenOnNextCombinedViaOnNext5()
    {
        const int ExpectedSum1121111 = 1_121_111;

        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                s7.Values,
                (a, b, c, d, e, f, g) => a + b + c + d + e + f + g).SubscribeAsync(
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

        await s5.OnNextAsync(ReEmitValue5, CancellationToken.None);

        await Assert.That(results[^1]).IsEqualTo(ExpectedSum1121111);

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
    }

    /// <summary>
    /// Verifies that CombineLatest7 OnNext_6 calls OnNextCombined when source 6 re-emits after all values are present.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest7Source6ReEmits_ThenOnNextCombinedViaOnNext6()
    {
        const int ExpectedSum1211111 = 1_211_111;

        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var results = new List<int>();

        await using var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                s7.Values,
                (a, b, c, d, e, f, g) => a + b + c + d + e + f + g).SubscribeAsync(
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

        await s6.OnNextAsync(ReEmitValue6, CancellationToken.None);

        await Assert.That(results[^1]).IsEqualTo(ExpectedSum1211111);

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
    }
}
