// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;
using AsyncObs = ReactiveUI.Primitives.Async.SignalAsync;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for CombineLatestArityTests.</summary>
public partial class CombineLatestArityTests
{
    /// <summary>Message thrown by a source whose subscription fails.</summary>
    private const string SubscribeFailedMessage = "subscribe failed";

    /// <summary>Message of the resumable error raised after disposal.</summary>
    private const string PostDisposeErrorMessage = "post-dispose error";

    /// <summary>Verifies that CombineLatest2 disposes on subscription failure (catch block).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest2SubscriptionThrows_ThenDisposesAndRethrows()
    {
        var s1 = Signal.Create<int>();
        var throwingSrc = AsyncObs.Create<int>(static (_, _) => throw new InvalidOperationException(SubscribeFailedMessage));
        await Assert
            .That(async () => await s1.Values.CombineLatest(throwingSrc, static (v1, v2) => v1 + v2)
                .SubscribeAsync(static (_, _) => default, null)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies that CombineLatest2 OnNextCombined guard returns when disposed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest2DisposedBeforeCombine_ThenOnNextCombinedIsGuarded()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        List<int> results = [];
        var sub = await s1.Values.CombineLatest(s2.Values, static (v1, v2) => v1 + v2).SubscribeAsync(
            (x, _) =>
            {
                results.Add(x);
                return default;
            },
            null);
        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(SeedValue2, CancellationToken.None);
        await sub.DisposeAsync();
        await s1.OnNextAsync(PostDisposeValue, CancellationToken.None);
        await Assert.That(results).Count().IsEqualTo(1);
    }

    /// <summary>Verifies that CombineLatest2 OnErrorResume guard returns when disposed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest2DisposedBeforeError_ThenOnErrorResumeIsGuarded()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        Exception? receivedError = null;
        var sub = await s1.Values.CombineLatest(s2.Values, static (v1, v2) => v1 + v2).SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                receivedError = ex;
                return default;
            });
        await sub.DisposeAsync();
        await s1.OnErrorResumeAsync(new InvalidOperationException(PostDisposeErrorMessage), CancellationToken.None);
        await Assert.That(receivedError).IsNull();
    }

    /// <summary>Verifies that CombineLatest2 OnNext for the last source returns early when not all sources have values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest2LastSourceEmitsFirst_ThenNoEmission()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        List<int> results = [];
        await using var sub = await s1.Values.CombineLatest(s2.Values, static (v1, v2) => v1 + v2).SubscribeAsync(
            (x, _) =>
            {
                results.Add(x);
                return default;
            },
            null);
        await s2.OnNextAsync(SentinelValue1, CancellationToken.None);
        await Assert.That(results).IsEmpty();
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
    }

    /// <summary>Verifies that CombineLatest3 disposes on subscription failure (catch block).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest3SubscriptionThrows_ThenDisposesAndRethrows()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var throwingSrc = AsyncObs.Create<int>(static (_, _) => throw new InvalidOperationException(SubscribeFailedMessage));
        await Assert
            .That(async () => await s1.Values.CombineLatest(s2.Values, throwingSrc, static (v1, v2, v3) => v1 + v2 + v3)
                .SubscribeAsync(static (_, _) => default, null)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies that CombineLatest3 OnNextCombined guard returns when disposed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest3DisposedBeforeCombine_ThenOnNextCombinedIsGuarded()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        List<int> results = [];
        var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, static (v1, v2, v3) => v1 + v2 + v3).SubscribeAsync(
            (x, _) =>
            {
                results.Add(x);
                return default;
            },
            null);
        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(SeedValue2, CancellationToken.None);
        await s3.OnNextAsync(SeedValue3, CancellationToken.None);
        await sub.DisposeAsync();
        await s1.OnNextAsync(PostDisposeValue, CancellationToken.None);
        await Assert.That(results).Count().IsEqualTo(1);
    }

    /// <summary>Verifies that CombineLatest3 OnErrorResume guard returns when disposed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest3DisposedBeforeError_ThenOnErrorResumeIsGuarded()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        Exception? receivedError = null;
        var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, static (v1, v2, v3) => v1 + v2 + v3).SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                receivedError = ex;
                return default;
            });
        await sub.DisposeAsync();
        await s1.OnErrorResumeAsync(new InvalidOperationException(PostDisposeErrorMessage), CancellationToken.None);
        await Assert.That(receivedError).IsNull();
    }

    /// <summary>Verifies that CombineLatest3 OnNext for the last source returns early when not all sources have values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest3LastSourceEmitsFirst_ThenNoEmission()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        List<int> results = [];
        await using var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, static (v1, v2, v3) => v1 + v2 + v3)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);
        await s3.OnNextAsync(SentinelValue1, CancellationToken.None);
        await Assert.That(results).IsEmpty();
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
    }

    /// <summary>Verifies that CombineLatest3 OnNext for the middle source returns early when not all sources have values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest3MiddleSourceEmitsFirst_ThenNoEmission()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        List<int> results = [];
        await using var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, static (v1, v2, v3) => v1 + v2 + v3)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);
        await s2.OnNextAsync(SentinelValue1, CancellationToken.None);
        await Assert.That(results).IsEmpty();
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
    }

    /// <summary>Verifies that CombineLatest3 OnNext_1 calls OnNextCombined when source 1 re-emits after all values are present.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest3Source1ReEmits_ThenOnNextCombinedViaOnNext1()
    {
        const int ExpectedSum112 = 112;
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        List<int> results = [];
        await using var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, static (a, b, c) => a + b + c)
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
        await s1.OnNextAsync(SeedValue2, CancellationToken.None);
        await Assert.That(results[^1]).IsEqualTo(ExpectedSum112);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
    }

    /// <summary>Verifies that CombineLatest3 OnNext_2 calls OnNextCombined when source 2 re-emits after all values are present.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest3Source2ReEmits_ThenOnNextCombinedViaOnNext2()
    {
        const int ExpectedSum121 = 121;
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        List<int> results = [];
        await using var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, static (a, b, c) => a + b + c)
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
        await s2.OnNextAsync(ReEmitValue2, CancellationToken.None);
        await Assert.That(results[^1]).IsEqualTo(ExpectedSum121);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
    }

    /// <summary>Verifies that CombineLatest4 disposes on subscription failure (catch block).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest4SubscriptionThrows_ThenDisposesAndRethrows()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var throwingSrc = AsyncObs.Create<int>(static (_, _) => throw new InvalidOperationException(SubscribeFailedMessage));
        await Assert
            .That(async () =>
                await s1.Values.CombineLatest(s2.Values, s3.Values, throwingSrc, static (v1, v2, v3, v4) => v1 + v2 + v3 + v4)
                    .SubscribeAsync(static (_, _) => default, null)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies that CombineLatest4 OnNextCombined guard returns when disposed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest4DisposedBeforeCombine_ThenOnNextCombinedIsGuarded()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        List<int> results = [];
        var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, s4.Values, static (v1, v2, v3, v4) => v1 + v2 + v3 + v4)
            .SubscribeAsync(
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
        await sub.DisposeAsync();
        await s1.OnNextAsync(PostDisposeValue, CancellationToken.None);
        await Assert.That(results).Count().IsEqualTo(1);
    }

    /// <summary>Verifies that CombineLatest4 OnErrorResume guard returns when disposed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest4DisposedBeforeError_ThenOnErrorResumeIsGuarded()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        Exception? receivedError = null;
        var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, s4.Values, static (v1, v2, v3, v4) => v1 + v2 + v3 + v4)
            .SubscribeAsync(static (_, _) => default, (ex, _) =>
            {
                receivedError = ex;
                return default;
            });
        await sub.DisposeAsync();
        await s1.OnErrorResumeAsync(new InvalidOperationException(PostDisposeErrorMessage), CancellationToken.None);
        await Assert.That(receivedError).IsNull();
    }

    /// <summary>Verifies that CombineLatest4 OnNext for the last source returns early when not all sources have values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest4LastSourceEmitsFirst_ThenNoEmission()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        List<int> results = [];
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, s3.Values, s4.Values, static (v1, v2, v3, v4) => v1 + v2 + v3 + v4).SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);
        await s4.OnNextAsync(SentinelValue1, CancellationToken.None);
        await Assert.That(results).IsEmpty();
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
    }

    /// <summary>Verifies that CombineLatest4 OnNext for middle sources returns early when not all sources have values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest4MiddleSourcesEmitFirst_ThenNoEmission()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        List<int> results = [];
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, s3.Values, s4.Values, static (v1, v2, v3, v4) => v1 + v2 + v3 + v4).SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);
        await s2.OnNextAsync(SentinelValue1, CancellationToken.None);
        await s3.OnNextAsync(SentinelValue2, CancellationToken.None);
        await Assert.That(results).IsEmpty();
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
    }

    /// <summary>Verifies that CombineLatest4 OnNext_1 calls OnNextCombined when source 1 re-emits after all values are present.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest4Source1ReEmits_ThenOnNextCombinedViaOnNext1()
    {
        const int ExpectedSum1112 = 1_112;
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        List<int> results = [];
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, s3.Values, s4.Values, static (a, b, c, d) => a + b + c + d).SubscribeAsync(
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
        await s1.OnNextAsync(SeedValue2, CancellationToken.None);
        await Assert.That(results[^1]).IsEqualTo(ExpectedSum1112);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
    }

    /// <summary>Verifies that CombineLatest4 OnNext_2 calls OnNextCombined when source 2 re-emits after all values are present.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest4Source2ReEmits_ThenOnNextCombinedViaOnNext2()
    {
        const int ExpectedSum1121 = 1_121;
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        List<int> results = [];
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, s3.Values, s4.Values, static (a, b, c, d) => a + b + c + d).SubscribeAsync(
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
        await s2.OnNextAsync(ReEmitValue2, CancellationToken.None);
        await Assert.That(results[^1]).IsEqualTo(ExpectedSum1121);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
    }

    /// <summary>Verifies that CombineLatest4 OnNext_3 calls OnNextCombined when source 3 re-emits after all values are present.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest4Source3ReEmits_ThenOnNextCombinedViaOnNext3()
    {
        const int ExpectedSum1211 = 1_211;
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        List<int> results = [];
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, s3.Values, s4.Values, static (a, b, c, d) => a + b + c + d).SubscribeAsync(
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
        await s3.OnNextAsync(ReEmitValue3, CancellationToken.None);
        await Assert.That(results[^1]).IsEqualTo(ExpectedSum1211);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
    }
}
