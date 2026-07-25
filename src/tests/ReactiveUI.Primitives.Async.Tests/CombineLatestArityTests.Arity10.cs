// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;
using AsyncObs = ReactiveUI.Primitives.Async.SignalAsync;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Smoke tests for arity-10 CombineLatest covering the subscription lifecycle,
/// dispose guard, error forwarding, and the all-sources-emit happy path.</summary>
public partial class CombineLatestArityTests
{
    /// <summary>Verifies that CombineLatest10 disposes on subscription failure (catch block).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest10SubscriptionThrows_ThenDisposesAndRethrows()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();
        var s9 = Signal.Create<int>();
        var throwingSrc = AsyncObs.Create<int>(static (_, _) => throw new InvalidOperationException("subscribe failed"));
        await Assert.That(async () => await s1.Values.CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                s7.Values,
                s8.Values,
                s9.Values,
                throwingSrc,
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10) => v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10)
            .SubscribeAsync(static (_, _) => default, null)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies that CombineLatest10 OnNextCombined guard returns when disposed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest10DisposedBeforeCombine_ThenOnNextCombinedIsGuarded()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();
        var s9 = Signal.Create<int>();
        var s10 = Signal.Create<int>();
        List<int> results = [];
        var sub = await s1.Values.CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                s7.Values,
                s8.Values,
                s9.Values,
                s10.Values,
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10) => v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);
        await EmitSeedAndPlaceValuesAsync(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10);
        await sub.DisposeAsync();
        await s1.OnNextAsync(PostDisposeValue, CancellationToken.None);
        await Assert.That(results).Count().IsEqualTo(1);
    }

    /// <summary>Verifies that CombineLatest10 forwards a source error to the downstream observer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest10OneSourceErrors_ThenCombinedErrorForwarded()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();
        var s9 = Signal.Create<int>();
        var s10 = Signal.Create<int>();
        Exception? receivedError = null;
        TaskCompletionSource errorReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                s7.Values,
                s8.Values,
                s9.Values,
                s10.Values,
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10) => v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10)
            .SubscribeAsync(static (_, _) => default, (ex, _) =>
            {
                receivedError = ex;
                IgnoredResult.Of(errorReceived.TrySetResult());
                return default;
            });
        InvalidOperationException expected = new("source error");
        await s1.OnErrorResumeAsync(expected, CancellationToken.None);
        await errorReceived.Task.WaitAsync(TimeSpan.FromSeconds(EmissionTimeoutSeconds));
        await Assert.That(receivedError).IsEqualTo(expected);
    }

    /// <summary>Verifies that CombineLatest10 produces the selector's result once every source has emitted at least once.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest10AllSourcesEmit_ThenSelectorResultEmitted()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();
        var s9 = Signal.Create<int>();
        var s10 = Signal.Create<int>();
        List<int> results = [];
        TaskCompletionSource emitted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                s7.Values,
                s8.Values,
                s9.Values,
                s10.Values,
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10) => v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    IgnoredResult.Of(emitted.TrySetResult());
                    return default;
                },
                null);
        await EmitSeedAndPlaceValuesAsync(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10);
        await emitted.Task.WaitAsync(TimeSpan.FromSeconds(EmissionTimeoutSeconds));
        await Assert.That(results[0]).IsEqualTo(1 + PlaceValue1 + PlaceValue2 + PlaceValue3 + PlaceValue4
                                                + PlaceValue5 + PlaceValue6 + PlaceValue7 + PlaceValue8 + PlaceValue9);
        await CompleteAllAsync(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10);
    }

    /// <summary>Verifies that CombineLatest10 completes once every source has completed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest10AllSourcesComplete_ThenCombinedCompletes()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();
        var s9 = Signal.Create<int>();
        var s10 = Signal.Create<int>();
        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                s7.Values,
                s8.Values,
                s9.Values,
                s10.Values,
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10) => v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10)
            .SubscribeAsync(static (_, _) => default, null, r =>
            {
                _ = completed.TrySetResult(r);
                return default;
            });
        await EmitSeedAndPlaceValuesAsync(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10);
        await CompleteAllAsync(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10);
        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(EmissionTimeoutSeconds));
        await Assert.That(result.IsSuccess).IsTrue();
    }
}
