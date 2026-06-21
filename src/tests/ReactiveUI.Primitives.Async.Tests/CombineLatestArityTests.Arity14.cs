// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;
using AsyncObs = ReactiveUI.Primitives.Async.SignalAsync;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Smoke tests for arity-14 CombineLatest covering the subscription lifecycle,
/// dispose guard, error forwarding, and the all-sources-emit happy path.</summary>
public partial class CombineLatestArityTests
{
    /// <summary>Verifies that CombineLatest14 disposes on subscription failure (catch block).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Major Code Smell",
        "S107:Methods should not have too many parameters",
        Justification = "Test Reasons")]
    [SuppressMessage(
        "Major Code Smell",
        "S138:Methods should not have too many lines",
        Justification =
            "Smoke test inherently lists N Signals + per-source calls; splitting would obscure the under-test sequence.")]
    public async Task WhenCombineLatest14SubscriptionThrows_ThenDisposesAndRethrows()
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
        var s11 = Signal.Create<int>();
        var s12 = Signal.Create<int>();
        var s13 = Signal.Create<int>();
        var throwingSrc = AsyncObs.Create<int>((_, _) => throw new InvalidOperationException("subscribe failed"));
        await Assert.That(async () => await s1.Values.CombineLatest(
            s2.Values,
            s3.Values,
            s4.Values,
            s5.Values,
            s6.Values,
            s7.Values,
            s8.Values,
            s9.Values,
            s10.Values,
            s11.Values,
            s12.Values,
            s13.Values,
            throwingSrc,
            (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14) =>
                    v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10 + v11 + v12 + v13 + v14).SubscribeAsync((_, _) => default, null)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies that CombineLatest14 OnNextCombined guard returns when disposed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Major Code Smell",
        "S107:Methods should not have too many parameters",
        Justification = "Test Reasons")]
    [SuppressMessage(
        "Major Code Smell",
        "S138:Methods should not have too many lines",
        Justification =
            "Smoke test inherently lists N Signals + per-source calls; splitting would obscure the under-test sequence.")]
    public async Task WhenCombineLatest14DisposedBeforeCombine_ThenOnNextCombinedIsGuarded()
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
        var s11 = Signal.Create<int>();
        var s12 = Signal.Create<int>();
        var s13 = Signal.Create<int>();
        var s14 = Signal.Create<int>();
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
            s11.Values,
            s12.Values,
            s13.Values,
            s14.Values,
            (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14) =>
                v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10 + v11 + v12 + v13 + v14).SubscribeAsync(
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
        await s9.OnNextAsync(PlaceValue8, CancellationToken.None);
        await s10.OnNextAsync(PlaceValue9, CancellationToken.None);
        await s11.OnNextAsync(PlaceValue10, CancellationToken.None);
        await s12.OnNextAsync(PlaceValue11, CancellationToken.None);
        await s13.OnNextAsync(PlaceValue12, CancellationToken.None);
        await s14.OnNextAsync(PlaceValue13, CancellationToken.None);
        await sub.DisposeAsync();
        await s1.OnNextAsync(PostDisposeValue, CancellationToken.None);
        await Assert.That(results).Count().IsEqualTo(1);
    }

    /// <summary>Verifies that CombineLatest14 forwards a source error to the downstream observer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Major Code Smell",
        "S107:Methods should not have too many parameters",
        Justification = "Test Reasons")]
    [SuppressMessage(
        "Major Code Smell",
        "S138:Methods should not have too many lines",
        Justification =
            "Smoke test inherently lists N Signals + per-source calls; splitting would obscure the under-test sequence.")]
    public async Task WhenCombineLatest14OneSourceErrors_ThenCombinedErrorForwarded()
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
        var s11 = Signal.Create<int>();
        var s12 = Signal.Create<int>();
        var s13 = Signal.Create<int>();
        var s14 = Signal.Create<int>();
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
            s11.Values,
            s12.Values,
            s13.Values,
            s14.Values,
            (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14) =>
                v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10 + v11 + v12 + v13 + v14).SubscribeAsync(
            (_, _) => default,
            (ex, _) =>
            {
                receivedError = ex;
                IgnoredResult.Of(errorReceived.TrySetResult());
                return default;
            });
        InvalidOperationException expected = new("source error");
        await s1.OnErrorResumeAsync(expected, CancellationToken.None);
        await errorReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(receivedError).IsEqualTo(expected);
    }

    /// <summary>Verifies that CombineLatest14 produces the selector's result once every source has emitted at least once.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Major Code Smell",
        "S107:Methods should not have too many parameters",
        Justification = "Test Reasons")]
    [SuppressMessage(
        "Major Code Smell",
        "S138:Methods should not have too many lines",
        Justification =
            "Smoke test inherently lists N Signals + per-source calls; splitting would obscure the under-test sequence.")]
    public async Task WhenCombineLatest14AllSourcesEmit_ThenSelectorResultEmitted()
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
        var s11 = Signal.Create<int>();
        var s12 = Signal.Create<int>();
        var s13 = Signal.Create<int>();
        var s14 = Signal.Create<int>();
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
            s11.Values,
            s12.Values,
            s13.Values,
            s14.Values,
            (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14) =>
                v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10 + v11 + v12 + v13 + v14).SubscribeAsync(
            (x, _) =>
            {
                results.Add(x);
                IgnoredResult.Of(emitted.TrySetResult());
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
        await s9.OnNextAsync(PlaceValue8, CancellationToken.None);
        await s10.OnNextAsync(PlaceValue9, CancellationToken.None);
        await s11.OnNextAsync(PlaceValue10, CancellationToken.None);
        await s12.OnNextAsync(PlaceValue11, CancellationToken.None);
        await s13.OnNextAsync(PlaceValue12, CancellationToken.None);
        await s14.OnNextAsync(PlaceValue13, CancellationToken.None);
        await emitted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(results[0]).IsEqualTo(1 + PlaceValue1 + PlaceValue2 + PlaceValue3 + PlaceValue4 +
                                                PlaceValue5 + PlaceValue6 + PlaceValue7 + PlaceValue8 + PlaceValue9 +
                                                PlaceValue10 + PlaceValue11 + PlaceValue12 + PlaceValue13);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
        await s8.OnCompletedAsync(Result.Success);
        await s9.OnCompletedAsync(Result.Success);
        await s10.OnCompletedAsync(Result.Success);
        await s11.OnCompletedAsync(Result.Success);
        await s12.OnCompletedAsync(Result.Success);
        await s13.OnCompletedAsync(Result.Success);
        await s14.OnCompletedAsync(Result.Success);
    }

    /// <summary>Verifies that CombineLatest14 completes once every source has completed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Major Code Smell",
        "S107:Methods should not have too many parameters",
        Justification = "Test Reasons")]
    [SuppressMessage(
        "Major Code Smell",
        "S138:Methods should not have too many lines",
        Justification =
            "Smoke test inherently lists N Signals + per-source calls; splitting would obscure the under-test sequence.")]
    public async Task WhenCombineLatest14AllSourcesComplete_ThenCombinedCompletes()
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
        var s11 = Signal.Create<int>();
        var s12 = Signal.Create<int>();
        var s13 = Signal.Create<int>();
        var s14 = Signal.Create<int>();
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
            s11.Values,
            s12.Values,
            s13.Values,
            s14.Values,
            (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14) =>
                v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10 + v11 + v12 + v13 + v14).SubscribeAsync(
            (_, _) => default,
            null,
            r =>
            {
                _ = completed.TrySetResult(r);
                return default;
            });
        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(PlaceValue1, CancellationToken.None);
        await s3.OnNextAsync(PlaceValue2, CancellationToken.None);
        await s4.OnNextAsync(PlaceValue3, CancellationToken.None);
        await s5.OnNextAsync(PlaceValue4, CancellationToken.None);
        await s6.OnNextAsync(PlaceValue5, CancellationToken.None);
        await s7.OnNextAsync(PlaceValue6, CancellationToken.None);
        await s8.OnNextAsync(PlaceValue7, CancellationToken.None);
        await s9.OnNextAsync(PlaceValue8, CancellationToken.None);
        await s10.OnNextAsync(PlaceValue9, CancellationToken.None);
        await s11.OnNextAsync(PlaceValue10, CancellationToken.None);
        await s12.OnNextAsync(PlaceValue11, CancellationToken.None);
        await s13.OnNextAsync(PlaceValue12, CancellationToken.None);
        await s14.OnNextAsync(PlaceValue13, CancellationToken.None);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
        await s8.OnCompletedAsync(Result.Success);
        await s9.OnCompletedAsync(Result.Success);
        await s10.OnCompletedAsync(Result.Success);
        await s11.OnCompletedAsync(Result.Success);
        await s12.OnCompletedAsync(Result.Success);
        await s13.OnCompletedAsync(Result.Success);
        await s14.OnCompletedAsync(Result.Success);
        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(result.IsSuccess).IsTrue();
    }
}
