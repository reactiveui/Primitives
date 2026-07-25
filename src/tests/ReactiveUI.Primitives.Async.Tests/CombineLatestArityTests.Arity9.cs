// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;
using AsyncObs = ReactiveUI.Primitives.Async.SignalAsync;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Smoke tests for arity-9 CombineLatest covering the subscription lifecycle,
/// dispose guard, error forwarding, and the all-sources-emit happy path.</summary>
public partial class CombineLatestArityTests
{
    /// <summary>How long an arity test waits for the combined sequence to emit, error, or complete.</summary>
    private const int EmissionTimeoutSeconds = 5;

    /// <summary>The place values emitted by every source after the first, in selector order.</summary>
    private static readonly int[] TrailingPlaceValues =
    [
        PlaceValue1, PlaceValue2, PlaceValue3, PlaceValue4, PlaceValue5, PlaceValue6, PlaceValue7, PlaceValue8,
        PlaceValue9, PlaceValue10, PlaceValue11, PlaceValue12, PlaceValue13, PlaceValue14, PlaceValue15
    ];

    /// <summary>Verifies that CombineLatest9 disposes on subscription failure (catch block).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest9SubscriptionThrows_ThenDisposesAndRethrows()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();
        var throwingSrc = AsyncObs.Create<int>(static (_, _) => throw new InvalidOperationException("subscribe failed"));
        await Assert.That(async () => await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                s7.Values,
                s8.Values,
                throwingSrc,
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9) => v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9)
            .SubscribeAsync(static (_, _) => default, null)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies that CombineLatest9 OnNextCombined guard returns when disposed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest9DisposedBeforeCombine_ThenOnNextCombinedIsGuarded()
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
            static (v1, v2, v3, v4, v5, v6, v7, v8, v9) => v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9).SubscribeAsync(RecordValues(results), null);
        await EmitSeedAndPlaceValuesAsync(s1, s2, s3, s4, s5, s6, s7, s8, s9);
        await sub.DisposeAsync();
        await s1.OnNextAsync(PostDisposeValue, CancellationToken.None);
        await Assert.That(results).Count().IsEqualTo(1);
    }

    /// <summary>Verifies that CombineLatest9 forwards a source error to the downstream observer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest9OneSourceErrors_ThenCombinedErrorForwarded()
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
        Exception? receivedError = null;
        TaskCompletionSource errorReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                s7.Values,
                s8.Values,
                s9.Values,
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9) => v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9).SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
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

    /// <summary>Verifies that CombineLatest9 produces the selector's result once every source has emitted at least once.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest9AllSourcesEmit_ThenSelectorResultEmitted()
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
            static (v1, v2, v3, v4, v5, v6, v7, v8, v9) => v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9).SubscribeAsync(RecordAndSignalValues(results, emitted), null);
        await EmitSeedAndPlaceValuesAsync(s1, s2, s3, s4, s5, s6, s7, s8, s9);
        await emitted.Task.WaitAsync(TimeSpan.FromSeconds(EmissionTimeoutSeconds));
        await Assert.That(results[0]).IsEqualTo(1 + PlaceValue1 + PlaceValue2 + PlaceValue3 + PlaceValue4
                                                + PlaceValue5 + PlaceValue6 + PlaceValue7 + PlaceValue8);
        await CompleteAllAsync(s1, s2, s3, s4, s5, s6, s7, s8, s9);
    }

    /// <summary>Verifies that CombineLatest9 completes once every source has completed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest9AllSourcesComplete_ThenCombinedCompletes()
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
        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values
            .CombineLatest(
                s2.Values,
                s3.Values,
                s4.Values,
                s5.Values,
                s6.Values,
                s7.Values,
                s8.Values,
                s9.Values,
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9) => v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9).SubscribeAsync(
                static (_, _) => default,
                null,
                r =>
                {
                    _ = completed.TrySetResult(r);
                    return default;
                });
        await EmitSeedAndPlaceValuesAsync(s1, s2, s3, s4, s5, s6, s7, s8, s9);
        await CompleteAllAsync(s1, s2, s3, s4, s5, s6, s7, s8, s9);
        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(EmissionTimeoutSeconds));
        await Assert.That(result.IsSuccess).IsTrue();
    }

    /// <summary>Emits the seed into the first source and a distinct place value into each of the others.</summary>
    /// <param name = "sources">The combined sources, in selector order.</param>
    /// <returns>A <see cref = "Task"/> representing the asynchronous operation.</returns>
    private static async Task EmitSeedAndPlaceValuesAsync(params ISignalAsync<int>[] sources)
    {
        await sources[0].OnNextAsync(1, CancellationToken.None);
        for (var index = 1; index < sources.Length; index++)
        {
            await sources[index].OnNextAsync(TrailingPlaceValues[index - 1], CancellationToken.None);
        }
    }

    /// <summary>Completes every source successfully, in selector order.</summary>
    /// <param name = "sources">The combined sources, in selector order.</param>
    /// <returns>A <see cref = "Task"/> representing the asynchronous operation.</returns>
    private static async Task CompleteAllAsync(params ISignalAsync<int>[] sources)
    {
        foreach (var source in sources)
        {
            await source.OnCompletedAsync(Result.Success);
        }
    }

    /// <summary>Builds an observer callback that records every value the combined sequence emits.</summary>
    /// <param name = "results">Receives every emitted value.</param>
    /// <returns>The callback passed to <c>SubscribeAsync</c>.</returns>
    private static Func<int, CancellationToken, ValueTask> RecordValues(List<int> results) =>
        (value, _) =>
        {
            results.Add(value);
            return default;
        };

    /// <summary>Builds an observer callback that records emitted values and signals the first one.</summary>
    /// <param name = "results">Receives every emitted value.</param>
    /// <param name = "emitted">Signalled as soon as the combined sequence emits.</param>
    /// <returns>The callback passed to <c>SubscribeAsync</c>.</returns>
    private static Func<int, CancellationToken, ValueTask> RecordAndSignalValues(
        List<int> results,
        TaskCompletionSource emitted) =>
        (value, _) =>
        {
            results.Add(value);
            IgnoredResult.Of(emitted.TrySetResult());
            return default;
        };
}
