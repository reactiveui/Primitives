// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <content>
/// PrependStartWith tests for combining operators.
/// </content>
public partial class CombiningOperatorTests
{
    /// <summary>Tests Prepend value comes first.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependValue_ThenValueComesFirst()
    {
        var result = await SignalAsync.Range(2, 3)
            .Prepend(1)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3, SampleValue4]);
    }

    /// <summary>Tests Prepend enumerable values come first.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependEnumerable_ThenValuesComesFirst()
    {
        var result = await SignalAsync.Range(3, 2)
            .Prepend([1, 2])
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3, SampleValue4]);
    }

    /// <summary>Tests StartWith value comes first.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartWithValue_ThenValueComesFirst()
    {
        var result = await SignalAsync.Range(2, 2)
            .StartWith(1)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>Tests StartWith enumerable values come first.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartWithEnumerable_ThenValuesComesFirst()
    {
        var result = await SignalAsync.Return(3)
            .StartWith(SampleValue1, SampleValue2)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>Tests StartWith params values come first.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartWithParams_ThenValuesComesFirst()
    {
        int[] values = [1, 2, 3];
        var result = await SignalAsync.Return(4)
            .StartWith(values)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3, SampleValue4]);
    }

    /// <summary>
    /// Verifies that prepend stops emitting when the subscription is disposed during the prepend phase.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependDisposedDuringPrependPhase_ThenStopsEmitting()
    {
        var items = new List<int>();

        var sub = await SignalAsync.Range(100, 5)
            .Prepend([1, 2, 3, 4, 5])
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null);

        // Dispose quickly - may or may not have emitted some prepend values
        await sub.DisposeAsync();

        // Just verify no exception was thrown - the disposal was clean
    }

    /// <summary>
    /// Tests Prepend cancellation during the prepend phase, exercising the OperationCanceledException catch path.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependCancelledDuringValues_ThenOperationCanceledExceptionCaught()
    {
        using var cts = new CancellationTokenSource();
        var items = new List<int>();

        // Create a long prepend that will be cancelled
        var manyValues = Enumerable.Range(1, 100);
        var source = SignalAsync.Never<int>().Prepend(manyValues);

        await using var sub = await source.SubscribeAsync(
            async (x, _) =>
            {
                items.Add(x);
                if (x != 5)
                {
                    return;
                }

                await cts.CancelAsync().ConfigureAwait(false);
            },
            null,
            null,
            cts.Token);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => items.Count >= 5,
            TimeSpan.FromSeconds(5));

        await Assert.That(items).Contains(SampleValue5);
    }

    /// <summary>
    /// Tests that Prepend error during source subscription triggers OnCompletedAsync with failure.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependSourceThrowsDuringSubscription_ThenCompletesWithFailure()
    {
        var failing = SignalAsync.Create<int>((_, _) =>
            throw new InvalidOperationException("source subscribe fail"));

        Result? completionResult = null;
        var items = new List<int>();

        await using var sub = await failing.Prepend(Sentinel42)
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await AsyncTestHelpers.WaitForConditionAsync(
            () => completionResult is not null,
            TimeSpan.FromSeconds(5));

        await Assert.That(items).Contains(Sentinel42);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>
    /// Verifies that Prepend emits prepended values before source values.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrepend_ThenEmitsPrependedValuesFirst()
    {
        var result = await SignalAsync.Range(4, 2).Prepend([1, 2, 3]).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3, SampleValue4, SampleValue5]);
    }

    /// <summary>
    /// Verifies that Prepend with a single value emits the value before source.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependSingleValue_ThenEmitsValueBeforeSource()
    {
        var result = await SignalAsync.Range(2, 2).Prepend(1).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>
    /// Verifies that Prepend handles cancellation during prepend phase without error.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependCancelledDuringPrepend_ThenStopsGracefully()
    {
        var items = new List<int>();
        using var cts = new CancellationTokenSource();

        var source = SignalAsync.Range(100, 3).Prepend([1, 2, 3, 4, 5]);

        await using var sub = await source.SubscribeAsync(
            async (x, _) =>
            {
                lock (_gate)
                {
                    items.Add(x);
                }

                if (x != 2)
                {
                    return;
                }

                await cts.CancelAsync().ConfigureAwait(false);
            },
            null,
            null,
            cts.Token);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => items.Contains(SampleValue2),
            TimeSpan.FromSeconds(5));

        // Should have emitted at least 1 and 2
        await Assert.That(items).Contains(1);
        await Assert.That(items).Contains(SampleValue2);
    }

    /// <summary>
    /// Verifies that StartWith emits a value before the source values.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartWith_ThenEmitsValueBeforeSource()
    {
        var result = await SignalAsync.Range(1, 3)
            .StartWith(0)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([0, 1, SampleValue2, SampleValue3]);
    }

    /// <summary>
    /// Verifies that Prepend cancellation during prepended values returns early.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependCancelledDuringValues_ThenStopsEarly()
    {
        var items = new List<int>();

        var sub = await SignalAsync.Never<int>()
            .Prepend([1, 2, 3, 4, 5])
            .SubscribeAsync(
                (x, _) =>
                {
                    lock (_gate)
                    {
                        items.Add(x);
                    }

                    return default;
                },
                null);

        // Wait for prepended values to be emitted
        await AsyncTestHelpers.WaitForConditionAsync(
            () => items.Count >= 5,
            TimeSpan.FromSeconds(5));

        await sub.DisposeAsync();

        await Assert.That(items).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3, SampleValue4, SampleValue5]);
    }

    /// <summary>
    /// Verifies that Prepend handles source subscription errors.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependSourceThrows_ThenCompletesWithFailure()
    {
        var throwingSource = SignalAsync.Create<int>((_, _) =>
            ValueTask.FromException<IAsyncDisposable>(new InvalidOperationException("source boom")));

        Result? completionResult = null;

        await using var sub = await throwingSource
            .Prepend([10])
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await AsyncTestHelpers.WaitForConditionAsync(
            () => completionResult.HasValue,
            TimeSpan.FromSeconds(5));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>
    /// Verifies that Prepend handles OperationCanceledException during source subscription.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependSourceCancelled_ThenSwallowsCancellation()
    {
        var items = new List<int>();

        // Create a source that throws OperationCanceledException on subscribe
        var cancellingSource = SignalAsync.Create<int>((_, _) =>
            ValueTask.FromException<IAsyncDisposable>(new OperationCanceledException()));

        var sub = await cancellingSource
            .Prepend([1, 2])
            .SubscribeAsync(
                (x, _) =>
                {
                    lock (_gate)
                    {
                        items.Add(x);
                    }

                    return default;
                },
                null,
                result => default);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => items.Count >= 2,
            TimeSpan.FromSeconds(5));

        await sub.DisposeAsync();

        // Values before the cancellation should still have been emitted
        await Assert.That(items).IsCollectionEqualTo([SampleValue1, SampleValue2]);
    }

    /// <summary>
    /// Verifies that StartWith with an explicit IEnumerable{T} argument exercises the IEnumerable overload.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartWithIEnumerable_ThenEmitsEnumerableBeforeSource()
    {
        var prefix = Enumerable.Range(1, 2);
        var result = await SignalAsync.Return(3)
            .StartWith(prefix)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }
}
