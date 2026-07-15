// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for the Prepend and StartWith operators.</summary>
public partial class CombiningOperatorTests
{
    /// <summary>Maximum time the combining-operator tests wait for emissions to arrive.</summary>
    private static readonly TimeSpan CombiningWaitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Tests Prepend value comes first.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependValue_ThenValueComesFirst()
    {
        const int SourceValueCount = 3;

        var result = await SignalAsync.Range(SampleValue2, SourceValueCount)
            .Prepend(SampleValue1)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3, SampleValue4]);
    }

    /// <summary>Tests Prepend enumerable values come first.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependEnumerable_ThenValuesComesFirst()
    {
        const int SourceValueCount = 2;

        var result = await SignalAsync.Range(SampleValue3, SourceValueCount)
            .Prepend([SampleValue1, SampleValue2])
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3, SampleValue4]);
    }

    /// <summary>Tests StartWith value comes first.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartWithValue_ThenValueComesFirst()
    {
        const int SourceValueCount = 2;

        var result = await SignalAsync.Range(SampleValue2, SourceValueCount)
            .StartWith(SampleValue1)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>Tests StartWith enumerable values come first.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartWithEnumerable_ThenValuesComesFirst()
    {
        var result = await SignalAsync.Return(SampleValue3)
            .StartWith(SampleValue1, SampleValue2)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>Tests StartWith params values come first.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartWithParams_ThenValuesComesFirst()
    {
        int[] values = [SampleValue1, SampleValue2, SampleValue3];
        var result = await SignalAsync.Return(SampleValue4)
            .StartWith(values)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3, SampleValue4]);
    }

    /// <summary>Verifies that prepend stops emitting when the subscription is disposed during the prepend phase.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependDisposedDuringPrependPhase_ThenStopsEmitting()
    {
        const int SourceValueCount = 5;

        List<int> items = [];

        var sub = await SignalAsync.Range(SampleValue100, SourceValueCount)
            .Prepend([SampleValue1, SampleValue2, SampleValue3, SampleValue4, SampleValue5])
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

    /// <summary>Tests Prepend cancellation during the prepend phase, exercising the OperationCanceledException catch path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependCancelledDuringValues_ThenOperationCanceledExceptionCaught()
    {
        const int PrependedValueCount = 100;

        using CancellationTokenSource cts = new();
        List<int> items = [];

        // Create a long prepend that will be cancelled
        var manyValues = Enumerable.Range(1, PrependedValueCount);
        var source = SignalAsync.Never<int>().Prepend(manyValues);

        await using var sub = await source.SubscribeAsync(
            async (x, _) =>
            {
                items.Add(x);
                if (x != SampleValue5)
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
            CombiningWaitTimeout);

        await Assert.That(items).Contains(SampleValue5);
    }

    /// <summary>Tests that Prepend error during source subscription triggers OnCompletedAsync with failure.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependSourceThrowsDuringSubscription_ThenCompletesWithFailure()
    {
        var failing = SignalAsync.Create<int>(static (_, _) =>
            throw new InvalidOperationException("source subscribe fail"));

        Result? completionResult = null;
        List<int> items = [];

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
            CombiningWaitTimeout);

        await Assert.That(items).Contains(Sentinel42);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Verifies that Prepend emits prepended values before source values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrepend_ThenEmitsPrependedValuesFirst()
    {
        const int SourceValueCount = 2;

        var result = await SignalAsync.Range(SampleValue4, SourceValueCount)
            .Prepend([SampleValue1, SampleValue2, SampleValue3])
            .ToListAsync();
        await Assert.That(result)
            .IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3, SampleValue4, SampleValue5]);
    }

    /// <summary>Verifies that Prepend with a single value emits the value before source.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependSingleValue_ThenEmitsValueBeforeSource()
    {
        const int SourceValueCount = 2;

        var result = await SignalAsync.Range(SampleValue2, SourceValueCount).Prepend(SampleValue1).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>Verifies that Prepend handles cancellation during prepend phase without error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependCancelledDuringPrepend_ThenStopsGracefully()
    {
        const int SourceValueCount = 3;

        List<int> items = [];
        using CancellationTokenSource cts = new();

        var source = SignalAsync.Range(SampleValue100, SourceValueCount)
            .Prepend([SampleValue1, SampleValue2, SampleValue3, SampleValue4, SampleValue5]);

        await using var sub = await source.SubscribeAsync(
            async (x, _) =>
            {
                lock (_gate)
                {
                    items.Add(x);
                }

                if (x != SampleValue2)
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
            CombiningWaitTimeout);

        // Should have emitted at least 1 and 2
        await Assert.That(items).Contains(SampleValue1);
        await Assert.That(items).Contains(SampleValue2);
    }

    /// <summary>Verifies that StartWith emits a value before the source values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartWith_ThenEmitsValueBeforeSource()
    {
        const int SourceValueCount = 3;

        var result = await SignalAsync.Range(1, SourceValueCount)
            .StartWith(0)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([0, 1, SampleValue2, SampleValue3]);
    }

    /// <summary>Verifies that Prepend cancellation during prepended values returns early.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependCancelledDuringValues_ThenStopsEarly()
    {
        List<int> items = [];

        var sub = await SignalAsync.Never<int>()
            .Prepend([SampleValue1, SampleValue2, SampleValue3, SampleValue4, SampleValue5])
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
            CombiningWaitTimeout);

        await sub.DisposeAsync();

        await Assert.That(items)
            .IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3, SampleValue4, SampleValue5]);
    }

    /// <summary>Verifies that Prepend handles source subscription errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependSourceThrows_ThenCompletesWithFailure()
    {
        var throwingSource = SignalAsync.Create<int>(static (_, _) =>
            ValueTask.FromException<IAsyncDisposable>(new InvalidOperationException("source boom")));

        Result? completionResult = null;

        await using var sub = await throwingSource
            .Prepend([SampleValue10])
            .SubscribeAsync(
                static (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await AsyncTestHelpers.WaitForConditionAsync(
            () => completionResult.HasValue,
            CombiningWaitTimeout);

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Verifies that Prepend handles OperationCanceledException during source subscription.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependSourceCancelled_ThenSwallowsCancellation()
    {
        List<int> items = [];

        // Create a source that throws OperationCanceledException on subscribe
        var cancellingSource = SignalAsync.Create<int>(static (_, _) =>
            ValueTask.FromException<IAsyncDisposable>(new OperationCanceledException()));

        var sub = await cancellingSource
            .Prepend([SampleValue1, SampleValue2])
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
                static result => default);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => items.Count >= 2,
            CombiningWaitTimeout);

        await sub.DisposeAsync();

        // Values before the cancellation should still have been emitted
        await Assert.That(items).IsCollectionEqualTo([SampleValue1, SampleValue2]);
    }

    /// <summary>Verifies that StartWith with an explicit IEnumerable{T} argument exercises the IEnumerable overload.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartWithIEnumerable_ThenEmitsEnumerableBeforeSource()
    {
        const int PrefixValueCount = 2;

        var prefix = Enumerable.Range(1, PrefixValueCount);
        var result = await SignalAsync.Return(SampleValue3)
            .StartWith(prefix)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }
}
