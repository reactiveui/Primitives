// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;
using AsyncObs = ReactiveUI.Primitives.Async.SignalAsync;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for CombineLatestOperatorTests.</summary>
public partial class CombineLatestOperatorTests
{
    /// <summary>Tests CombineLatestEnumerable SubscribeAsyncCore catch block when a source throws during subscription.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableSourceThrowsDuringSubscribe_ThenDisposesAndRethrows()
    {
        var failing = AsyncObs.Create<int>((_, _) => throw new InvalidOperationException("subscribe fail"));
        IObservableAsync<int>[] sources = [AsyncObs.Return(1), failing];
        await Assert.That(async () => await sources.CombineLatest().ToListAsync())
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Tests CombineLatestEnumerable OnErrorResumeAsync is forwarded when disposed is not set.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableErrorResume_ThenForwardedToObserver()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        Exception? received = null;
        IObservableAsync<int>[] sources = [s1.Values, s2.Values];
        await using var sub = await sources.CombineLatest().SubscribeAsync((_, _) => default, (ex, _) =>
        {
            received = ex;
            return default;
        });
        await s1.OnErrorResumeAsync(new InvalidOperationException("resume"), CancellationToken.None);
        await AsyncTestHelpers.WaitForConditionAsync(
            () => received is not null,
            TimeSpan.FromSeconds(WaitTimeoutSeconds));
        await Assert.That(received).IsNotNull();
        await Assert.That(received!.Message).IsEqualTo("resume");
    }

    /// <summary>Tests CombineLatestEnumerable OnErrorResumeAsync returns early when disposed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableErrorResumeAfterDisposed_ThenIgnored()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        Exception? received = null;
        IObservableAsync<int>[] sources = [s1.Values, s2.Values];
        var sub = await sources.CombineLatest().SubscribeAsync((_, _) => default, (ex, _) =>
        {
            received = ex;
            return default;
        });
        await sub.DisposeAsync();

        // After disposal, error resume should be ignored
        await s1.OnErrorResumeAsync(new InvalidOperationException("late error"), CancellationToken.None);
        await Assert.That(received).IsNull();
    }

    /// <summary>Tests that CombineLatestEnumerable completes when a source completes without ever emitting a value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableSourceCompletesWithoutEmitting_ThenCompletes()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        Result? completionResult = null;
        IObservableAsync<int>[] sources = [s1.Values, s2.Values];
        await using var sub = await sources.CombineLatest().SubscribeAsync((_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });

        // s1 completes without emitting - should trigger completion since !_values[0].HasValue
        await s1.OnCompletedAsync(Result.Success);
        await AsyncTestHelpers.WaitForConditionAsync(
            () => completionResult is not null,
            TimeSpan.FromSeconds(WaitTimeoutSeconds));
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Tests that CombineLatestValuesAreAllFalse returns true for an empty source set.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestValuesAreAllFalseWithNoSources_ThenReturnsTrue()
    {
        IObservableAsync<bool>[] sources = [];
        var result = await sources.CombineLatestValuesAreAllFalse().FirstAsync();
        await Assert.That(result).IsTrue();
    }

    /// <summary>Tests CombineLatestValuesAreAllTrue with empty IEnumerable returns true (vacuous truth).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestValuesAreAllTrueEmptyList_ThenReturnsTrue()
    {
        var result = await new List<IObservableAsync<bool>>().CombineLatestValuesAreAllTrue().FirstAsync();
        await Assert.That(result).IsTrue();
    }
}
