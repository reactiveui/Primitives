// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests cancellation completion through the serialized notification path.</summary>
public sealed class CancellationStopCoordinatorTests
{
    /// <summary>The recorded value notification.</summary>
    private const string ValueNotification = "value";

    /// <summary>The recorded start of completion.</summary>
    private const string CompletionEntered = "completion entered";

    /// <summary>Disposing before the sources are subscribed releases the coordinator without a token registration or source subscription.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task DisposeAsync_BeforeSubscribe_ReleasesWithoutRegistration()
    {
        List<Result> completions = [];
        CallbackWitnessAsync<int> observer = new(static (_, _) => default, null, result =>
        {
            completions.Add(result);
            return default;
        });
        CancellationStopSignal<int> signal = new(SignalAsync.Never<int>(), CancellationToken.None);
        CancellationStopCoordinator<int> coordinator = new(signal, observer);

        await Assert.That(async () => await coordinator.DisposeAsync()).ThrowsNothing();
        await Assert.That(completions).IsEmpty();
    }

    /// <summary>Completion waits for the downstream completion callback.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task CompleteFromCancellationAsync_WhenCompletionIsPending_ThenWaitsForObserver()
    {
        var source = Signal.Create<int>();
        TaskCompletionSource resume = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<string> calls = [];
        CallbackWitnessAsync<int> observer = new(
            (_, _) =>
        {
            calls.Add(ValueNotification);
            return default;
        },
            null,
            async result =>
        {
            calls.Add(result.IsSuccess ? CompletionEntered : "failed");
            await resume.Task;
            calls.Add("completion finished");
        });
        CancellationStopSignal<int> signal = new(source.Values, CancellationToken.None);
        await using CancellationStopCoordinator<int> coordinator = new(signal, observer);
        await coordinator.SubscribeSourcesAsync(CancellationToken.None);
        await source.OnNextAsync(1, CancellationToken.None);
        var completion = coordinator.CompleteFromCancellationAsync().AsTask();
        await Assert.That(completion.IsCompleted).IsFalse();
        await Assert.That(calls).IsCollectionEqualTo([ValueNotification, CompletionEntered]);
        resume.SetResult();
        await completion;
        await Assert.That(calls).IsCollectionEqualTo([ValueNotification, CompletionEntered, "completion finished"]);
    }

    /// <summary>Cancelling the stop token during source subscription disposes the newly attached source subscription.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task SubscribeSourcesAsync_WhenTokenCancelledDuringSubscribe_DisposesSubscription()
    {
        using CancellationTokenSource cts = new();
        int[] disposed = [0];
        var source = SignalAsync.Create<int>(async (_, _) =>
        {
            await cts.CancelAsync();
            return ReactiveUI.Primitives.Async.Disposables.DisposableAsync.Create(
                disposed,
                static state =>
                {
                    _ = Interlocked.Increment(ref state[0]);
                    return default;
                });
        });

        CallbackWitnessAsync<int> observer = new(static (_, _) => default);
        CancellationStopSignal<int> signal = new(source, cts.Token);
        await using CancellationStopCoordinator<int> coordinator = new(signal, observer);
        await coordinator.SubscribeSourcesAsync(CancellationToken.None);

        await Assert.That(Volatile.Read(ref disposed[0])).IsEqualTo(1);
    }
}
