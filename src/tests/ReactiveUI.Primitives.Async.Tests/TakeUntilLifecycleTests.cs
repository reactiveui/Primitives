// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests the teardown of the take-until notification lifecycle.</summary>
public sealed class TakeUntilLifecycleTests
{
    /// <summary>A throwing cancellation callback still releases the lifecycle before the failure surfaces.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DisposeAsync_CancellationCallbackThrows_ReleasesAndRethrows()
    {
        CallbackWitnessAsync<int> observer = new(static (_, _) => default);
        TakeUntilLifecycle<int> lifecycle = new(observer);
        Exception expected = new InvalidOperationException("cancellation callback failed");
        await using var registration = lifecycle.DisposeToken.UnsafeRegister(static state => throw (Exception)state!, expected);

        var error = await Assert.That(async () => await lifecycle.DisposeAsync()).ThrowsExactly<AggregateException>();

        await Assert.That(error!.InnerExceptions).IsCollectionEqualTo([expected]);
        await Assert.That(lifecycle.DisposeToken.IsCancellationRequested).IsTrue();
    }

    /// <summary>Verifies <see cref="TakeUntilLifecycle{T}.CompleteWhenCancelled(CancellationToken)"/> handles cancellable and non-cancellable tokens and deduplicates completion.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CompleteWhenCancelledAndDuplicateRelayCompletion_CompleteDownstreamOnce()
    {
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var completionCount = 0;
        CallbackWitnessAsync<int> observer = new(
            static (_, _) => default,
            null,
            result =>
            {
                _ = result;
                completionCount++;
                _ = completed.TrySetResult();
                return default;
            });
        await using TakeUntilLifecycle<int> lifecycle = new(observer);

        await using var nonCancellable = lifecycle.CompleteWhenCancelled(CancellationToken.None);
        using CancellationTokenSource cts = new();
        await using var cancellable = lifecycle.CompleteWhenCancelled(cts.Token);

        await cts.CancelAsync();
        await completed.Task;
        await lifecycle.RelayCompletionAsync(Result.Success);

        await Assert.That(completionCount).IsEqualTo(1);
    }

    /// <summary>Verifies three-argument stop-signal constructors and post-termination witness guards.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task StopSignalThreeArgConstructorsAndWitnessPostTerminalGuards_CompleteCleanly()
    {
        DelegateStopSignal<int> delegateStop = new(SignalAsync.Never<int>(), static _ => DisposableAsync.Empty, TakeUntilOptions.Default);
        TaskStopSignal<int> taskStop = new(SignalAsync.Never<int>(), Task.CompletedTask, TakeUntilOptions.Default);
        TakeUntilAsyncSignal<int, int> asyncStop = new(SignalAsync.Never<int>(), SignalAsync.Never<int>(), TakeUntilOptions.Default);
        await Assert.That(delegateStop).IsNotNull();
        await Assert.That(taskStop).IsNotNull();
        await Assert.That(asyncStop).IsNotNull();

        CallbackWitnessAsync<int> observer = new(static (_, _) => default);
        await using TakeWhileAsyncWitness<int> asyncWhile = new(observer, static (x, _) => new(x > 0), CancellationToken.None);
        await ((IWitnessAsync<int>)asyncWhile).OnCompletedAsyncCore(Result.Success);
        await ((IWitnessAsync<int>)asyncWhile).OnNextAsyncCore(0, CancellationToken.None);
        await ((IWitnessAsync<int>)asyncWhile).OnNextAsyncCore(1, CancellationToken.None);

        await using TakeWhileSyncWitness<int> syncWhile = new(observer, static x => x > 0, CancellationToken.None);
        await ((IWitnessAsync<int>)syncWhile).OnNextAsyncCore(0, CancellationToken.None);
        await ((IWitnessAsync<int>)syncWhile).OnNextAsyncCore(1, CancellationToken.None);

        await using TakeWitness<int> takeWitness = new(observer, 1, CancellationToken.None);
        await ((IWitnessAsync<int>)takeWitness).OnNextAsyncCore(1, CancellationToken.None);
        await ((IWitnessAsync<int>)takeWitness).OnNextAsyncCore(1, CancellationToken.None);

        List<Exception> logged = [];
        Exception failure = new InvalidOperationException("terminal");
        await using LogErrorsWitness<int> logWitness = new(observer, logged.Add);
        await logWitness.OnCompletedAsync(Result.Failure(failure));
        await Assert.That(logged).IsCollectionEqualTo([failure]);
    }
}
