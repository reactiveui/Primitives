// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for the OnDispose operator.</summary>
public partial class CombiningOperatorTests
{
    /// <summary>Verifies that the synchronous OnDispose overload forwards OnNext values to the downstream observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnDisposeSyncOnNext_ThenForwardsValues()
    {
        List<int> items = [];
        var disposed = false;

        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnNextAsync(SampleValue1, ct);
            await observer.OnNextAsync(SampleValue2, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        await using var sub = await source
            .OnDispose(() => disposed = true)
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null);

        await Assert.That(items).IsCollectionEqualTo([SampleValue1, SampleValue2]);
        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Verifies that the synchronous OnDispose overload forwards OnErrorResume to the downstream observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnDisposeSyncOnErrorResume_ThenForwardsError()
    {
        List<Exception> errors = [];

        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException("resume"), ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        await using var sub = await source
            .OnDispose(static () => { })
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0].Message).IsEqualTo("resume");
    }

    /// <summary>Verifies that the synchronous OnDispose overload forwards OnCompleted with failure to the downstream observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnDisposeSyncOnCompletedFailure_ThenForwardsFailure()
    {
        Result? completionResult = null;

        var source = SignalAsync.Create<int>(static async (observer, _) =>
        {
            await observer.OnCompletedAsync(Result.Failure(new InvalidOperationException("fail")));
            return DisposableAsync.Empty;
        });

        await using var sub = await source
            .OnDispose(static () => { })
            .SubscribeAsync(
                static (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Verifies that the synchronous OnDispose action is invoked when the subscription is explicitly disposed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnDisposeSyncExplicitDispose_ThenActionInvoked()
    {
        var disposed = false;
        var signal = Signal.Create<int>();

        await using var sub = await signal.Values
            .OnDispose(() => disposed = true)
            .SubscribeAsync(
                static (_, _) => default,
                null);

        await Assert.That(disposed).IsFalse();

        await signal.OnCompletedAsync(Result.Success);

        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Verifies that the asynchronous OnDispose overload forwards OnNext values to the downstream observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCleanupBranchAsyncOnNext_ThenForwardsValues()
    {
        List<int> items = [];
        var disposed = false;

        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnNextAsync(SampleValue1, ct);
            await observer.OnNextAsync(SampleValue2, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        await using var sub = await source
            .OnDispose(() =>
            {
                disposed = true;
                return default;
            })
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null);

        await Assert.That(items).IsCollectionEqualTo([SampleValue1, SampleValue2]);
        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Verifies that the asynchronous OnDispose overload forwards OnErrorResume to the downstream observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCleanupBranchAsyncOnErrorResume_ThenForwardsError()
    {
        List<Exception> errors = [];

        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException("async resume"), ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        await using var sub = await source
            .OnDispose(static () => default)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0].Message).IsEqualTo("async resume");
    }

    /// <summary>Verifies that the asynchronous OnDispose overload forwards OnCompleted with failure to the downstream observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCleanupBranchAsyncOnCompletedFailure_ThenForwardsFailure()
    {
        Result? completionResult = null;

        var source = SignalAsync.Create<int>(static async (observer, _) =>
        {
            await observer.OnCompletedAsync(Result.Failure(new InvalidOperationException("async fail")));
            return DisposableAsync.Empty;
        });

        await using var sub = await source
            .OnDispose(static () => default)
            .SubscribeAsync(
                static (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Verifies that the asynchronous OnDispose callback is invoked when the subscription is explicitly disposed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCleanupBranchAsyncExplicitDispose_ThenCallbackInvoked()
    {
        var disposed = false;
        var signal = Signal.Create<int>();

        await using var sub = await signal.Values
            .OnDispose(() =>
            {
                disposed = true;
                return default;
            })
            .SubscribeAsync(
                static (_, _) => default,
                null);

        await Assert.That(disposed).IsFalse();

        await signal.OnCompletedAsync(Result.Success);

        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Verifies BlendCoordinator.RelayNextAsync pre-gate disposed guard returns early.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBlendCoordinatorDisposed_ThenRelayNextAsyncReturnsDirectly()
    {
        CallbackWitnessAsync<int> observer = new(static (_, _) => default);
        SignalAsyncExtensions.BlendCoordinator<int> subscription = new(observer);
        await subscription.DisposeAsync();

        await subscription.RelayNextAsync(Sentinel99, CancellationToken.None);
    }

    /// <summary>Verifies BlendCoordinator.RelayErrorAsync pre-gate disposed guard returns early.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBlendCoordinatorDisposed_ThenRelayErrorAsyncReturnsDirectly()
    {
        CallbackWitnessAsync<int> observer = new(static (_, _) => default);
        SignalAsyncExtensions.BlendCoordinator<int> subscription = new(observer);
        await subscription.DisposeAsync();

        await subscription.RelayErrorAsync(new InvalidOperationException("test"), CancellationToken.None);
    }

    /// <summary>
    /// Verifies BlendCoordinator.RelayNextAsync post-gate disposed guard.
    /// Directly calls OnNextAsync on the subscription while FinishAsync blocks on downstream completion.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBlendCoordinatorDisposedWhileGateHeld_ThenRelayNextAsyncPostGateReturns()
    {
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> items = [];

        CallbackWitnessAsync<int> observer = new(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null,
            async _ =>
            {
                IgnoredResult.Of(completionBlocked.TrySetResult());
                await allowCompletion.Task;
            });

        SignalAsyncExtensions.BlendCoordinator<int> subscription = new(observer);

        // Trigger FinishAsync with failure - blocks on observer.OnCompletedAsync
        var failTask = Task.Run(() =>
            subscription.FinishAsync(Result.Failure(new InvalidOperationException("fail"))));
        await completionBlocked.Task;

        // _disposed is 1, gate is still alive → OnNextAsync acquires gate and hits post-gate check
        await subscription.RelayNextAsync(Sentinel99, CancellationToken.None);

        await Assert.That(items).IsEmpty();

        _ = allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>Verifies BlendCoordinator.RelayErrorAsync post-gate disposed guard.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBlendCoordinatorDisposedWhileGateHeld_ThenRelayErrorAsyncPostGateReturns()
    {
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<Exception> errors = [];

        CallbackWitnessAsync<int> observer = new(
            static (_, _) => default,
            (ex, _) =>
            {
                errors.Add(ex);
                return default;
            },
            async _ =>
            {
                IgnoredResult.Of(completionBlocked.TrySetResult());
                await allowCompletion.Task;
            });

        SignalAsyncExtensions.BlendCoordinator<int> subscription = new(observer);

        var failTask = Task.Run(() =>
            subscription.FinishAsync(Result.Failure(new InvalidOperationException("fail"))));
        await completionBlocked.Task;

        await subscription.RelayErrorAsync(new InvalidOperationException("post-dispose"), CancellationToken.None);

        await Assert.That(errors).IsEmpty();

        _ = allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>Verifies that BlendSequenceCoordinator.RelayNextAsync returns early when called directly after disposal.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBlendSequenceCoordinatorDisposed_ThenOnNextReturnsDirectly()
    {
        CallbackWitnessAsync<int> observer = new(static (_, _) => default);
        IObservableAsync<int>[] sources = [];
        SignalAsyncExtensions.BlendEnumerableSignal<int>.BlendSequenceCoordinator subscription = new(observer, sources);
        subscription.BeginSubscribing();
        await subscription.DisposeAsync();

        await subscription.RelayNextAsync(Sentinel99, CancellationToken.None);
    }

    /// <summary>Verifies that BlendSequenceCoordinator.RelayErrorAsync returns early when called directly after disposal.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBlendSequenceCoordinatorDisposed_ThenOnErrorResumeReturnsDirectly()
    {
        CallbackWitnessAsync<int> observer = new(static (_, _) => default);
        IObservableAsync<int>[] sources = [];
        SignalAsyncExtensions.BlendEnumerableSignal<int>.BlendSequenceCoordinator subscription = new(observer, sources);
        subscription.BeginSubscribing();
        await subscription.DisposeAsync();

        await subscription.RelayErrorAsync(new InvalidOperationException("test"), CancellationToken.None);
    }
}
