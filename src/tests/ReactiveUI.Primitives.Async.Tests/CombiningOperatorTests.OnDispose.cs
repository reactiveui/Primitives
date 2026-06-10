// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Internals;
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
        var items = new List<int>();
        var disposed = false;

        var source = SignalAsync.Create<int>(async (observer, ct) =>
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
        var errors = new List<Exception>();

        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException("resume"), ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        await using var sub = await source
            .OnDispose(() => { })
            .SubscribeAsync(
                (_, _) => default,
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

        var source = SignalAsync.Create<int>(async (observer, _) =>
        {
            await observer.OnCompletedAsync(Result.Failure(new InvalidOperationException("fail")));
            return DisposableAsync.Empty;
        });

        await using var sub = await source
            .OnDispose(() => { })
            .SubscribeAsync(
                (_, _) => default,
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
                (_, _) => default,
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
        var items = new List<int>();
        var disposed = false;

        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnNextAsync(2, ct);
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
        var errors = new List<Exception>();

        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException("async resume"), ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        await using var sub = await source
            .OnDispose(() => default)
            .SubscribeAsync(
                (_, _) => default,
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

        var source = SignalAsync.Create<int>(async (observer, _) =>
        {
            await observer.OnCompletedAsync(Result.Failure(new InvalidOperationException("async fail")));
            return DisposableAsync.Empty;
        });

        await using var sub = await source
            .OnDispose(() => default)
            .SubscribeAsync(
                (_, _) => default,
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
                (_, _) => default,
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
        var observer = new CallbackWitnessAsync<int>((_, _) => default);
        var subscription = new SignalAsyncExtensions.BlendCoordinator<int>(observer);
        await subscription.DisposeAsync();

        await subscription.RelayNextAsync(Sentinel99, CancellationToken.None);
    }

    /// <summary>Verifies BlendCoordinator.RelayErrorAsync pre-gate disposed guard returns early.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBlendCoordinatorDisposed_ThenRelayErrorAsyncReturnsDirectly()
    {
        var observer = new CallbackWitnessAsync<int>((_, _) => default);
        var subscription = new SignalAsyncExtensions.BlendCoordinator<int>(observer);
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
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var items = new List<int>();

        var observer = new CallbackWitnessAsync<int>(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null,
            async _ =>
            {
                completionBlocked.TrySetResult();
                await allowCompletion.Task;
            });

        var subscription = new SignalAsyncExtensions.BlendCoordinator<int>(observer);

        // Trigger FinishAsync with failure - blocks on observer.OnCompletedAsync
        var failTask = Task.Run(() =>
            subscription.FinishAsync(Result.Failure(new InvalidOperationException("fail"))));
        await completionBlocked.Task;

        // _disposed is 1, gate is still alive → OnNextAsync acquires gate and hits post-gate check
        await subscription.RelayNextAsync(Sentinel99, CancellationToken.None);

        await Assert.That(items).IsEmpty();

        allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>Verifies BlendCoordinator.RelayErrorAsync post-gate disposed guard.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBlendCoordinatorDisposedWhileGateHeld_ThenRelayErrorAsyncPostGateReturns()
    {
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var errors = new List<Exception>();

        var observer = new CallbackWitnessAsync<int>(
            (_, _) => default,
            (ex, _) =>
            {
                errors.Add(ex);
                return default;
            },
            async _ =>
            {
                completionBlocked.TrySetResult();
                await allowCompletion.Task;
            });

        var subscription = new SignalAsyncExtensions.BlendCoordinator<int>(observer);

        var failTask = Task.Run(() =>
            subscription.FinishAsync(Result.Failure(new InvalidOperationException("fail"))));
        await completionBlocked.Task;

        await subscription.RelayErrorAsync(new InvalidOperationException("post-dispose"), CancellationToken.None);

        await Assert.That(errors).IsEmpty();

        allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>Verifies that BlendSequenceCoordinator.RelayNextAsync returns early when called directly after disposal.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBlendSequenceCoordinatorDisposed_ThenOnNextReturnsDirectly()
    {
        var observer = new CallbackWitnessAsync<int>((_, _) => default);
        IObservableAsync<int>[] sources = [];
        var subscription =
            new SignalAsyncExtensions.BlendEnumerableSignal<int>.BlendSequenceCoordinator(observer, sources);
        subscription.BeginSubscribing();
        await subscription.DisposeAsync();

        await subscription.RelayNextAsync(Sentinel99, CancellationToken.None);
    }

    /// <summary>Verifies that BlendSequenceCoordinator.RelayErrorAsync returns early when called directly after disposal.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBlendSequenceCoordinatorDisposed_ThenOnErrorResumeReturnsDirectly()
    {
        var observer = new CallbackWitnessAsync<int>((_, _) => default);
        IObservableAsync<int>[] sources = [];
        var subscription =
            new SignalAsyncExtensions.BlendEnumerableSignal<int>.BlendSequenceCoordinator(observer, sources);
        subscription.BeginSubscribing();
        await subscription.DisposeAsync();

        await subscription.RelayErrorAsync(new InvalidOperationException("test"), CancellationToken.None);
    }
}
