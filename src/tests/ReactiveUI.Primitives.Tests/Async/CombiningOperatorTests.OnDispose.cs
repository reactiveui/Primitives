// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Internals;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <content>
/// OnDispose tests for combining operators.
/// </content>
public partial class CombiningOperatorTests
{
    /// <summary>
    /// Verifies that the synchronous OnDispose overload forwards OnNext values to the downstream observer.
    /// </summary>
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

    /// <summary>
    /// Verifies that the synchronous OnDispose overload forwards OnErrorResume to the downstream observer.
    /// </summary>
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

    /// <summary>
    /// Verifies that the synchronous OnDispose overload forwards OnCompleted with failure to the downstream observer.
    /// </summary>
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

    /// <summary>
    /// Verifies that the synchronous OnDispose action is invoked when the subscription is explicitly disposed.
    /// </summary>
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

    /// <summary>
    /// Verifies that the asynchronous OnDispose overload forwards OnNext values to the downstream observer.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnDisposeAsyncOnNext_ThenForwardsValues()
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

    /// <summary>
    /// Verifies that the asynchronous OnDispose overload forwards OnErrorResume to the downstream observer.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnDisposeAsyncOnErrorResume_ThenForwardsError()
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

    /// <summary>
    /// Verifies that the asynchronous OnDispose overload forwards OnCompleted with failure to the downstream observer.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnDisposeAsyncOnCompletedFailure_ThenForwardsFailure()
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

    /// <summary>
    /// Verifies that the asynchronous OnDispose callback is invoked when the subscription is explicitly disposed.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnDisposeAsyncExplicitDispose_ThenCallbackInvoked()
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

    /// <summary>
    /// Verifies MergeSubscription.ForwardOnNext pre-gate disposed guard returns early.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSubscriptionDisposed_ThenForwardOnNextReturnsDirectly()
    {
        var observer = new AnonymousObserverAsync<int>((_, _) => default);
        var subscription = new SignalAsync.MergeSubscription<int>(observer);
        await subscription.DisposeAsync();

        await subscription.ForwardOnNext(Sentinel99, CancellationToken.None);
    }

    /// <summary>
    /// Verifies MergeSubscription.ForwardOnErrorResume pre-gate disposed guard returns early.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSubscriptionDisposed_ThenForwardOnErrorResumeReturnsDirectly()
    {
        var observer = new AnonymousObserverAsync<int>((_, _) => default);
        var subscription = new SignalAsync.MergeSubscription<int>(observer);
        await subscription.DisposeAsync();

        await subscription.ForwardOnErrorResume(new InvalidOperationException("test"), CancellationToken.None);
    }

    /// <summary>
    /// Verifies MergeSubscription.ForwardOnNext post-gate disposed guard.
    /// Directly calls ForwardOnNext on the subscription while CompleteAsync blocks on downstream completion.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSubscriptionDisposedWhileGateHeld_ThenForwardOnNextPostGateReturns()
    {
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var items = new List<int>();

        var observer = new AnonymousObserverAsync<int>(
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

        var subscription = new SignalAsync.MergeSubscription<int>(observer);

        // Trigger CompleteAsync with failure - blocks on observer.OnCompletedAsync
        var failTask = Task.Run(() =>
            subscription.CompleteAsync(Result.Failure(new InvalidOperationException("fail"))));
        await completionBlocked.Task;

        // _disposed is 1, gate is still alive → ForwardOnNext acquires gate and hits post-gate check
        await subscription.ForwardOnNext(Sentinel99, CancellationToken.None);

        await Assert.That(items).IsEmpty();

        allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies MergeSubscription.ForwardOnErrorResume post-gate disposed guard.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSubscriptionDisposedWhileGateHeld_ThenForwardOnErrorResumePostGateReturns()
    {
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var errors = new List<Exception>();

        var observer = new AnonymousObserverAsync<int>(
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

        var subscription = new SignalAsync.MergeSubscription<int>(observer);

        var failTask = Task.Run(() =>
            subscription.CompleteAsync(Result.Failure(new InvalidOperationException("fail"))));
        await completionBlocked.Task;

        await subscription.ForwardOnErrorResume(new InvalidOperationException("post-dispose"), CancellationToken.None);

        await Assert.That(errors).IsEmpty();

        allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that MergeEnumerableSubscription.OnNextAsync returns early when called directly after disposal.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableSubscriptionDisposed_ThenOnNextReturnsDirectly()
    {
        var observer = new AnonymousObserverAsync<int>((_, _) => default);
        IObservableAsync<int>[] sources = [];
        var subscription =
            new SignalAsync.MergeEnumerableSignal<int>.MergeEnumerableSubscription(observer, sources);
        subscription.StartAsync();
        await subscription.DisposeAsync();

        await subscription.OnNextAsync(Sentinel99, CancellationToken.None);
    }

    /// <summary>
    /// Verifies that MergeEnumerableSubscription.OnErrorResumeAsync returns early when called directly after disposal.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableSubscriptionDisposed_ThenOnErrorResumeReturnsDirectly()
    {
        var observer = new AnonymousObserverAsync<int>((_, _) => default);
        IObservableAsync<int>[] sources = [];
        var subscription =
            new SignalAsync.MergeEnumerableSignal<int>.MergeEnumerableSubscription(observer, sources);
        subscription.StartAsync();
        await subscription.DisposeAsync();

        await subscription.OnErrorResumeAsync(new InvalidOperationException("test"), CancellationToken.None);
    }
}
