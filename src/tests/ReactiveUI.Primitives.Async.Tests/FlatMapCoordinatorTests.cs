// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Advanced;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Direct coverage for <see cref="FlatMapCoordinator{TResult}"/> lifecycle paths.</summary>
public class FlatMapCoordinatorTests
{
    /// <summary>Sample integer value one.</summary>
    private const int One = 1;

    /// <summary>Sample integer value two.</summary>
    private const int Two = 2;

    /// <summary>Verifies the flat-map coordinator relays values, error-resume notifications, and outer success completion.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRelayingNotifications_ThenObserverReceivesThem()
    {
        FlatMapRecordingObserver<int> observer = new();
        FlatMapCoordinator<int> coordinator = new(observer);
        InvalidOperationException expected = new("flat-map-resume");

        await coordinator.RelayNextAsync(One);
        await coordinator.RelayErrorAsync(expected);
        await coordinator.CompleteOuterAsync(Result.Success);

        await Assert.That(observer.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(observer.Completions[0].IsSuccess).IsTrue();
    }

    /// <summary>Verifies the flat-map coordinator waits for active inner sources before completing.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOuterCompletesBeforeInner_ThenWaitsForInner()
    {
        FlatMapRecordingObserver<int> observer = new();
        FlatMapCoordinator<int> coordinator = new(observer);
        DirectSource<int> inner = new();

        await coordinator.SubscribeInnerAsync(inner);
        await coordinator.CompleteOuterAsync(Result.Success);
        await Assert.That(observer.Completions.Count).IsEqualTo(0);

        await inner.EmitNext(Two);
        await inner.Complete(Result.Success);

        await Assert.That(observer.Values.SequenceEqual([Two])).IsTrue();
        await Assert.That(observer.Completions[0].IsSuccess).IsTrue();
    }

    /// <summary>Verifies the flat-map coordinator forwards inner and subscription failures as terminal results.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenInnerFails_ThenCompletesWithFailure()
    {
        InvalidOperationException innerFailure = new("flat-map-inner");
        FlatMapRecordingObserver<int> innerObserver = new();
        FlatMapCoordinator<int> innerCoordinator = new(innerObserver);
        DirectSource<int> inner = new();
        await innerCoordinator.SubscribeInnerAsync(inner);
        await inner.Complete(Result.Failure(innerFailure));

        await Assert.That(innerObserver.Completions[0].IsFailure).IsTrue();
        await Assert.That(innerObserver.Completions[0].Exception).IsSameReferenceAs(innerFailure);

        InvalidOperationException subscribeFailure = new("flat-map-subscribe");
        FlatMapRecordingObserver<int> subscribeObserver = new();
        FlatMapCoordinator<int> subscribeCoordinator = new(subscribeObserver);
        await subscribeCoordinator.SubscribeInnerAsync(new ThrowingSubscribeSignal(subscribeFailure));

        await Assert.That(subscribeObserver.Completions[0].IsFailure).IsTrue();
        await Assert.That(subscribeObserver.Completions[0].Exception).IsSameReferenceAs(subscribeFailure);
    }

    /// <summary>Verifies external cancellation links are reflected in inner subscription tokens.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLinkingExternalCancellation_ThenInnerSubscriptionsSeeCancellation()
    {
        using CancellationTokenSource alreadyCanceled = new();
        await alreadyCanceled.CancelAsync();
        FlatMapCoordinator<int> canceledCoordinator = new(new FlatMapRecordingObserver<int>());
        TokenRecordingSignal canceledSignal = new();
        canceledCoordinator.LinkExternalCancellation(alreadyCanceled.Token);
        await canceledCoordinator.SubscribeInnerAsync(canceledSignal);
        await Assert.That(canceledSignal.SubscribeCount).IsEqualTo(One);
        await Assert.That(canceledSignal.Token.IsCancellationRequested).IsTrue();
        await canceledCoordinator.DisposeAsync();

        using CancellationTokenSource external = new();
        FlatMapCoordinator<int> linkedCoordinator = new(new FlatMapRecordingObserver<int>());
        TokenRecordingSignal linkedSignal = new();
        linkedCoordinator.LinkExternalCancellation(external.Token);
        await external.CancelAsync();
        await linkedCoordinator.SubscribeInnerAsync(linkedSignal);
        await Assert.That(linkedSignal.SubscribeCount).IsEqualTo(One);
        await Assert.That(linkedSignal.Token.IsCancellationRequested).IsTrue();
        await linkedCoordinator.DisposeAsync();

        FlatMapCoordinator<int> uncancelableCoordinator = new(new FlatMapRecordingObserver<int>());
        TokenRecordingSignal uncancelableSignal = new();
        uncancelableCoordinator.LinkExternalCancellation(CancellationToken.None);
        await uncancelableCoordinator.SubscribeInnerAsync(uncancelableSignal);
        await Assert.That(uncancelableSignal.Token.IsCancellationRequested).IsFalse();
        await uncancelableCoordinator.DisposeAsync();
    }

    /// <summary>Verifies an outer failure terminates immediately while inner subscriptions are still active.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOuterFailsWithActiveInner_ThenCompletesImmediately()
    {
        InvalidOperationException expected = new("flat-map-outer");
        FlatMapRecordingObserver<int> observer = new();
        FlatMapCoordinator<int> coordinator = new(observer);
        await coordinator.SubscribeInnerAsync(new DirectSource<int>());
        await coordinator.CompleteOuterAsync(Result.Failure(expected));

        await Assert.That(observer.Completions[0].IsFailure).IsTrue();
        await Assert.That(observer.Completions[0].Exception).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies disposed flat-map coordinators ignore new inners and route late failures.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAlreadyDisposed_ThenIgnoresInnerAndRoutesLateFailure()
    {
        InvalidOperationException expected = new("flat-map-late");
        using UnhandledExceptionCapture capture = new();
        FlatMapRecordingObserver<int> observer = new();
        FlatMapCoordinator<int> coordinator = new(observer);
        await coordinator.DisposeAsync();

        TokenRecordingSignal ignored = new();
        await coordinator.SubscribeInnerAsync(ignored);
        await coordinator.FinishAsync(Result.Success);
        await coordinator.FinishAsync(Result.Failure(expected));

        var captured = await capture.WaitForAsync(ex => ReferenceEquals(ex, expected), TimeSpan.FromSeconds(One));
        await Assert.That(ignored.SubscribeCount).IsEqualTo(0);
        await Assert.That(observer.Completions.Count).IsEqualTo(0);
        await Assert.That(captured).IsSameReferenceAs(expected);
    }

    /// <summary>Records flat-map coordinator observer callbacks.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class FlatMapRecordingObserver<T> : IObserverAsync<T>
    {
        /// <summary>Gets observed values.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets observed error-resume notifications.</summary>
        public List<Exception> Errors { get; } = [];

        /// <summary>Gets terminal completion results.</summary>
        public List<Result> Completions { get; } = [];

        /// <inheritdoc/>
        public ValueTask OnNextAsync(T value, CancellationToken cancellationToken)
        {
            Values.Add(value);
            return default;
        }

        /// <inheritdoc/>
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken)
        {
            Errors.Add(error);
            return default;
        }

        /// <inheritdoc/>
        public ValueTask OnCompletedAsync(Result result)
        {
            Completions.Add(result);
            return default;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => default;
    }

    /// <summary>Async signal that fails during subscription.</summary>
    /// <param name="error">The subscription failure.</param>
    private sealed class ThrowingSubscribeSignal(Exception error) : IObservableAsync<int>
    {
        /// <inheritdoc/>
        public ValueTask<IAsyncDisposable> SubscribeAsync(
            IObserverAsync<int> observer,
            CancellationToken cancellationToken)
        {
            _ = observer;
            _ = cancellationToken;
            return ValueTask.FromException<IAsyncDisposable>(error);
        }
    }

    /// <summary>Async signal that records subscription tokens without emitting values.</summary>
    private sealed class TokenRecordingSignal : IObservableAsync<int>
    {
        /// <summary>Gets the number of subscribe calls.</summary>
        public int SubscribeCount { get; private set; }

        /// <summary>Gets the cancellation token supplied to the most recent subscription.</summary>
        public CancellationToken Token { get; private set; }

        /// <inheritdoc/>
        public ValueTask<IAsyncDisposable> SubscribeAsync(
            IObserverAsync<int> observer,
            CancellationToken cancellationToken)
        {
            _ = observer;
            SubscribeCount++;
            Token = cancellationToken;
            return new(DisposableAsync.Empty);
        }
    }
}
