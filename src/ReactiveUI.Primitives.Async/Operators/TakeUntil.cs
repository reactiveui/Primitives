// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides a set of extension methods for creating observable sequences that emit items from a source sequence until a
/// specified condition is met or an external signal is received.
/// </summary>
/// <remarks>The methods in this class allow you to control the lifetime of an observable sequence based on
/// various triggers, such as another observable, a task, a cancellation token, or a predicate. These methods are useful
/// for scenarios where you need to automatically stop processing items from a source sequence when a certain event
/// occurs or a condition is satisfied.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Take-until operators that emit items from an observable source until a stop condition is met.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>
        /// Returns an observable sequence that emits items from the source sequence until the specified other
        /// observable emits an item or completes.
        /// </summary>
        /// <typeparam name="TOther">The type of the elements in the other observable sequence that triggers termination of the source sequence.</typeparam>
        /// <param name="other">The observable sequence whose first emission or completion will cause the returned sequence to stop emitting
        /// items from the source.</param>
        /// <returns>An observable sequence that emits items from the source sequence until the other observable emits an item or
        /// completes.</returns>
        /// <exception cref="ArgumentNullException">Thrown if either the source sequence or the other observable is null.</exception>
        public IObservableAsync<T> TakeUntil<TOther>(IObservableAsync<TOther> other) =>
            source.TakeUntil(other, null, CancellationToken.None);

        /// <summary>
        /// Returns an observable sequence that emits items from the source sequence until the specified other
        /// observable emits an item or completes.
        /// </summary>
        /// <typeparam name="TOther">The type of the elements in the other observable sequence that triggers termination of the source sequence.</typeparam>
        /// <param name="other">The observable sequence whose first emission or completion will cause the returned sequence to stop emitting
        /// items from the source.</param>
        /// <param name="options">An optional set of options that control the behavior of the take-until operation. If null, default options
        /// are used.</param>
        /// <returns>An observable sequence that emits items from the source sequence until the other observable emits an item or
        /// completes.</returns>
        /// <exception cref="ArgumentNullException">Thrown if either the source sequence or the other observable is null.</exception>
        public IObservableAsync<T> TakeUntil<TOther>(IObservableAsync<TOther> other, TakeUntilOptions? options) =>
            source.TakeUntil(other, options, CancellationToken.None);

        /// <summary>Emits source items until <paramref name="other"/> or <paramref name="cancellationToken"/> fires.</summary>
        /// <typeparam name="TOther">Element type of the other sequence.</typeparam>
        /// <param name="other">The observable sequence whose first emission or completion terminates the result.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        public IObservableAsync<T> TakeUntil<TOther>(IObservableAsync<TOther> other, CancellationToken cancellationToken) =>
            source.TakeUntil(other, null, cancellationToken);

        /// <summary>Emits source items until <paramref name="other"/> or <paramref name="cancellationToken"/> fires.</summary>
        /// <typeparam name="TOther">Element type of the other sequence.</typeparam>
        /// <param name="other">The observable sequence whose first emission or completion terminates the result.</param>
        /// <param name="options">Options controlling the take-until behavior, or null for defaults.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        public IObservableAsync<T> TakeUntil<TOther>(
            IObservableAsync<TOther> other,
            TakeUntilOptions? options,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(other);

            TakeUntilAsyncSignal<T, TOther> inner = new(source, other, options ?? TakeUntilOptions.Default);
            return cancellationToken.CanBeCanceled ? inner.TakeUntil(cancellationToken) : inner;
        }

        /// <summary>Returns an observable sequence that emits items from the source until the specified task completes.</summary>
        /// <param name="task">The task whose completion will signal the termination of the observable sequence. The sequence will stop
        /// emitting items when this task completes, regardless of its result.</param>
        /// <returns>An observable sequence that emits items from the source until the specified task completes.</returns>
        /// <exception cref="ArgumentNullException">Thrown if the source observable is null.</exception>
        public IObservableAsync<T> TakeUntil(Task task) =>
            source.TakeUntil(task, null, CancellationToken.None);

        /// <summary>Returns an observable sequence that emits items from the source until the specified task completes.</summary>
        /// <param name="task">The task whose completion will signal the termination of the observable sequence. The sequence will stop
        /// emitting items when this task completes, regardless of its result.</param>
        /// <param name="options">An optional set of options that control the behavior of the take-until operation. If null, default options
        /// are used.</param>
        /// <returns>An observable sequence that emits items from the source until the specified task completes.</returns>
        /// <exception cref="ArgumentNullException">Thrown if the source observable is null.</exception>
        public IObservableAsync<T> TakeUntil(Task task, TakeUntilOptions? options) =>
            source.TakeUntil(task, options, CancellationToken.None);

        /// <summary>
        /// Returns an observable sequence that emits items from the source until <paramref name="task"/> completes or
        /// <paramref name="cancellationToken"/> is cancelled — whichever comes first.
        /// </summary>
        /// <param name="task">The task whose completion terminates the result.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        public IObservableAsync<T> TakeUntil(Task task, CancellationToken cancellationToken) =>
            source.TakeUntil(task, null, cancellationToken);

        /// <summary>
        /// Returns an observable sequence that emits items from the source until <paramref name="task"/> completes or
        /// <paramref name="cancellationToken"/> is cancelled — whichever comes first.
        /// </summary>
        /// <param name="task">The task whose completion terminates the result.</param>
        /// <param name="options">Options controlling the take-until behavior, or null for defaults.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        public IObservableAsync<T> TakeUntil(
            Task task,
            TakeUntilOptions? options,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            TaskStopSignal<T> inner = new(source, task, options ?? TakeUntilOptions.Default);
            return cancellationToken.CanBeCanceled ? inner.TakeUntil(cancellationToken) : inner;
        }

        /// <summary>
        /// Returns an observable sequence that emits items from the source sequence until the specified cancellation
        /// token is canceled.
        /// </summary>
        /// <remarks>If the cancellation token is already canceled when the method is called, the
        /// resulting observable sequence will complete immediately.</remarks>
        /// <param name="cancellationToken">A cancellation token that, when canceled, will terminate the resulting observable sequence.</param>
        /// <returns>An observable sequence that completes when the provided cancellation token is canceled or when the source
        /// sequence completes.</returns>
        public IObservableAsync<T> TakeUntil(CancellationToken cancellationToken) =>
            new CancellationStopSignal<T>(source, cancellationToken);

        /// <summary>Returns a sequence that emits elements from the source until the specified predicate returns true for an element.</summary>
        /// <remarks>The element that causes the predicate to return true is not included in the resulting
        /// sequence. Subsequent elements from the source are not emitted.</remarks>
        /// <param name="predicate">A function to test each element for a condition. The sequence will stop emitting elements when this function
        /// returns true.</param>
        /// <returns>An observable sequence that contains the elements from the source sequence up to, but not including, the
        /// first element for which the predicate returns true.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="predicate"/> is null.</exception>
        public IObservableAsync<T> TakeUntil(Func<T, bool> predicate) =>
            source.TakeUntil(predicate, CancellationToken.None);

        /// <summary>
        /// Returns an observable sequence that emits items from the source until <paramref name="predicate"/> returns
        /// <see langword="true"/> for an element or <paramref name="cancellationToken"/> is cancelled — whichever
        /// comes first.
        /// </summary>
        /// <param name="predicate">A predicate evaluated for each element; first true terminates the sequence.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        public IObservableAsync<T> TakeUntil(Func<T, bool> predicate, CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(predicate);

            PredicateStopSignal<T> inner = new(source, predicate);
            return cancellationToken.CanBeCanceled ? inner.TakeUntil(cancellationToken) : inner;
        }

        /// <summary>
        /// Returns an observable sequence that emits elements from the source sequence until the specified asynchronous
        /// predicate returns true for an element.
        /// </summary>
        /// <param name="asyncPredicate">A function that evaluates each element and its associated cancellation token asynchronously. The sequence
        /// stops emitting elements when this function returns true.</param>
        /// <returns>An observable sequence that contains the elements from the source sequence up to, but not including, the
        /// first element for which the asynchronous predicate returns true.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="asyncPredicate"/> is null.</exception>
        public IObservableAsync<T> TakeUntil(Func<T, CancellationToken, ValueTask<bool>> asyncPredicate) =>
            source.TakeUntil(asyncPredicate, CancellationToken.None);

        /// <summary>
        /// Returns an observable sequence that emits items from the source until <paramref name="asyncPredicate"/>
        /// returns <see langword="true"/> for an element or <paramref name="cancellationToken"/> is cancelled —
        /// whichever comes first.
        /// </summary>
        /// <param name="asyncPredicate">An async predicate evaluated for each element; first true terminates the sequence.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        public IObservableAsync<T> TakeUntil(
            Func<T, CancellationToken, ValueTask<bool>> asyncPredicate,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(asyncPredicate);

            AsyncPredicateStopSignal<T> inner = new(source, asyncPredicate);
            return cancellationToken.CanBeCanceled ? inner.TakeUntil(cancellationToken) : inner;
        }

        /// <summary>Returns an observable sequence that emits items from the source sequence until the specified stop signal completes.</summary>
        /// <param name="stopSignal">A delegate that provides a completion signal. The returned observable will stop emitting items when this
        /// signal completes.</param>
        /// <returns>An observable sequence that emits items from the source until the stop signal completes.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="stopSignal"/> is null.</exception>
        public IObservableAsync<T> TakeUntil(CompletionSignalDelegate stopSignal) =>
            source.TakeUntil(stopSignal, null, CancellationToken.None);

        /// <summary>Returns an observable sequence that emits items from the source sequence until the specified stop signal completes.</summary>
        /// <param name="stopSignal">A delegate that provides a completion signal. The returned observable will stop emitting items when this
        /// signal completes.</param>
        /// <param name="options">An optional set of options that configure the behavior of the take-until operation. If null, default options
        /// are used.</param>
        /// <returns>An observable sequence that emits items from the source until the stop signal completes.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="stopSignal"/> is null.</exception>
        public IObservableAsync<T> TakeUntil(
            CompletionSignalDelegate stopSignal,
            TakeUntilOptions? options) =>
            source.TakeUntil(stopSignal, options, CancellationToken.None);

        /// <summary>Emits source items until <paramref name="stopSignal"/> or <paramref name="cancellationToken"/> fires.</summary>
        /// <param name="stopSignal">A delegate that provides the completion stop signal.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        public IObservableAsync<T> TakeUntil(
            CompletionSignalDelegate stopSignal,
            CancellationToken cancellationToken) =>
            source.TakeUntil(stopSignal, null, cancellationToken);

        /// <summary>Emits source items until <paramref name="stopSignal"/> or <paramref name="cancellationToken"/> fires.</summary>
        /// <param name="stopSignal">A delegate that provides the completion stop signal.</param>
        /// <param name="options">Options controlling the take-until behavior, or null for defaults.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        public IObservableAsync<T> TakeUntil(
            CompletionSignalDelegate stopSignal,
            TakeUntilOptions? options,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(stopSignal);

            DelegateStopSignal<T> inner = new(source, stopSignal, options ?? TakeUntilOptions.Default);
            return cancellationToken.CanBeCanceled ? inner.TakeUntil(cancellationToken) : inner;
        }
    }

    /// <summary>Async observable that emits items from the source until the specified predicate returns true.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">The predicate that signals when to stop emitting items.</param>
    internal sealed class PredicateStopSignal<T>(IObservableAsync<T> source, Func<T, bool> predicate)
        : SignalAsync<T>
    {
        /// <summary>The predicate that signals when to stop emitting items.</summary>
        private readonly Func<T, bool> _predicate = predicate;

        /// <summary>The source observable sequence.</summary>
        private readonly IObservableAsync<T> _source = source;

        /// <inheritdoc/>
        protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            PredicateStopCoordinator subscription = new(this, observer);
            return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
                subscription,
                () => subscription.SubscribeSourcesAsync(cancellationToken));
        }

        /// <summary>Observer that forwards items from the source until the predicate returns true.</summary>
        /// <param name="parent">The parent observable that owns this subscription.</param>
        /// <param name="observer">The downstream observer to forward items to.</param>
        internal sealed class PredicateStopCoordinator(PredicateStopSignal<T> parent, IObserverAsync<T> observer)
            : WitnessAsync<T>
        {
            /// <summary>The inner subscription handle.</summary>
            private IAsyncDisposable? _subscription;

            /// <summary>Subscribes to the source observable.</summary>
            /// <param name="cancellationToken">A token to cancel the subscription.</param>
            /// <returns>A task representing the asynchronous subscribe operation.</returns>
            public async ValueTask SubscribeSourcesAsync(CancellationToken cancellationToken) =>
                _subscription = await parent._source.SubscribeAsync(this, cancellationToken).ConfigureAwait(false);

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                if (parent._predicate(value))
                {
                    return OnCompletedAsyncCore(Result.Success);
                }

                return observer.OnNextAsync(value, cancellationToken);
            }

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                observer.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) => observer.OnCompletedAsync(result);

            /// <inheritdoc/>
            protected override async ValueTask DisposeAsyncCore()
            {
                if (_subscription is not null)
                {
                    await _subscription.DisposeAsync().ConfigureAwait(false);
                }

                await base.DisposeAsyncCore().ConfigureAwait(false);
            }
        }
    }

    /// <summary>Async observable that emits items from the source until the specified cancellation token is canceled.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="cancellationToken">The cancellation token that triggers completion.</param>
    internal sealed class CancellationStopSignal<T>(IObservableAsync<T> source, CancellationToken cancellationToken)
        : SignalAsync<T>
    {
        /// <summary>The source observable sequence.</summary>
        private readonly IObservableAsync<T> _source = source;

        /// <summary>The cancellation token that triggers completion.</summary>
        private readonly CancellationToken _cancellationToken = cancellationToken;

        /// <inheritdoc/>
        protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            CancellationStopCoordinator subscription = new(this, observer);
            subscription.LinkExternalCancellation(cancellationToken);
            return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
                subscription,
                () => subscription.SubscribeSourcesAsync(cancellationToken));
        }

        /// <summary>
        /// Manages the subscription lifetime and completes when the cancellation token is canceled.
        /// Composes <see cref="TakeUntilLifecycle{T}"/> for the shared gate / dispose-CTS / external-
        /// link / gated-forwarding plumbing so this class only carries the operator-specific state.
        /// </summary>
        internal sealed class CancellationStopCoordinator : IAsyncDisposable
        {
            /// <summary>The parent observable that owns this subscription.</summary>
            private readonly CancellationStopSignal<T> _parent;

            /// <summary>Shared subscription lifecycle (gate / dispose CTS / external link / forwarders).</summary>
            private readonly TakeUntilLifecycle<T> _lifecycle;

            /// <summary>The inner subscription handle.</summary>
            private IAsyncDisposable? _subscription;

            /// <summary>The registration handle for the external cancellation token callback.</summary>
            private CancellationTokenRegistration? _tokenRegistration;

            /// <summary>Initializes a new instance of the <see cref="CancellationStopCoordinator"/> class.</summary>
            /// <param name="parent">The parent observable that owns this subscription.</param>
            /// <param name="observer">The downstream observer to forward items to.</param>
            public CancellationStopCoordinator(CancellationStopSignal<T> parent, IObserverAsync<T> observer)
            {
                _parent = parent;
                _lifecycle = new(observer);
            }

            /// <summary>Subscribes to the source observable and registers the cancellation token callback.</summary>
            /// <param name="cancellationToken">A token to cancel the subscription.</param>
            /// <returns>A task representing the asynchronous subscribe operation.</returns>
            public async ValueTask SubscribeSourcesAsync(CancellationToken cancellationToken)
            {
                _tokenRegistration = _parent._cancellationToken.Register(CompleteFromCancellation);
                _subscription = await _parent._source.SubscribeAsync(new TakeUntilSourceWitness<T>(_lifecycle), cancellationToken).ConfigureAwait(false);
            }

            /// <summary>Asynchronously releases resources used by this subscription.</summary>
            /// <returns>A task representing the asynchronous dispose operation.</returns>
            public async ValueTask DisposeAsync()
            {
                if (_tokenRegistration is { } reg)
                {
#if NET8_0_OR_GREATER
                    await reg.DisposeAsync().ConfigureAwait(false);
#else
                    reg.Dispose();
#endif
                }

                if (_subscription is not null)
                {
                    await _subscription.DisposeAsync().ConfigureAwait(false);
                }

                await _lifecycle.DisposeAsync().ConfigureAwait(false);
            }

            /// <summary>Forwards the original subscribe-time token into the shared lifecycle's dispose chain.</summary>
            /// <param name="external">The subscribe-time token.</param>
            internal void LinkExternalCancellation(CancellationToken external) =>
                _lifecycle.LinkExternalCancellation(external);

            /// <summary>Callback invoked when the external cancellation token is canceled; forwards completion to the observer.</summary>
            internal void CompleteFromCancellation() => FireAndForgetHelper.Run(async () =>
            {
                await Task.Yield();
                await _lifecycle.RelayCompletionAsync(Result.Success).ConfigureAwait(false);
            });
        }
    }

    /// <summary>Async observable that emits items from the source until a raw completion signal fires.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="stopSignal">The delegate that provides the stop signal.</param>
    /// <param name="options">Options controlling the take-until behavior.</param>
    internal sealed class DelegateStopSignal<T>(
        IObservableAsync<T> source,
        CompletionSignalDelegate stopSignal,
        TakeUntilOptions options) : SignalAsync<T>
    {
        /// <summary>The source observable sequence.</summary>
        private readonly IObservableAsync<T> _source = source;

        /// <summary>The delegate that provides the stop signal.</summary>
        private readonly CompletionSignalDelegate _stopSignal = stopSignal;

        /// <summary>Options controlling the take-until behavior.</summary>
        private readonly TakeUntilOptions _options = options;

        /// <inheritdoc/>
        protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            DelegateStopCoordinator subscription = new(this, observer);
            subscription.LinkExternalCancellation(cancellationToken);
            return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
                subscription,
                () => subscription.SubscribeSourcesAsync(cancellationToken));
        }

        /// <summary>Manages the subscription lifetime and completes when the raw stop signal fires.</summary>
        internal sealed class DelegateStopCoordinator : IAsyncDisposable
        {
            /// <summary>The parent observable that owns this subscription.</summary>
            private readonly DelegateStopSignal<T> _parent;

            /// <summary>Shared subscription lifecycle (gate / dispose CTS / external link / forwarders).</summary>
            private readonly TakeUntilLifecycle<T> _lifecycle;

            /// <summary>The inner subscription handle.</summary>
            private IAsyncDisposable? _subscription;

            /// <summary>Initializes a new instance of the <see cref="DelegateStopCoordinator"/> class.</summary>
            /// <param name="parent">The parent observable that owns this subscription.</param>
            /// <param name="observer">The downstream observer to forward items to.</param>
            public DelegateStopCoordinator(DelegateStopSignal<T> parent, IObserverAsync<T> observer)
            {
                _parent = parent;
                _lifecycle = new(observer);
            }

            /// <summary>Subscribes to the source observable and begins waiting for the stop signal.</summary>
            /// <param name="cancellationToken">A token to cancel the subscription.</param>
            /// <returns>A task representing the asynchronous subscribe operation.</returns>
            public async ValueTask SubscribeSourcesAsync(CancellationToken cancellationToken)
            {
                AwaitStopThenComplete();
                _subscription = await _parent._source.SubscribeAsync(new TakeUntilSourceWitness<T>(_lifecycle), cancellationToken).ConfigureAwait(false);
            }

            /// <summary>Asynchronously releases resources used by this subscription.</summary>
            /// <returns>A task representing the asynchronous dispose operation.</returns>
            public async ValueTask DisposeAsync()
            {
                if (_subscription is not null)
                {
                    await _subscription.DisposeAsync().ConfigureAwait(false);
                }

                await _lifecycle.DisposeAsync().ConfigureAwait(false);
            }

            /// <summary>Forwards the original subscribe-time token into the shared lifecycle's dispose chain.</summary>
            /// <param name="external">The subscribe-time token.</param>
            internal void LinkExternalCancellation(CancellationToken external) =>
                _lifecycle.LinkExternalCancellation(external);

            /// <summary>Waits for the stop signal to fire, then forwards completion or error to the downstream observer.</summary>
            internal void AwaitStopThenComplete() => FireAndForgetHelper.Run(async () =>
            {
                TaskCompletionSource<object?> tcs = new();

                void Stop(Result result)
                {
                    if (result.IsFailure)
                    {
                        tcs.SetException(result.Exception);
                    }
                    else
                    {
                        tcs.SetResult(null);
                    }
                }

                var disposable = _parent._stopSignal(Stop);

                try
                {
                    await tcs.Task.WaitAsync(System.Threading.Timeout.InfiniteTimeSpan, _lifecycle.DisposeToken).ConfigureAwait(false);
                    try
                    {
                        await disposable.DisposeAsync().ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        // Best-effort: a secondary dispose failure during cleanup goes to the global handler.
                        UnhandledExceptionHandler.ReportUnhandledException(e);
                    }

                    await _lifecycle.RelayCompletionAsync(Result.Success).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    try
                    {
                        await disposable.DisposeAsync().ConfigureAwait(false);
                    }
                    catch (Exception disposeError)
                    {
                        // Best-effort: a secondary dispose failure during error handling goes to the global handler.
                        UnhandledExceptionHandler.ReportUnhandledException(disposeError);
                    }

                    if (_parent._options.SourceFailsWhenOtherFails)
                    {
                        await _lifecycle.RelayCompletionAsync(Result.Failure(e)).ConfigureAwait(false);
                    }
                    else
                    {
                        await _lifecycle.RelayErrorAsync(e).ConfigureAwait(false);
                    }
                }
            });
        }
    }

    /// <summary>Async observable that emits items from the source until the specified task completes.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="task">The task whose completion triggers the end of the sequence.</param>
    /// <param name="options">Options controlling the take-until behavior.</param>
    internal sealed class TaskStopSignal<T>(IObservableAsync<T> source, Task task, TakeUntilOptions options)
        : SignalAsync<T>
    {
        /// <summary>The source observable sequence.</summary>
        private readonly IObservableAsync<T> _source = source;

        /// <summary>The task whose completion triggers the end of the sequence.</summary>
        private readonly Task _task = task;

        /// <summary>Options controlling the take-until behavior.</summary>
        private readonly TakeUntilOptions _options = options;

        /// <inheritdoc/>
        protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            TaskStopCoordinator subscription = new(this, observer);
            subscription.LinkExternalCancellation(cancellationToken);
            return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
                subscription,
                () => subscription.SubscribeSourcesAsync(cancellationToken));
        }

        /// <summary>Manages the subscription lifetime and completes when the task finishes. Composes <see cref="TakeUntilLifecycle{T}"/> for the shared plumbing.</summary>
        internal sealed class TaskStopCoordinator : IAsyncDisposable
        {
            /// <summary>The parent observable that owns this subscription.</summary>
            private readonly TaskStopSignal<T> _parent;

            /// <summary>Shared subscription lifecycle (gate / dispose CTS / external link / forwarders).</summary>
            private readonly TakeUntilLifecycle<T> _lifecycle;

            /// <summary>The inner subscription handle.</summary>
            private IAsyncDisposable? _subscription;

            /// <summary>Initializes a new instance of the <see cref="TaskStopCoordinator"/> class.</summary>
            /// <param name="parent">The parent observable that owns this subscription.</param>
            /// <param name="observer">The downstream observer to forward items to.</param>
            public TaskStopCoordinator(TaskStopSignal<T> parent, IObserverAsync<T> observer)
            {
                _parent = parent;
                _lifecycle = new(observer);
            }

            /// <summary>Subscribes to the source observable and begins waiting for the task to complete.</summary>
            /// <param name="cancellationToken">A token to cancel the subscription.</param>
            /// <returns>A task representing the asynchronous subscribe operation.</returns>
            public async ValueTask SubscribeSourcesAsync(CancellationToken cancellationToken)
            {
                var task = _parent._task;
                AwaitStopThenComplete(task);
                _subscription = await _parent._source.SubscribeAsync(new TakeUntilSourceWitness<T>(_lifecycle), cancellationToken).ConfigureAwait(false);
            }

            /// <summary>Asynchronously releases resources used by this subscription.</summary>
            /// <returns>A task representing the asynchronous dispose operation.</returns>
            public async ValueTask DisposeAsync()
            {
                if (_subscription is not null)
                {
                    await _subscription.DisposeAsync().ConfigureAwait(false);
                }

                await _lifecycle.DisposeAsync().ConfigureAwait(false);
            }

            /// <summary>Forwards the original subscribe-time token into the shared lifecycle's dispose chain.</summary>
            /// <param name="external">The subscribe-time token.</param>
            internal void LinkExternalCancellation(CancellationToken external) =>
                _lifecycle.LinkExternalCancellation(external);

            /// <summary>Waits for the task to complete, then forwards completion or error to the downstream observer.</summary>
            /// <param name="task">The task to await.</param>
            internal void AwaitStopThenComplete(Task task) => FireAndForgetHelper.Run(async () =>
            {
                try
                {
                    await task.WaitAsync(System.Threading.Timeout.InfiniteTimeSpan, _lifecycle.DisposeToken).ConfigureAwait(false);
                    await _lifecycle.RelayCompletionAsync(Result.Success).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (_parent._options.SourceFailsWhenOtherFails)
                    {
                        await _lifecycle.RelayCompletionAsync(Result.Failure(e)).ConfigureAwait(false);
                    }
                    else
                    {
                        await _lifecycle.RelayErrorAsync(e).ConfigureAwait(false);
                    }
                }
            });
        }
    }

    /// <summary>Async observable that emits items from the source until another async observable emits or completes.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <typeparam name="TOther">The type of the elements in the signal sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="other">The signal observable whose emission triggers completion.</param>
    /// <param name="options">Options controlling the take-until behavior.</param>
    internal sealed class TakeUntilAsyncSignal<T, TOther>(
        IObservableAsync<T> source,
        IObservableAsync<TOther> other,
        TakeUntilOptions options) : SignalAsync<T>
    {
        /// <summary>The source observable sequence.</summary>
        private readonly IObservableAsync<T> _source = source;

        /// <summary>The signal observable whose emission triggers completion.</summary>
        private readonly IObservableAsync<TOther> _other = other;

        /// <summary>Options controlling the take-until behavior.</summary>
        private readonly TakeUntilOptions _options = options;

        /// <inheritdoc/>
        protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            AsyncStopCoordinator subscription = new(this, observer);
            subscription.LinkExternalCancellation(cancellationToken);
            return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
                subscription,
                async () => await subscription.SubscribeSourcesAsync(cancellationToken).ConfigureAwait(false));
        }

        /// <summary>
        /// Manages subscriptions to both the source and signal observables, completing when the signal
        /// fires. Composes <see cref="TakeUntilLifecycle{T}"/> for the shared plumbing.
        /// </summary>
        internal sealed class AsyncStopCoordinator : IAsyncDisposable
        {
            /// <summary>The parent observable that owns this subscription.</summary>
            private readonly TakeUntilAsyncSignal<T, TOther> _parent;

            /// <summary>Shared subscription lifecycle (gate / dispose CTS / external link / forwarders).</summary>
            private readonly TakeUntilLifecycle<T> _lifecycle;

            /// <summary>Holds the source subscription so it can be disposed on teardown.</summary>
            private readonly SingleAssignmentDisposableAsync _disposable = new();

            /// <summary>Holds the signal subscription so it can be disposed on teardown.</summary>
            private readonly SingleAssignmentDisposableAsync _otherDisposable = new();

            /// <summary>Initializes a new instance of the <see cref="AsyncStopCoordinator"/> class.</summary>
            /// <param name="parent">The parent observable that owns this subscription.</param>
            /// <param name="observer">The downstream observer to forward items to.</param>
            public AsyncStopCoordinator(TakeUntilAsyncSignal<T, TOther> parent, IObserverAsync<T> observer)
            {
                _parent = parent;
                _lifecycle = new(observer);
            }

            /// <summary>Subscribes to both the source and signal observables.</summary>
            /// <param name="cancellationToken">A token to cancel the subscription.</param>
            /// <returns>This subscription as an async disposable.</returns>
            public async ValueTask<IAsyncDisposable> SubscribeSourcesAsync(CancellationToken cancellationToken)
            {
                var otherSubscription = await _parent._other.SubscribeAsync(new StopSignalWitness(this), cancellationToken).ConfigureAwait(false);
                await _otherDisposable.SetDisposableAsync(otherSubscription).ConfigureAwait(false);

                var sourceSubscription =
                    await _parent._source.SubscribeAsync(new TakeUntilSourceWitness<T>(_lifecycle), cancellationToken).ConfigureAwait(false);
                await _disposable.SetDisposableAsync(sourceSubscription).ConfigureAwait(false);

                return this;
            }

            /// <summary>Asynchronously releases resources used by this subscription.</summary>
            /// <returns>A task representing the asynchronous dispose operation.</returns>
            public async ValueTask DisposeAsync()
            {
                await _otherDisposable.DisposeAsync().ConfigureAwait(false);
                await _disposable.DisposeAsync().ConfigureAwait(false);
                await _lifecycle.DisposeAsync().ConfigureAwait(false);
            }

            /// <summary>Forwards the original subscribe-time token into the shared lifecycle's dispose chain.</summary>
            /// <param name="external">The subscribe-time token.</param>
            internal void LinkExternalCancellation(CancellationToken external) =>
                _lifecycle.LinkExternalCancellation(external);

            /// <summary>Observer for the signal observable that triggers completion of the source subscription.</summary>
            /// <param name="parent">The parent coordinator that owns this witness.</param>
            internal sealed class StopSignalWitness(AsyncStopCoordinator parent) : WitnessAsync<TOther>
            {
                /// <inheritdoc/>
                protected override async ValueTask OnNextAsyncCore(TOther value, CancellationToken cancellationToken)
                {
                    _ = value;
                    _ = cancellationToken;
                    await parent._lifecycle.RelayCompletionAsync(Result.Success).ConfigureAwait(false);
                    await DisposeAsync().ConfigureAwait(false);
                }

                /// <inheritdoc/>
                protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
                {
                    _ = cancellationToken;
                    return parent._lifecycle.RelayErrorAsync(error);
                }

                /// <inheritdoc/>
                protected override ValueTask OnCompletedAsyncCore(Result result)
                {
                    if (!result.IsFailure)
                    {
                        return default;
                    }

                    if (parent._parent._options.SourceFailsWhenOtherFails)
                    {
                        return parent._lifecycle.RelayCompletionAsync(result);
                    }

                    return parent._lifecycle.RelayCompletionAsync(Result.Success);
                }
            }
        }
    }

    /// <summary>Emits source items until an async predicate returns true.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="asyncPredicate">Predicate that signals when to stop.</param>
    internal sealed class AsyncPredicateStopSignal<T>(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask<bool>> asyncPredicate) : SignalAsync<T>
    {
        /// <summary>The async predicate that signals when to stop emitting items.</summary>
        private readonly Func<T, CancellationToken, ValueTask<bool>> _asyncPredicate = asyncPredicate;

        /// <summary>The source observable sequence.</summary>
        private readonly IObservableAsync<T> _source = source;

        /// <inheritdoc/>
        protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            AsyncPredicateStopCoordinator subscription = new(this, observer);
            return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
                subscription,
                () => subscription.SubscribeSourcesAsync(cancellationToken));
        }

        /// <summary>Forwards source items until the async predicate returns true.</summary>
        /// <param name="parent">The owning signal.</param>
        /// <param name="observer">The downstream observer.</param>
        internal sealed class AsyncPredicateStopCoordinator(
            AsyncPredicateStopSignal<T> parent,
            IObserverAsync<T> observer) : WitnessAsync<T>
        {
            /// <summary>The inner subscription handle.</summary>
            private IAsyncDisposable? _subscription;

            /// <summary>Subscribes to the source observable.</summary>
            /// <param name="cancellationToken">A token to cancel the subscription.</param>
            /// <returns>A task representing the asynchronous subscribe operation.</returns>
            public async ValueTask SubscribeSourcesAsync(CancellationToken cancellationToken) =>
                _subscription = await parent._source.SubscribeAsync(this, cancellationToken).ConfigureAwait(false);

            /// <inheritdoc/>
            protected override async ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                if (await parent._asyncPredicate(value, cancellationToken).ConfigureAwait(false))
                {
                    await OnCompletedAsyncCore(Result.Success).ConfigureAwait(false);
                    return;
                }

                await observer.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
            }

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                observer.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) => observer.OnCompletedAsync(result);

            /// <inheritdoc/>
            protected override async ValueTask DisposeAsyncCore()
            {
                if (_subscription is not null)
                {
                    await _subscription.DisposeAsync().ConfigureAwait(false);
                }

                await base.DisposeAsyncCore().ConfigureAwait(false);
            }
        }
    }
}
