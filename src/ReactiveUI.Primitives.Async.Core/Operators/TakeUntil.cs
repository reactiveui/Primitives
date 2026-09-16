// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides a set of extension methods for creating observable sequences that emit items from a source sequence until a
/// specified condition is met or an external signal is received.
/// </summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Take-until operators that emit items from an observable source until a stop condition is met.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Returns an observable sequence that emits items from the source sequence until the specified other observable emits an item or completes.</summary>
        /// <typeparam name="TOther">The type of the elements in the other observable sequence that triggers termination of the source sequence.</typeparam>
        /// <param name="other">The observable sequence whose first emission or completion will cause the returned sequence to stop emitting
        /// items from the source.</param>
        /// <returns>An observable sequence that emits items from the source sequence until the other observable emits an item or
        /// completes.</returns>
        /// <exception cref="ArgumentNullException">Thrown if either the source sequence or the other observable is null.</exception>
        public IObservableAsync<T> TakeUntil<TOther>(IObservableAsync<TOther> other)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(other);

            return new TakeUntilAsyncSignal<T, TOther>(source, other, TakeUntilOptions.Default);
        }

        /// <summary>Returns an observable sequence that emits items from the source sequence until the specified other observable emits an item or completes.</summary>
        /// <typeparam name="TOther">The type of the elements in the other observable sequence that triggers termination of the source sequence.</typeparam>
        /// <param name="other">The observable sequence whose first emission or completion will cause the returned sequence to stop emitting
        /// items from the source.</param>
        /// <param name="options">An optional set of options that control the behavior of the take-until operation. If null, default options
        /// are used.</param>
        /// <returns>An observable sequence that emits items from the source sequence until the other observable emits an item or
        /// completes.</returns>
        /// <exception cref="ArgumentNullException">Thrown if either the source sequence or the other observable is null.</exception>
        public IObservableAsync<T> TakeUntil<TOther>(IObservableAsync<TOther> other, TakeUntilOptions? options)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(other);

            return new TakeUntilAsyncSignal<T, TOther>(source, other, options ?? TakeUntilOptions.Default);
        }

        /// <summary>Emits source items until <paramref name="other"/> or <paramref name="cancellationToken"/> fires.</summary>
        /// <typeparam name="TOther">Element type of the other sequence.</typeparam>
        /// <param name="other">The observable sequence whose first emission or completion terminates the result.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        /// <exception cref="ArgumentNullException">Thrown if either the source sequence or the other observable is null.</exception>
        public IObservableAsync<T> TakeUntil<TOther>(
            IObservableAsync<TOther> other,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(other);

            return new TakeUntilAsyncSignal<T, TOther>(source, other, TakeUntilOptions.Default, cancellationToken);
        }

        /// <summary>Emits source items until <paramref name="other"/> or <paramref name="cancellationToken"/> fires.</summary>
        /// <typeparam name="TOther">Element type of the other sequence.</typeparam>
        /// <param name="other">The observable sequence whose first emission or completion terminates the result.</param>
        /// <param name="options">Options controlling the take-until behavior, or null for defaults.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        /// <exception cref="ArgumentNullException">Thrown if either the source sequence or the other observable is null.</exception>
        public IObservableAsync<T> TakeUntil<TOther>(
            IObservableAsync<TOther> other,
            TakeUntilOptions? options,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(other);

            return new TakeUntilAsyncSignal<T, TOther>(source, other, options ?? TakeUntilOptions.Default, cancellationToken);
        }

        /// <summary>Returns an observable sequence that emits items from the source until the specified task completes.</summary>
        /// <param name="task">The task whose completion will signal the termination of the observable sequence. The sequence will stop
        /// emitting items when this task completes, regardless of its result.</param>
        /// <returns>An observable sequence that emits items from the source until the specified task completes.</returns>
        /// <exception cref="ArgumentNullException">Thrown if the source observable is null.</exception>
        public IObservableAsync<T> TakeUntil(Task task)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new TaskStopSignal<T>(source, task, TakeUntilOptions.Default);
        }

        /// <summary>Returns an observable sequence that emits items from the source until the specified task completes.</summary>
        /// <param name="task">The task whose completion will signal the termination of the observable sequence. The sequence will stop
        /// emitting items when this task completes, regardless of its result.</param>
        /// <param name="options">An optional set of options that control the behavior of the take-until operation. If null, default options
        /// are used.</param>
        /// <returns>An observable sequence that emits items from the source until the specified task completes.</returns>
        /// <exception cref="ArgumentNullException">Thrown if the source observable is null.</exception>
        public IObservableAsync<T> TakeUntil(Task task, TakeUntilOptions? options)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new TaskStopSignal<T>(source, task, options ?? TakeUntilOptions.Default);
        }

        /// <summary>
        /// Returns an observable sequence that emits items from the source until <paramref name="task"/> completes or
        /// <paramref name="cancellationToken"/> is cancelled - whichever comes first.
        /// </summary>
        /// <param name="task">The task whose completion terminates the result.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        /// <exception cref="ArgumentNullException">Thrown if the source observable is null.</exception>
        public IObservableAsync<T> TakeUntil(Task task, CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new TaskStopSignal<T>(source, task, TakeUntilOptions.Default, cancellationToken);
        }

        /// <summary>
        /// Returns an observable sequence that emits items from the source until <paramref name="task"/> completes or
        /// <paramref name="cancellationToken"/> is cancelled - whichever comes first.
        /// </summary>
        /// <param name="task">The task whose completion terminates the result.</param>
        /// <param name="options">Options controlling the take-until behavior, or null for defaults.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        /// <exception cref="ArgumentNullException">Thrown if the source observable is null.</exception>
        public IObservableAsync<T> TakeUntil(
            Task task,
            TakeUntilOptions? options,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new TaskStopSignal<T>(source, task, options ?? TakeUntilOptions.Default, cancellationToken);
        }

        /// <summary>Returns an observable sequence that emits items from the source sequence until the specified cancellation token is canceled.</summary>
        /// <param name="cancellationToken">A cancellation token that, when canceled, will terminate the resulting observable sequence.</param>
        /// <returns>An observable sequence that completes when the provided cancellation token is canceled or when the source
        /// sequence completes.</returns>
        /// <remarks>A token that is canceled at the time of the call completes the sequence immediately.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> TakeUntil(CancellationToken cancellationToken) =>
            new CancellationStopSignal<T>(source, cancellationToken);

        /// <summary>Returns a sequence that emits elements from the source until the specified predicate returns true for an element.</summary>
        /// <param name="predicate">A function to test each element for a condition. The sequence stops after the first
        /// element for which this function returns true.</param>
        /// <returns>An observable sequence that contains the elements from the source sequence up to and including the
        /// first element for which the predicate returns true.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="predicate"/> is null.</exception>
        public IObservableAsync<T> TakeUntil(Func<T, bool> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(predicate);

            return new PredicateStopSignal<T>(source, predicate);
        }

        /// <summary>
        /// Returns an observable sequence that emits items from the source until <paramref name="predicate"/> returns
        /// <see langword="true"/> for an element or <paramref name="cancellationToken"/> is cancelled - whichever
        /// comes first.
        /// </summary>
        /// <param name="predicate">A predicate evaluated for each element; first true terminates the sequence.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="predicate"/> is null.</exception>
        public IObservableAsync<T> TakeUntil(Func<T, bool> predicate, CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(predicate);

            return new PredicateStopSignal<T>(source, predicate, cancellationToken);
        }

        /// <summary>Returns an observable sequence that emits elements from the source sequence until the specified asynchronous predicate returns true for an element.</summary>
        /// <param name="asyncPredicate">A function that evaluates each element and its associated cancellation token asynchronously. The sequence
        /// stops after the first element for which this function returns true.</param>
        /// <returns>An observable sequence that contains the elements from the source sequence up to and including the
        /// first element for which the asynchronous predicate returns true.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="asyncPredicate"/> is null.</exception>
        public IObservableAsync<T> TakeUntil(Func<T, CancellationToken, ValueTask<bool>> asyncPredicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(asyncPredicate);

            return new AsyncPredicateStopSignal<T>(source, asyncPredicate);
        }

        /// <summary>
        /// Returns an observable sequence that emits items from the source until <paramref name="asyncPredicate"/>
        /// returns <see langword="true"/> for an element or <paramref name="cancellationToken"/> is cancelled -
        /// whichever comes first.
        /// </summary>
        /// <param name="asyncPredicate">An async predicate evaluated for each element; first true terminates the sequence.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="asyncPredicate"/> is null.</exception>
        public IObservableAsync<T> TakeUntil(
            Func<T, CancellationToken, ValueTask<bool>> asyncPredicate,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(asyncPredicate);

            return new AsyncPredicateStopSignal<T>(source, asyncPredicate, cancellationToken);
        }

        /// <summary>Returns an observable sequence that emits items from the source sequence until the specified stop signal completes.</summary>
        /// <param name="stopSignal">A delegate that provides a completion signal. The returned observable will stop emitting items when this
        /// signal completes.</param>
        /// <returns>An observable sequence that emits items from the source until the stop signal completes.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="stopSignal"/> is null.</exception>
        public IObservableAsync<T> TakeUntil(CompletionSignalDelegate stopSignal)
        {
            ArgumentExceptionHelper.ThrowIfNull(stopSignal);

            return new DelegateStopSignal<T>(source, stopSignal, TakeUntilOptions.Default);
        }

        /// <summary>Returns an observable sequence that emits items from the source sequence until the specified stop signal completes.</summary>
        /// <param name="stopSignal">A delegate that provides a completion signal. The returned observable will stop emitting items when this
        /// signal completes.</param>
        /// <param name="options">An optional set of options that configure the behavior of the take-until operation. If null, default options
        /// are used.</param>
        /// <returns>An observable sequence that emits items from the source until the stop signal completes.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="stopSignal"/> is null.</exception>
        public IObservableAsync<T> TakeUntil(
            CompletionSignalDelegate stopSignal,
            TakeUntilOptions? options)
        {
            ArgumentExceptionHelper.ThrowIfNull(stopSignal);

            return new DelegateStopSignal<T>(source, stopSignal, options ?? TakeUntilOptions.Default);
        }

        /// <summary>Emits source items until <paramref name="stopSignal"/> or <paramref name="cancellationToken"/> fires.</summary>
        /// <param name="stopSignal">A delegate that provides the completion stop signal.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="stopSignal"/> is null.</exception>
        public IObservableAsync<T> TakeUntil(
            CompletionSignalDelegate stopSignal,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(stopSignal);

            return new DelegateStopSignal<T>(source, stopSignal, TakeUntilOptions.Default, cancellationToken);
        }

        /// <summary>Emits source items until <paramref name="stopSignal"/> or <paramref name="cancellationToken"/> fires.</summary>
        /// <param name="stopSignal">A delegate that provides the completion stop signal.</param>
        /// <param name="options">Options controlling the take-until behavior, or null for defaults.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="stopSignal"/> is null.</exception>
        public IObservableAsync<T> TakeUntil(
            CompletionSignalDelegate stopSignal,
            TakeUntilOptions? options,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(stopSignal);

            return new DelegateStopSignal<T>(source, stopSignal, options ?? TakeUntilOptions.Default, cancellationToken);
        }
    }

    /// <summary>Async observable that emits items from the source until the specified cancellation token is canceled.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="cancellationToken">The cancellation token that triggers completion.</param>
    internal sealed class CancellationStopSignal<T>(IObservableAsync<T> source, CancellationToken cancellationToken) : IObservableAsync<T>
    {
        /// <summary>The source observable sequence.</summary>
        private readonly IObservableAsync<T> _source = source;

        /// <summary>The cancellation token that triggers completion.</summary>
        private readonly CancellationToken _cancellationToken = cancellationToken;

        /// <inheritdoc/>
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            CancellationStopCoordinator subscription = new(this, observer);
            subscription.LinkExternalCancellation(cancellationToken);
            return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
                subscription,
                () => subscription.SubscribeSourcesAsync(cancellationToken));
        }

        /// <summary>Manages the subscription lifetime and completes the sequence when the cancellation token is canceled.</summary>
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

            /// <summary>Subscribes to the source observable and registers the cancellation token callback.</summary>
            /// <param name="cancellationToken">A token to cancel the subscription.</param>
            /// <returns>A task representing the asynchronous subscribe operation.</returns>
            internal async ValueTask SubscribeSourcesAsync(CancellationToken cancellationToken)
            {
                _tokenRegistration = _parent._cancellationToken.Register(CompleteFromCancellation);
                _subscription = await _parent._source
                    .SubscribeAsync(new TakeUntilSourceWitness<T>(_lifecycle), cancellationToken).ConfigureAwait(false);
            }

            /// <summary>Forwards the original subscribe-time token into the shared lifecycle's dispose chain.</summary>
            /// <param name="external">The subscribe-time token.</param>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void LinkExternalCancellation(CancellationToken external) =>
                _lifecycle.LinkExternalCancellation(external);

            /// <summary>Callback invoked when the external cancellation token is canceled; forwards completion to the observer.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void CompleteFromCancellation() => FireAndForgetHelper.Run(CompleteAfterYieldAsync);

            /// <summary>Forwards successful completion through the notification gate.</summary>
            /// <returns>The completion notification.</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal ValueTask CompleteFromCancellationAsync() => _lifecycle.RelayCompletionAsync(Result.Success);

            /// <summary>Defers completion until after the cancellation callback returns.</summary>
            /// <returns>The deferred completion notification.</returns>
            [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
            private async ValueTask CompleteAfterYieldAsync()
            {
                await Task.Yield();
                await CompleteFromCancellationAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>Async observable that emits items from the source until a raw completion signal fires.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="stopSignal">The delegate that provides the stop signal.</param>
    /// <param name="options">Options controlling the take-until behavior.</param>
    /// <param name="stopToken">A token whose cancellation also completes the sequence.</param>
    internal sealed class DelegateStopSignal<T>(
        IObservableAsync<T> source,
        CompletionSignalDelegate stopSignal,
        TakeUntilOptions options,
        CancellationToken stopToken = default) : IObservableAsync<T>
    {
        /// <summary>The source observable sequence.</summary>
        private readonly IObservableAsync<T> _source = source;

        /// <summary>The delegate that provides the stop signal.</summary>
        private readonly CompletionSignalDelegate _stopSignal = stopSignal;

        /// <summary>Options controlling the take-until behavior.</summary>
        private readonly TakeUntilOptions _options = options;

        /// <summary>A token whose cancellation also completes the sequence.</summary>
        private readonly CancellationToken _stopToken = stopToken;

        /// <inheritdoc/>
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
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

            /// <summary>The handle the stop delegate returned, disposed once the signal has been dealt with.</summary>
            private IAsyncDisposable? _stopRegistration;

            /// <summary>Set once the stop signal has fired, so a second notification is ignored.</summary>
            private int _stopSignalled;

            /// <summary>Set once <see cref="_stopRegistration"/> has been disposed, so it is disposed once.</summary>
            private int _stopRegistrationDisposed;

            /// <summary>The registration that completes this sequence when the stop token fires.</summary>
            private CancellationTokenRegistration _stopTokenRegistration;

            /// <summary>Initializes a new instance of the <see cref="DelegateStopCoordinator"/> class.</summary>
            /// <param name="parent">The parent observable that owns this subscription.</param>
            /// <param name="observer">The downstream observer to forward items to.</param>
            public DelegateStopCoordinator(DelegateStopSignal<T> parent, IObserverAsync<T> observer)
            {
                _parent = parent;
                _lifecycle = new(observer);
            }

            /// <summary>Asynchronously releases resources used by this subscription.</summary>
            /// <returns>A task representing the asynchronous dispose operation.</returns>
            public async ValueTask DisposeAsync()
            {
#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
                await _stopTokenRegistration.DisposeAsync().ConfigureAwait(false);
#else
                _stopTokenRegistration.Dispose();
#endif

                if (_subscription is not null)
                {
                    await _subscription.DisposeAsync().ConfigureAwait(false);
                }

                await DisposeStopRegistrationAsync().ConfigureAwait(false);
                await _lifecycle.DisposeAsync().ConfigureAwait(false);
            }

            /// <summary>Subscribes to the source observable and begins waiting for the stop signal.</summary>
            /// <param name="cancellationToken">A token to cancel the subscription.</param>
            /// <returns>A task representing the asynchronous subscribe operation.</returns>
            internal async ValueTask SubscribeSourcesAsync(CancellationToken cancellationToken)
            {
                _stopTokenRegistration = _lifecycle.CompleteWhenCancelled(_parent._stopToken);
                AwaitStopThenComplete();
                _subscription = await _parent._source
                    .SubscribeAsync(new TakeUntilSourceWitness<T>(_lifecycle), cancellationToken).ConfigureAwait(false);
            }

            /// <summary>Forwards the original subscribe-time token into the shared lifecycle's dispose chain.</summary>
            /// <param name="external">The subscribe-time token.</param>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void LinkExternalCancellation(CancellationToken external) =>
                _lifecycle.LinkExternalCancellation(external);

            /// <summary>Hands the stop callback to the delegate and releases a registration that arrives after the callback has fired.</summary>
            internal void AwaitStopThenComplete()
            {
                Volatile.Write(ref _stopRegistration, _parent._stopSignal(Stop));

                // Registrations returned after termination are disposed immediately.
                if (Volatile.Read(ref _stopSignalled) != 1)
                {
                    return;
                }

                FireAndForgetHelper.Run(DisposeStopRegistrationAsync);
            }

            /// <summary>Ends the sequence the first time the stop delegate notifies.</summary>
            /// <param name="result">The result the stop delegate reported.</param>
            private void Stop(Result result)
            {
                if (Interlocked.Exchange(ref _stopSignalled, 1) != 0)
                {
                    return;
                }

                FireAndForgetHelper.Run(() => CompleteAsync(result));
            }

            /// <summary>Releases the stop registration, then forwards the outcome to the downstream observer.</summary>
            /// <param name="result">The result the stop delegate reported.</param>
            /// <returns>A task representing the asynchronous completion.</returns>
            private async ValueTask CompleteAsync(Result result)
            {
                await DisposeStopRegistrationAsync().ConfigureAwait(false);

                if (!result.IsFailure)
                {
                    await _lifecycle.RelayCompletionAsync(Result.Success).ConfigureAwait(false);
                    return;
                }

                var error = result.Exception;
                if (_parent._options.SourceFailsWhenOtherFails)
                {
                    await _lifecycle.RelayCompletionAsync(Result.Failure(error)).ConfigureAwait(false);
                    return;
                }

                await _lifecycle.RelayErrorAsync(error).ConfigureAwait(false);
            }

            /// <summary>Disposes the handle the stop delegate returned, exactly once.</summary>
            /// <returns>A task representing the asynchronous dispose operation.</returns>
            private async ValueTask DisposeStopRegistrationAsync()
            {
                if (Volatile.Read(ref _stopRegistration) is not { } registration
                    || Interlocked.Exchange(ref _stopRegistrationDisposed, 1) != 0)
                {
                    return;
                }

                try
                {
                    await registration.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception disposeError)
                {
                    // Secondary disposal failures reach the global handler.
                    UnhandledExceptionHandler.ReportUnhandledException(disposeError);
                }
            }
        }
    }

    /// <summary>Async observable that emits items from the source until the specified task completes.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="task">The task whose completion triggers the end of the sequence.</param>
    /// <param name="options">Options controlling the take-until behavior.</param>
    /// <param name="stopToken">A token whose cancellation also completes the sequence.</param>
    internal sealed class TaskStopSignal<T>(
        IObservableAsync<T> source,
        Task task,
        TakeUntilOptions options,
        CancellationToken stopToken = default) : IObservableAsync<T>
    {
        /// <summary>The source observable sequence.</summary>
        private readonly IObservableAsync<T> _source = source;

        /// <summary>The task whose completion triggers the end of the sequence.</summary>
        private readonly Task _task = task;

        /// <summary>Options controlling the take-until behavior.</summary>
        private readonly TakeUntilOptions _options = options;

        /// <summary>A token whose cancellation also completes the sequence.</summary>
        private readonly CancellationToken _stopToken = stopToken;

        /// <inheritdoc/>
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            TaskStopCoordinator subscription = new(this, observer);
            subscription.LinkExternalCancellation(cancellationToken);
            return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
                subscription,
                () => subscription.SubscribeSourcesAsync(cancellationToken));
        }

        /// <summary>Manages the subscription lifetime and completes the sequence when the task finishes.</summary>
        internal sealed class TaskStopCoordinator : IAsyncDisposable
        {
            /// <summary>The parent observable that owns this subscription.</summary>
            private readonly TaskStopSignal<T> _parent;

            /// <summary>Shared subscription lifecycle (gate / dispose CTS / external link / forwarders).</summary>
            private readonly TakeUntilLifecycle<T> _lifecycle;

            /// <summary>The inner subscription handle.</summary>
            private IAsyncDisposable? _subscription;

            /// <summary>The registration that completes this sequence when the stop token fires.</summary>
            private CancellationTokenRegistration _stopTokenRegistration;

            /// <summary>Initializes a new instance of the <see cref="TaskStopCoordinator"/> class.</summary>
            /// <param name="parent">The parent observable that owns this subscription.</param>
            /// <param name="observer">The downstream observer to forward items to.</param>
            public TaskStopCoordinator(TaskStopSignal<T> parent, IObserverAsync<T> observer)
            {
                _parent = parent;
                _lifecycle = new(observer);
            }

            /// <summary>Asynchronously releases resources used by this subscription.</summary>
            /// <returns>A task representing the asynchronous dispose operation.</returns>
            public async ValueTask DisposeAsync()
            {
#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
                await _stopTokenRegistration.DisposeAsync().ConfigureAwait(false);
#else
                _stopTokenRegistration.Dispose();
#endif

                if (_subscription is not null)
                {
                    await _subscription.DisposeAsync().ConfigureAwait(false);
                }

                await _lifecycle.DisposeAsync().ConfigureAwait(false);
            }

            /// <summary>Subscribes to the source observable and begins waiting for the task to complete.</summary>
            /// <param name="cancellationToken">A token to cancel the subscription.</param>
            /// <returns>A task representing the asynchronous subscribe operation.</returns>
            internal async ValueTask SubscribeSourcesAsync(CancellationToken cancellationToken)
            {
                _stopTokenRegistration = _lifecycle.CompleteWhenCancelled(_parent._stopToken);
                AwaitStopThenComplete(_parent._task);
                _subscription = await _parent._source
                    .SubscribeAsync(new TakeUntilSourceWitness<T>(_lifecycle), cancellationToken).ConfigureAwait(false);
            }

            /// <summary>Forwards the original subscribe-time token into the shared lifecycle's dispose chain.</summary>
            /// <param name="external">The subscribe-time token.</param>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void LinkExternalCancellation(CancellationToken external) =>
                _lifecycle.LinkExternalCancellation(external);

            /// <summary>Waits for the task to complete, then forwards completion or error to the downstream observer.</summary>
            /// <param name="task">The task to await.</param>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void AwaitStopThenComplete(Task task) => FireAndForgetHelper.Run(async () =>
            {
                try
                {
                    await task.WaitAsync(System.Threading.Timeout.InfiniteTimeSpan, _lifecycle.DisposeToken)
                        .ConfigureAwait(false);
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
    /// <param name="stopToken">A token whose cancellation also completes the sequence.</param>
    internal sealed class TakeUntilAsyncSignal<T, TOther>(
        IObservableAsync<T> source,
        IObservableAsync<TOther> other,
        TakeUntilOptions options,
        CancellationToken stopToken = default) : IObservableAsync<T>
    {
        /// <summary>The source observable sequence.</summary>
        private readonly IObservableAsync<T> _source = source;

        /// <summary>The signal observable whose emission triggers completion.</summary>
        private readonly IObservableAsync<TOther> _other = other;

        /// <summary>Options controlling the take-until behavior.</summary>
        private readonly TakeUntilOptions _options = options;

        /// <summary>A token whose cancellation also completes the sequence.</summary>
        private readonly CancellationToken _stopToken = stopToken;

        /// <inheritdoc/>
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            AsyncStopCoordinator subscription = new(this, observer);
            subscription.LinkExternalCancellation(cancellationToken);
            return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
                subscription,
                async () => await subscription.SubscribeSourcesAsync(cancellationToken).ConfigureAwait(false));
        }

        /// <summary>Manages subscriptions to both the source and the signal observable, completing when the signal fires.</summary>
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

            /// <summary>The registration that completes this sequence when the stop token fires.</summary>
            private CancellationTokenRegistration _stopTokenRegistration;

            /// <summary>Initializes a new instance of the <see cref="AsyncStopCoordinator"/> class.</summary>
            /// <param name="parent">The parent observable that owns this subscription.</param>
            /// <param name="observer">The downstream observer to forward items to.</param>
            public AsyncStopCoordinator(TakeUntilAsyncSignal<T, TOther> parent, IObserverAsync<T> observer)
            {
                _parent = parent;
                _lifecycle = new(observer);
            }

            /// <summary>Asynchronously releases resources used by this subscription.</summary>
            /// <returns>A task representing the asynchronous dispose operation.</returns>
            public async ValueTask DisposeAsync()
            {
#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
                await _stopTokenRegistration.DisposeAsync().ConfigureAwait(false);
#else
                _stopTokenRegistration.Dispose();
#endif
                await _otherDisposable.DisposeAsync().ConfigureAwait(false);
                await _disposable.DisposeAsync().ConfigureAwait(false);
                await _lifecycle.DisposeAsync().ConfigureAwait(false);
            }

            /// <summary>Subscribes to both the source and signal observables.</summary>
            /// <param name="cancellationToken">A token to cancel the subscription.</param>
            /// <returns>This subscription as an async disposable.</returns>
            internal async ValueTask<IAsyncDisposable> SubscribeSourcesAsync(CancellationToken cancellationToken)
            {
                _stopTokenRegistration = _lifecycle.CompleteWhenCancelled(_parent._stopToken);

                var otherSubscription = await _parent._other
                    .SubscribeAsync(new StopSignalWitness(this), cancellationToken).ConfigureAwait(false);
                await _otherDisposable.SetDisposableAsync(otherSubscription).ConfigureAwait(false);

                var sourceSubscription =
                    await _parent._source.SubscribeAsync(new TakeUntilSourceWitness<T>(_lifecycle), cancellationToken)
                        .ConfigureAwait(false);
                await _disposable.SetDisposableAsync(sourceSubscription).ConfigureAwait(false);

                return this;
            }

            /// <summary>Forwards the original subscribe-time token into the shared lifecycle's dispose chain.</summary>
            /// <param name="external">The subscribe-time token.</param>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void LinkExternalCancellation(CancellationToken external) =>
                _lifecycle.LinkExternalCancellation(external);

            /// <summary>Observer for the signal observable that triggers completion of the source subscription.</summary>
            /// <param name="parent">The parent coordinator that owns this witness.</param>
            [DebuggerDisplay("StopSignalWitness: {_witness}")]
            internal sealed class StopSignalWitness(AsyncStopCoordinator parent) : IWitnessAsync<TOther>
            {
                /// <summary>The notification gate, cancellation link and disposal state.</summary>
                private WitnessAsyncState _witness;

                /// <inheritdoc/>
                ref WitnessAsyncState IWitnessState.Witness => ref _witness;

                /// <inheritdoc/>
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public ValueTask OnNextAsync(TOther value, CancellationToken cancellationToken) =>
                    WitnessAsync.OnNextAsync(this, value, cancellationToken);

                /// <inheritdoc/>
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
                    WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

                /// <inheritdoc/>
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

                /// <inheritdoc/>
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

                /// <inheritdoc/>
                async ValueTask IWitnessAsync<TOther>.OnNextAsyncCore(TOther value, CancellationToken cancellationToken)
                {
                    _ = value;
                    _ = cancellationToken;
                    await parent._lifecycle.RelayCompletionAsync(Result.Success).ConfigureAwait(false);
                    await DisposeAsync().ConfigureAwait(false);
                }

                /// <inheritdoc/>
                ValueTask IWitnessAsync<TOther>.OnErrorResumeAsyncCore(
                    Exception error,
                    CancellationToken cancellationToken)
                {
                    _ = cancellationToken;
                    return parent._lifecycle.RelayErrorAsync(error);
                }

                /// <inheritdoc/>
                ValueTask IWitnessAsync<TOther>.OnCompletedAsyncCore(Result result) =>
                    !result.IsFailure
                        ? default
                        : parent._lifecycle.RelayCompletionAsync(
                            parent._parent._options.SourceFailsWhenOtherFails
                                ? result
                                : Result.Success);
            }
        }
    }
}
