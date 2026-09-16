// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Zip extension methods for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Pair/Zip operators for a first observable source sequence.</summary>
    /// <typeparam name="T1">The type of elements in the first source sequence.</typeparam>
    /// <param name="src1">The first observable sequence.</param>
    extension<T1>(IObservableAsync<T1> src1)
    {
        /// <summary>Combines two observable sequences element-by-element using the specified result selector.</summary>
        /// <typeparam name="T2">The type of elements in the second source sequence.</typeparam>
        /// <typeparam name="TResult">The type of elements in the result sequence.</typeparam>
        /// <param name="second">The second observable sequence. Cannot be null.</param>
        /// <param name="resultSelector">A function to apply to each pair of elements. Cannot be null.</param>
        /// <returns>An observable sequence whose elements are the result of pair-wise combining the source
        /// elements using the result selector.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="src1"/>, <paramref name="second"/> or <paramref name="resultSelector"/> is <see langword="null"/>.</exception>
        /// <remarks>Values wait for their partner at the same index; the result completes when either source completes
        /// with no pending pair.</remarks>
        public IObservableAsync<TResult> Pair<T2, TResult>(
            IObservableAsync<T2> second,
            Func<T1, T2, TResult> resultSelector)
        {
            ArgumentExceptionHelper.ThrowIfNull(src1);
            ArgumentExceptionHelper.ThrowIfNull(second);
            ArgumentExceptionHelper.ThrowIfNull(resultSelector);

            return new ZipSignal<T1, T2, TResult>(src1, second, resultSelector);
        }
    }

    /// <summary>Pairs elements of two sources in arrival order and projects each pair through a selector.</summary>
    /// <typeparam name="T1">The type of the elements in the first source sequence.</typeparam>
    /// <typeparam name="T2">The type of the elements in the second source sequence.</typeparam>
    /// <typeparam name="TResult">The type of the elements in the resulting sequence produced by the selector function.</typeparam>
    /// <param name="first">The first asynchronous observable sequence to combine.</param>
    /// <param name="second">The second asynchronous observable sequence to combine.</param>
    /// <param name="resultSelector">A function that specifies how to combine elements from the first and second sequences into a result element.</param>
    internal sealed class ZipSignal<T1, T2, TResult>(
        IObservableAsync<T1> first,
        IObservableAsync<T2> second,
        Func<T1, T2, TResult> resultSelector) : IObservableAsync<TResult>
    {
        /// <summary>Subscribes the specified observer by creating a shared <see cref="ZipState"/> and subscribing to both source sequences.</summary>
        /// <param name="observer">The observer to receive zipped result elements.</param>
        /// <param name="cancellationToken">A token to cancel the subscription.</param>
        /// <returns>An async disposable that tears down both subscriptions when disposed.</returns>
        async ValueTask<IAsyncDisposable> IObservableAsync<TResult>.SubscribeAsync(
            IObserverAsync<TResult> observer,
            CancellationToken cancellationToken)
        {
            ZipState state = new(observer, resultSelector);
            state.LinkExternalCancellation(cancellationToken);

            var sub1 = await first.SubscribeAsync(
                new FirstWitness(state),
                cancellationToken).ConfigureAwait(false);

            var sub2 = await second.SubscribeAsync(
                new SecondWitness(state),
                cancellationToken).ConfigureAwait(false);

            return new MultipleDisposableAsync(sub1, sub2, state);
        }

        /// <summary>Shared state that coordinates pair-wise combination of elements from both source sequences.</summary>
        /// <param name="observer">The downstream observer to forward combined results to.</param>
        /// <param name="resultSelector">The function that combines paired elements.</param>
        internal sealed class ZipState(
            IObserverAsync<TResult> observer,
            Func<T1, T2, TResult> resultSelector) : IAsyncDisposable
        {
            /// <summary>Cancellation source for disposal.</summary>
            private readonly CancellationTokenSource _disposeCts = new();

            /// <summary>The synchronization gate protecting shared state access.</summary>
            private readonly Lock _gate = new();

            /// <summary>Queue of buffered elements from the first source awaiting a pair from the second source.</summary>
            private readonly Queue<T1> _queue1 = new();

            /// <summary>Queue of buffered elements from the second source awaiting a pair from the first source.</summary>
            private readonly Queue<T2> _queue2 = new();

            /// <summary>Registration that propagates the original subscribe-token cancellation into <see cref="_disposeCts"/>.</summary>
            private CancellationTokenRegistration _externalLinkRegistration;

            /// <summary>Whether this state has been disposed.</summary>
            private int _disposed;

            /// <summary>Indicates whether the first source has completed.</summary>
            private bool _completed1;

            /// <summary>Indicates whether the second source has completed.</summary>
            private bool _completed2;

            /// <summary>Indicates whether the zip operation has finished and no more elements will be emitted.</summary>
            private bool _done;

            /// <inheritdoc/>
            public async ValueTask DisposeAsync()
            {
                if (DisposalHelper.TrySetDisposed(ref _disposed))
                {
                    return;
                }

                await _disposeCts.CancelAsync().ConfigureAwait(false);
#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
                await _externalLinkRegistration.DisposeAsync().ConfigureAwait(false);
#else
                _externalLinkRegistration.Dispose();
#endif
                _disposeCts.Dispose();
            }

            /// <summary>Handles a new element from the first source, pairing it with a buffered element from the second source if available.</summary>
            /// <param name="value">The element from the first source.</param>
            /// <param name="token">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            internal async ValueTask OnNext1Async(T1 value, CancellationToken token)
            {
                _ = token;
                T2 second;
                lock (_gate)
                {
                    if (_done)
                    {
                        return;
                    }

                    if (_queue2.Count > 0)
                    {
                        second = _queue2.Dequeue();
                    }
                    else
                    {
                        _queue1.Enqueue(value);
                        return;
                    }
                }

                var result = resultSelector(value, second);
                await observer.OnNextAsync(result, _disposeCts.Token).ConfigureAwait(false);
            }

            /// <summary>Handles a new element from the second source, pairing it with a buffered element from the first source if available.</summary>
            /// <param name="value">The element from the second source.</param>
            /// <param name="token">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            internal async ValueTask OnNext2Async(T2 value, CancellationToken token)
            {
                _ = token;
                T1 firstVal;
                lock (_gate)
                {
                    if (_done)
                    {
                        return;
                    }

                    if (_queue1.Count > 0)
                    {
                        firstVal = _queue1.Dequeue();
                    }
                    else
                    {
                        _queue2.Enqueue(value);
                        return;
                    }
                }

                var result = resultSelector(firstVal, value);
                await observer.OnNextAsync(result, _disposeCts.Token).ConfigureAwait(false);
            }

            /// <summary>Handles the first source completing, propagating completion downstream when appropriate.</summary>
            /// <param name="result">The completion result from the first source.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            internal async ValueTask OnCompleted1Async(Result result)
            {
                bool shouldComplete;
                lock (_gate)
                {
                    if (_done)
                    {
                        return;
                    }

                    _completed1 = true;
                    shouldComplete = result.IsFailure || _completed2 || _queue1.Count == 0;
                    if (shouldComplete)
                    {
                        _done = true;
                    }
                }

                if (shouldComplete)
                {
                    await observer.OnCompletedAsync(result).ConfigureAwait(false);
                }
            }

            /// <summary>Handles the second source completing, propagating completion downstream when appropriate.</summary>
            /// <param name="result">The completion result from the second source.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            internal async ValueTask OnCompleted2Async(Result result)
            {
                bool shouldComplete;
                lock (_gate)
                {
                    if (_done)
                    {
                        return;
                    }

                    _completed2 = true;
                    shouldComplete = result.IsFailure || _completed1 || _queue2.Count == 0;
                    if (shouldComplete)
                    {
                        _done = true;
                    }
                }

                if (shouldComplete)
                {
                    await observer.OnCompletedAsync(result).ConfigureAwait(false);
                }
            }

            /// <summary>Forwards a non-fatal error from either source to the downstream observer.</summary>
            /// <param name="error">The error to forward.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
                observer.OnErrorResumeAsync(error, cancellationToken);

            /// <summary>Routes cancellation of the subscribe-time token into <see cref="_disposeCts"/>, so per-emission code needs no linked source.</summary>
            /// <param name="external">The subscribe-time token.</param>
            internal void LinkExternalCancellation(CancellationToken external)
            {
                if (!external.CanBeCanceled)
                {
                    return;
                }

                if (external.IsCancellationRequested)
                {
                    _disposeCts.Cancel();
                    return;
                }

                _externalLinkRegistration = external.UnsafeRegister(
                    static state => ((CancellationTokenSource)state!).Cancel(),
                    _disposeCts);
            }
        }

        /// <summary>Observer for the first source sequence that delegates to the shared <see cref="ZipState"/>.</summary>
        /// <param name="state">The shared zip state.</param>
        [DebuggerDisplay("FirstWitness: {_witness}")]
        internal sealed class FirstWitness(ZipState state) : IWitnessAsync<T1>
        {
            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness;

            /// <inheritdoc/>
            ref WitnessAsyncState IWitnessState.Witness => ref _witness;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnNextAsync(T1 value, CancellationToken cancellationToken) =>
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

            /// <summary>Forwards an element from the first source to the zip state for pairing.</summary>
            /// <param name="value">The element from the first source.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T1>.OnNextAsyncCore(T1 value, CancellationToken cancellationToken) =>
                state.OnNext1Async(value, cancellationToken);

            /// <summary>Forwards a non-fatal error to the downstream observer.</summary>
            /// <param name="error">The error to forward.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T1>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                state.OnErrorResumeAsync(error, cancellationToken);

            /// <summary>Handles the first source completing.</summary>
            /// <param name="result">The completion result.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T1>.OnCompletedAsyncCore(Result result) =>
                state.OnCompleted1Async(result);
        }

        /// <summary>Observer for the second source sequence that delegates to the shared <see cref="ZipState"/>.</summary>
        /// <param name="state">The shared zip state.</param>
        [DebuggerDisplay("SecondWitness: {_witness}")]
        internal sealed class SecondWitness(ZipState state) : IWitnessAsync<T2>
        {
            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness;

            /// <inheritdoc/>
            ref WitnessAsyncState IWitnessState.Witness => ref _witness;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnNextAsync(T2 value, CancellationToken cancellationToken) =>
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

            /// <summary>Forwards an element from the second source to the zip state for pairing.</summary>
            /// <param name="value">The element from the second source.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T2>.OnNextAsyncCore(T2 value, CancellationToken cancellationToken) =>
                state.OnNext2Async(value, cancellationToken);

            /// <summary>Forwards a non-fatal error to the downstream observer.</summary>
            /// <param name="error">The error to forward.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T2>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                state.OnErrorResumeAsync(error, cancellationToken);

            /// <summary>Handles the second source completing.</summary>
            /// <param name="result">The completion result.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T2>.OnCompletedAsyncCore(Result result) =>
                state.OnCompleted2Async(result);
        }
    }
}
