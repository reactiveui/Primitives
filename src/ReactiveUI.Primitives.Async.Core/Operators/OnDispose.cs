// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for composing and managing asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Disposal-callback operators that run an action when the observable source subscription is disposed.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Registers a callback to be invoked asynchronously when the observable sequence is disposed.</summary>
        /// <param name="disposeAction">A function that returns a ValueTask representing the asynchronous operation to execute upon disposal of the
        /// observable sequence. Cannot be null.</param>
        /// <returns>An SignalAsync{T} that invokes the specified asynchronous callback when disposed.</returns>
        /// <remarks>The callback runs when the subscription is disposed, whether explicitly or through completion or
        /// error.</remarks>
        public IObservableAsync<T> OnDispose(Func<ValueTask> disposeAction)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(disposeAction);

            return new OnDisposeSignal<T>(source, disposeAction);
        }

        /// <summary>Registers an action to be invoked when the observable sequence is disposed.</summary>
        /// <param name="disposeAction">The action to execute when the subscription is disposed. Cannot be null.</param>
        /// <returns>An observable sequence that invokes the specified action upon disposal of the subscription.</returns>
        /// <remarks>The action runs synchronously during disposal; chained registrations run in the order they were
        /// added.</remarks>
        public IObservableAsync<T> OnDispose(Action disposeAction)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(disposeAction);

            return new OnDisposeSignal<T>(source, disposeAction);
        }
    }

    /// <summary>Wraps a source observable with an observer that runs a callback when the subscription is disposed.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    internal sealed class OnDisposeSignal<T> : IObservableAsync<T>
    {
        /// <summary>The upstream observable.</summary>
        private readonly IObservableAsync<T> _source;

        /// <summary>The synchronous dispose action; <see langword="null"/> when the callback is asynchronous.</summary>
        private readonly Action? _finallySync;

        /// <summary>The asynchronous dispose callback; <see langword="null"/> when the callback is synchronous.</summary>
        private readonly Func<ValueTask>? _finallyAsync;

        /// <summary>Initializes a new instance of the <see cref="OnDisposeSignal{T}"/> class with an asynchronous callback.</summary>
        /// <param name="source">The upstream observable.</param>
        /// <param name="disposeAction">The asynchronous dispose callback.</param>
        internal OnDisposeSignal(IObservableAsync<T> source, Func<ValueTask> disposeAction)
        {
            _source = source;
            _finallyAsync = disposeAction;
        }

        /// <summary>Initializes a new instance of the <see cref="OnDisposeSignal{T}"/> class with a synchronous action.</summary>
        /// <param name="source">The upstream observable.</param>
        /// <param name="disposeAction">The synchronous dispose action.</param>
        internal OnDisposeSignal(IObservableAsync<T> source, Action disposeAction)
        {
            _source = source;
            _finallySync = disposeAction;
        }

        /// <inheritdoc/>
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            OnDisposeWitness<T> sink = new(observer, _finallySync, _finallyAsync);
            return _source.SubscribeAsync(sink, cancellationToken);
        }
    }

    /// <summary>A witness that runs a callback when disposed.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="observer">The downstream observer to forward notifications to.</param>
    /// <param name="finallySync">The synchronous action to invoke when disposed; <see langword="null"/> when the callback is asynchronous.</param>
    /// <param name="finallyAsync">The asynchronous callback to invoke when disposed; <see langword="null"/> when the callback is synchronous.</param>
    [DebuggerDisplay("OnDisposeWitness: {_witness}")]
    internal sealed class OnDisposeWitness<T>(
        IObserverAsync<T> observer,
        Action? finallySync,
        Func<ValueTask>? finallyAsync) : IWitnessAsync<T>
    {
        /// <summary>The notification gate, cancellation link and disposal state.</summary>
        private WitnessAsyncState _witness;

        /// <summary>Gets the observer that receives forwarded notifications.</summary>
        private IObserverAsync<T> Downstream { get; } = observer;

        /// <inheritdoc/>
        ref WitnessAsyncState IWitnessState.Witness => ref _witness;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
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
        ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
            Downstream.OnNextAsync(value, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            Downstream.OnErrorResumeAsync(error, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) => Downstream.OnCompletedAsync(result);

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            ExceptionDispatchInfo? failure = null;
            try
            {
                if (finallySync is not null)
                {
                    finallySync();
                }
                else if (finallyAsync is not null)
                {
                    await finallyAsync().ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                failure = ExceptionDispatchInfo.Capture(e);
            }

            await WitnessAsync.DisposeStateAsync(this).ConfigureAwait(false);
            failure?.Throw();
        }
    }
}
