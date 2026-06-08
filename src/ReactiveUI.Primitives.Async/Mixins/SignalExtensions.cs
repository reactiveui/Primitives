// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Async.Signals;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with signals in a reactive programming context.</summary>
/// <remarks>The methods in this class enable interoperability between signals and asynchronous observer
/// patterns. These extensions are intended to simplify the integration of signals with APIs that expect asynchronous
/// observers.</remarks>
public static class SignalExtensions
{
    /// <summary>Observer-wrapping and value-mapping operators for a signal source.</summary>
    /// <param name="this">The signal to wrap as an asynchronous observer. Cannot be null.</param>
    /// <typeparam name="T">The type of the elements processed by the signal.</typeparam>
    extension<T>(ISignalAsync<T> @this)
    {
        /// <summary>Creates an asynchronous observer wrapper for the specified signal.</summary>
        /// <returns>An asynchronous observer that forwards notifications to the specified signal.</returns>
        [SuppressMessage(
            "Roslynator",
            "RCS1047:Non-asynchronous method name should not end with \'Async\'",
            Justification = "This is an existing method")]
        public IObserverAsync<T> AsObserverAsync()
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);

            return new SignalAsyncObserver<T>(@this);
        }

        /// <summary>
        /// Creates a new signal that applies a transformation to the values of the source signal using the specified
        /// mapping function.
        /// </summary>
        /// <remarks>The returned signal reflects the mapped values of the original signal. Subscribers to the
        /// returned signal will observe the transformed sequence as defined by the mapper function. The mapping is applied
        /// to all values published by the source signal.</remarks>
        /// <param name="mapper">A function that takes an asynchronous observable of type T and returns a transformed asynchronous observable of
        /// type T. This function defines how the values are mapped.</param>
        /// <returns>A signal that emits values transformed by the specified mapping function.</returns>
        public ISignalAsync<T> MapValues(Func<IObservableAsync<T>, IObservableAsync<T>> mapper)
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);
            ArgumentExceptionHelper.ThrowIfNull(mapper);

            return new MappedSignal<T>(@this, mapper);
        }
    }

    /// <summary>A signal that applies a transformation to the observable values of the source signal.</summary>
    /// <param name="original">The source signal.</param>
    /// <param name="mapper">A function that takes an asynchronous observable of type T and returns a transformed asynchronous observable of
    /// type T. This function defines how the values are mapped.</param>
    /// <typeparam name="T">The type of elements processed by the signal.</typeparam>
    internal sealed class MappedSignal<T>(
        ISignalAsync<T> original,
        Func<IObservableAsync<T>, IObservableAsync<T>> mapper) : ISignalAsync<T>
    {
        /// <inheritdoc/>
        public IObservableAsync<T> Values { get; } = mapper(original.Values);

        /// <inheritdoc/>
        public ValueTask<IAsyncDisposable> SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken) =>
            Values.SubscribeAsync(observer, cancellationToken);

        /// <inheritdoc/>
        public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
            original.OnNextAsync(value, cancellationToken);

        /// <inheritdoc/>
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
            original.OnErrorResumeAsync(error, cancellationToken);

        /// <inheritdoc/>
        public ValueTask OnCompletedAsync(Result result) => original.OnCompletedAsync(result);

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => original.DisposeAsync();
    }

    /// <summary>An asynchronous observer that forwards all notifications to the wrapped signal.</summary>
    /// <typeparam name="T">The type of elements processed by the observer.</typeparam>
    /// <param name="signal">The signal to forward notifications to.</param>
    internal sealed class SignalAsyncObserver<T>(ISignalAsync<T> signal) : ObserverAsync<T>
    {
        /// <summary>
        /// Forwards the value to the wrapped signal. The cancellation token is intentionally
        /// replaced with <see cref="CancellationToken.None"/> rather than passing our own dispose
        /// token through: subscribers downstream of the signal are <see cref="ObserverAsync{T}"/>
        /// wraps whose <c>TryEnter</c> short-circuits on <see cref="CancellationToken.None"/> via
        /// its fast path, avoiding a per-emission linked-CTS allocation on every observer. The
        /// upstream-disposal cascade is unaffected — by the time this observer is disposed (on
        /// source completion / error) no further <c>OnNext</c> calls reach this method.
        /// </summary>
        /// <param name="value">The value to be processed by the observer.</param>
        /// <param name="cancellationToken">The token captured by the base observer's TryEnter scope. Ignored on the forward.</param>
        /// <returns>A ValueTask that represents the asynchronous operation.</returns>
        protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            return signal.OnNextAsync(value, CancellationToken.None);
        }

        /// <summary>Forwards the error to the wrapped signal.</summary>
        /// <param name="error">The exception that caused the error condition. Cannot be null.</param>
        /// <param name="cancellationToken">The token captured by the base observer's TryEnter scope. Ignored on the forward.</param>
        /// <returns>A ValueTask that represents the asynchronous error handling operation.</returns>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            return signal.OnErrorResumeAsync(error, CancellationToken.None);
        }

        /// <summary>Performs asynchronous completion logic when the operation has finished, using the specified result.</summary>
        /// <param name="result">The result of the completed operation, containing any relevant outcome information.</param>
        /// <returns>A ValueTask that represents the asynchronous completion operation.</returns>
        protected override ValueTask OnCompletedAsyncCore(Result result) => signal.OnCompletedAsync(result);
    }
}
