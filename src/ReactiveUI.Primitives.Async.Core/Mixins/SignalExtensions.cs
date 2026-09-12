// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with signals in a reactive programming context.</summary>
public static class SignalExtensions
{
    /// <summary>Observer-wrapping and value-mapping operators for a signal source.</summary>
    /// <typeparam name="T">The type of the elements processed by the signal.</typeparam>
    /// <param name="source">The signal to wrap as an asynchronous observer. Cannot be null.</param>
    extension<T>(ISignalAsync<T> source)
    {
        /// <summary>Creates an asynchronous observer wrapper for the specified signal.</summary>
        /// <returns>An asynchronous observer that forwards notifications to the specified signal.</returns>
        [SuppressMessage(
            "Roslynator",
            "RCS1047:Non-asynchronous method name should not end with \'Async\'",
            Justification = "The suffix names the IObserverAsync the method returns, not asynchronous work.")]
        public IObserverAsync<T> AsObserverAsync()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new SignalAsyncWitness<T>(source);
        }

        /// <summary>
        /// Creates a new signal that applies a transformation to the values of the source signal using the specified
        /// mapping function.
        /// </summary>
        /// <param name="mapper">A function that takes an asynchronous observable of type T and returns a transformed asynchronous observable of
        /// type T. This function defines how the values are mapped.</param>
        /// <returns>A signal that publishes into <paramref name="source"/> but exposes the mapped sequence to its own
        /// subscribers.</returns>
        /// <remarks><paramref name="mapper"/> runs once, against the source's value sequence, rather than per
        /// subscriber.</remarks>
        public ISignalAsync<T> MapValues(Func<IObservableAsync<T>, IObservableAsync<T>> mapper)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(mapper);

            return new MappedSignal<T>(source, mapper);
        }
    }

    /// <summary>A signal that applies a transformation to the observable values of the source signal.</summary>
    /// <typeparam name="T">The type of elements processed by the signal.</typeparam>
    /// <param name="original">The source signal.</param>
    /// <param name="mapper">A function that takes an asynchronous observable of type T and returns a transformed asynchronous observable of
    /// type T. This function defines how the values are mapped.</param>
    internal sealed class MappedSignal<T>(
        ISignalAsync<T> original,
        Func<IObservableAsync<T>, IObservableAsync<T>> mapper) : ISignalAsync<T>
    {
        /// <inheritdoc/>
        public IObservableAsync<T> Values { get; } = mapper(original.Values);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IAsyncDisposable> SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken) =>
            Values.SubscribeAsync(observer, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
            original.OnNextAsync(value, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
            original.OnErrorResumeAsync(error, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnCompletedAsync(Result result) => original.OnCompletedAsync(result);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => original.DisposeAsync();
    }

    /// <summary>An asynchronous observer that forwards all notifications to the wrapped signal.</summary>
    /// <typeparam name="T">The type of elements processed by the observer.</typeparam>
    /// <param name="signal">The signal to forward notifications to.</param>
    internal sealed class SignalAsyncWitness<T>(ISignalAsync<T> signal) : WitnessAsync<T>
    {
        /// <summary>
        /// Forwards the value to the wrapped signal under <see cref="CancellationToken.None"/>, which every downstream
        /// <see cref="WitnessAsync{T}"/> takes its no-link fast path on. Passing this observer's own dispose token
        /// instead would buy nothing: disposal stops values from reaching this method at all.
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

        /// <summary>Forwards the terminal result to the wrapped signal.</summary>
        /// <param name="result">The result of the completed operation, containing any relevant outcome information.</param>
        /// <returns>A task that completes when the signal has handled the result.</returns>
        protected override ValueTask OnCompletedAsyncCore(Result result) => signal.OnCompletedAsync(result);
    }
}
