// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides factory methods for creating asynchronous observable sequences.</summary>
public static partial class SignalAsync
{
    /// <summary>Creates an observable sequence that terminates immediately with the specified exception.</summary>
    /// <typeparam name="T">The type of the elements in the observable sequence.</typeparam>
    /// <param name="error">The exception to be propagated to observers as an error notification. Cannot be null.</param>
    /// <returns>An observable sequence of type <typeparamref name="T"/> that signals the specified exception upon subscription.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="error"/> is <see langword="null"/>.</exception>
    /// <remarks>The exception arrives as a terminal completion, not thrown from the subscribe call.</remarks>
    [SuppressMessage(
        "Design",
        "SST2307:Generic method type parameters should be inferable from the parameters",
        Justification = "The element type cannot be inferred from an exception; the caller states it: SignalAsync.Fail<int>(ex).")]
    public static IObservableAsync<T> Fail<T>(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);

        return new ThrowSignalAsync<T>(error);
    }

    /// <summary>Creates an observable sequence that terminates immediately with the specified exception.</summary>
    /// <typeparam name="T">The type of the elements in the observable sequence.</typeparam>
    /// <param name="error">The exception to be propagated to observers as an error notification. Cannot be null.</param>
    /// <returns>An observable sequence of type <typeparamref name="T"/> that signals the specified exception upon subscription.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="error"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SuppressMessage(
        "Design",
        "SST2307:Generic method type parameters should be inferable from the parameters",
        Justification = "The element type cannot be inferred from an exception; the caller states it: SignalAsync.Throw<int>(ex).")]
    public static IObservableAsync<T> Throw<T>(Exception error) =>
        new ThrowSignalAsync<T>(error ?? throw new ArgumentNullException(nameof(error)));

    /// <summary>Represents an asynchronous observable sequence that immediately terminates with the specified exception.</summary>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    /// <param name="error">The exception that will be signaled to observers as the terminal error.</param>
    internal sealed class ThrowSignalAsync<T>(Exception error) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            await observer.OnCompletedAsync(Result.Failure(error)).ConfigureAwait(false);
            return DisposableAsync.Empty;
        }
    }
}
