// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides factory methods for creating asynchronous observable sequences.</summary>
/// <remarks>The SignalAsync class offers static methods to construct and manipulate asynchronous observables.
/// Use these methods to create sequences that emit values, errors, or completion notifications in an asynchronous
/// manner.</remarks>
public static partial class SignalAsync
{
    /// <summary>Creates an observable sequence that terminates immediately with the specified exception.</summary>
    /// <remarks>Use this method to create an observable sequence that fails immediately, which can be useful
    /// for testing error handling or representing error conditions in reactive workflows.</remarks>
    /// <typeparam name="T">The type of the elements in the observable sequence.</typeparam>
    /// <param name="error">The exception to be propagated to observers as an error notification. Cannot be null.</param>
    /// <returns>An observable sequence of type <typeparamref name="T"/> that signals the specified exception upon subscription.</returns>
    /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="error"/> is null.</exception>
    [SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "Public factory API — caller specifies T explicitly: SignalAsync.Throw<int>(ex).")]
    public static IObservableAsync<T> Fail<T>(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);

        return new ThrowSignalAsync<T>(error);
    }

    /// <summary>Creates an observable sequence that terminates immediately with the specified exception.</summary>
    /// <typeparam name="T">The type of the elements in the observable sequence.</typeparam>
    /// <param name="error">The exception to be propagated to observers as an error notification. Cannot be null.</param>
    /// <returns>An observable sequence of type <typeparamref name="T"/> that signals the specified exception upon subscription.</returns>
    /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="error"/> is null.</exception>
    [SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "Public factory API — caller specifies T explicitly: SignalAsync.Throw<int>(ex).")]
    public static IObservableAsync<T> Throw<T>(Exception error) => Fail<T>(error);

    /// <summary>Represents an asynchronous observable sequence that immediately terminates with the specified exception.</summary>
    /// <remarks>Use this type to create an observable sequence that fails immediately upon subscription,
    /// propagating the provided exception to subscribers. This can be useful for representing error conditions in
    /// asynchronous observable scenarios.</remarks>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    /// <param name="error">The exception that will be signaled to observers as the terminal error.</param>
    internal sealed class ThrowSignalAsync<T>(Exception error) : SignalAsync<T>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            await observer.OnCompletedAsync(Result.Failure(error)).ConfigureAwait(false);
            return DisposableAsync.Empty;
        }
    }
}
