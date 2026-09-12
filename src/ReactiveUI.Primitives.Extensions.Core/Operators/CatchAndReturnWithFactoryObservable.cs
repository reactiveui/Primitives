// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Catches <typeparamref name="TException"/>, emits a fallback built from it, and completes. Other exception types
/// propagate unchanged, and an exception thrown by <paramref name="fallbackFactory"/> terminates the sequence in place
/// of the caught one.
/// </summary>
/// <typeparam name="T">Element type.</typeparam>
/// <typeparam name="TException">Exception type to catch.</typeparam>
/// <param name="source">Upstream source.</param>
/// <param name="fallbackFactory">Builds the fallback from the caught exception.</param>
public sealed class CatchAndReturnWithFactoryObservable<T, TException>(
    IObservable<T> source,
    Func<TException, T> fallbackFactory) : IObservable<T>
    where TException : Exception
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(fallbackFactory);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new CatchAndReturnWithFactoryWitness(observer, fallbackFactory));
    }

    /// <summary>Forwarding observer that turns a matching error into the factory's fallback value followed by completion.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="fallbackFactory">The fallback factory.</param>
    private sealed class CatchAndReturnWithFactoryWitness(
        IObserver<T> downstream,
        Func<TException, T> fallbackFactory) : IObserver<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => downstream.OnNext(value);

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (error is not TException typed)
            {
                downstream.OnError(error);
                return;
            }

            T fallback;
            try
            {
                fallback = fallbackFactory(typed);
            }
            catch (Exception factoryError)
            {
                downstream.OnError(factoryError);
                return;
            }

            downstream.OnNext(fallback);
            downstream.OnCompleted();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => downstream.OnCompleted();
    }
}
