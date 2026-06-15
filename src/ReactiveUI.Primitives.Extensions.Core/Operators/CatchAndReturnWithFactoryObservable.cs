// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Catches the configured exception type, emits a fallback built from the exception, and completes.
/// Other exception types propagate downstream.
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

    /// <summary>
    /// Forwarding observer that passes <see cref="OnNext"/> / <see cref="OnCompleted"/>
    /// through and converts a matching <see cref="OnError"/> into an inline emit of the
    /// factory-produced fallback followed by terminal <see cref="IObserver{T}.OnCompleted"/>.
    /// </summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="fallbackFactory">The fallback factory.</param>
    private sealed class CatchAndReturnWithFactoryWitness(
        IObserver<T> downstream,
        Func<TException, T> fallbackFactory) : IObserver<T>
    {
        /// <inheritdoc/>
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
        public void OnCompleted() => downstream.OnCompleted();
    }
}
