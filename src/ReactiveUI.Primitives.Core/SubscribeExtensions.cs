// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives;

/// <summary>Provides subscription extension methods for observables.</summary>
public static class SubscribeExtensions
{
    /// <summary>Error callback that rethrows with the original exception dispatch information.</summary>
    private static readonly Action<Exception> rethrow = static e => ExceptionDispatchInfo.Capture(e).Throw();

    /// <summary>Completion callback that does nothing.</summary>
    private static readonly Action nop = static () => { };

    /// <summary>Exception helpers for a nullable exception receiver.</summary>
    /// <param name="exception">The exception.</param>
    extension(Exception? exception)
    {
        /// <summary>Rethrows Exception.</summary>
        public void Rethrow()
        {
            if (exception is null)
            {
                return;
            }

            throw exception;
        }
    }

    /// <summary>Subscription operators for an observable source sequence.</summary>
    /// <param name="source">Signals sequence to subscribe to.</param>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    extension<T>(IObservable<T> source)
    {
        /// <summary>
        /// Subscribes to the Signals sequence without specifying any handlers.
        /// This method can be used to evaluate the Signals sequence for its side-effects only.
        /// </summary>
        /// <returns><see cref="IDisposable"/> object used to unsubscribe from the Signals sequence.</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="source"/> is <c>null</c>.</exception>
        public IDisposable Subscribe()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source.Subscribe(OnNextNoOpCache<T>.Instance, nop);
        }

        /// <summary>Subscribes to the Signals providing just the <paramref name="onNext" /> delegate.</summary>
        /// <param name="onNext">The on next.</param>
        /// <returns>A IDisposable.</returns>
        public IDisposable Subscribe(Action<T> onNext)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(onNext);

            return source switch
            {
                Signals.Signal<T> signal => signal.SubscribeAction(onNext),
                IInlineSignal<T> inline => inline.Subscribe(onNext, rethrow, nop),
                _ => source.Subscribe(onNext, rethrow, nop)
            };
        }

        /// <summary>Subscribes to the Signals providing both the <paramref name="onNext" /> and <paramref name="onError" /> delegates.</summary>
        /// <param name="onNext">The on next.</param>
        /// <param name="onError">The on error.</param>
        /// <returns>A IDisposable.</returns>
        public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError)
        {
            ArgumentExceptionHelper.ThrowIfNull(onError);

            return source.Subscribe(onNext, onError, nop);
        }

        /// <summary>Subscribes to the Signals providing both the <paramref name="onNext" /> and <paramref name="onCompleted" /> delegates.</summary>
        /// <param name="onNext">The on next.</param>
        /// <param name="onCompleted">The on completed.</param>
        /// <returns>A IDisposable.</returns>
        public IDisposable Subscribe(Action<T> onNext, Action onCompleted)
        {
            ArgumentExceptionHelper.ThrowIfNull(onCompleted);

            return source.Subscribe(onNext, rethrow, onCompleted);
        }

        /// <summary>Subscribes to the Signals providing all three <paramref name="onNext" />, <paramref name="onError" /> and <paramref name="onCompleted" /> delegates.</summary>
        /// <param name="onNext">The on next.</param>
        /// <param name="onError">The on error.</param>
        /// <param name="onCompleted">The on completed.</param>
        /// <returns>A IDisposable.</returns>
        public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(onNext);

            ArgumentExceptionHelper.ThrowIfNull(onError);

            ArgumentExceptionHelper.ThrowIfNull(onCompleted);

            return source is IInlineSignal<T> inline
                ? inline.Subscribe(onNext, onError, onCompleted)
                : source.Subscribe(new EmptyWitness<T>(onNext, onError, onCompleted));
        }
    }

    /// <summary>Holds cached no-op value callbacks by value type.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private static class OnNextNoOpCache<T>
    {
        /// <summary>Gets the cached no-op value callback.</summary>
        public static readonly Action<T> Instance = static _ => { };
    }
}
