// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Signals.Core;

namespace ReactiveUI.Primitives;

/// <summary>SubscribeExtensions.</summary>
public static class SubscribeExtensions
{
    /// <summary>Error callback that rethrows with the original exception dispatch information.</summary>
    private static readonly Action<Exception> rethrow = e => ExceptionDispatchInfo.Capture(e).Throw();

    /// <summary>Completion callback that does nothing.</summary>
    private static readonly Action nop = () => { };

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
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <c>null</c>.</exception>
        public IDisposable Subscribe()
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            return source.Subscribe(OnNextNoOpCache<T>.Instance, nop);
        }

        /// <summary>Subscribes to the Signals providing just the <paramref name="onNext" /> delegate.</summary>
        /// <param name="onNext">The on next.</param>
        /// <returns>A IDisposable.</returns>
        public IDisposable Subscribe(Action<T> onNext)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (onNext is null)
            {
                throw new ArgumentNullException(nameof(onNext));
            }

            if (source is Signals.Signal<T> signal)
            {
                return signal.SubscribeAction(onNext);
            }

            if (source is IInlineSignal<T> inline)
            {
                return inline.Subscribe(onNext, rethrow, nop);
            }

            return source.Subscribe(onNext, rethrow, nop);
        }

        /// <summary>Subscribes to the Signals providing both the <paramref name="onNext" /> and <paramref name="onError" /> delegates.</summary>
        /// <param name="onNext">The on next.</param>
        /// <param name="onError">The on error.</param>
        /// <returns>A IDisposable.</returns>
        public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError)
        {
            if (onError is null)
            {
                throw new ArgumentNullException(nameof(onError));
            }

            return source.Subscribe(onNext, onError, nop);
        }

        /// <summary>Subscribes to the Signals providing both the <paramref name="onNext" /> and <paramref name="onCompleted" /> delegates.</summary>
        /// <param name="onNext">The on next.</param>
        /// <param name="onCompleted">The on completed.</param>
        /// <returns>A IDisposable.</returns>
        public IDisposable Subscribe(Action<T> onNext, Action onCompleted)
        {
            if (onCompleted is null)
            {
                throw new ArgumentNullException(nameof(onCompleted));
            }

            return source.Subscribe(onNext, rethrow, onCompleted);
        }

        /// <summary>Subscribes to the Signals providing all three <paramref name="onNext" />, <paramref name="onError" /> and <paramref name="onCompleted" /> delegates.</summary>
        /// <param name="onNext">The on next.</param>
        /// <param name="onError">The on error.</param>
        /// <param name="onCompleted">The on completed.</param>
        /// <returns>A IDisposable.</returns>
        public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (onNext is null)
            {
                throw new ArgumentNullException(nameof(onNext));
            }

            if (onError is null)
            {
                throw new ArgumentNullException(nameof(onError));
            }

            if (onCompleted is null)
            {
                throw new ArgumentNullException(nameof(onCompleted));
            }

            if (source is IInlineSignal<T> inline)
            {
                return inline.Subscribe(onNext, onError, onCompleted);
            }

            return source.Subscribe(new EmptyWitness<T>(onNext, onError, onCompleted));
        }
    }

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

    /// <summary>Holds cached no-op value callbacks by value type.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private static class OnNextNoOpCache<T>
    {
        /// <summary>Gets the cached no-op value callback.</summary>
        public static readonly Action<T> Instance = _ => { };
    }
}
