// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Signals.Core;

namespace ReactiveUI.Primitives;

/// <summary>
/// SubscribeMixins.
/// </summary>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
public static class SubscribeMixins
{
    /// <summary>
    /// Error callback that rethrows with the original exception dispatch information.
    /// </summary>
    private static readonly Action<Exception> rethrow = e => ExceptionDispatchInfo.Capture(e).Throw();

    /// <summary>
    /// Completion callback that does nothing.
    /// </summary>
    private static readonly Action nop = () => { };

    /// <summary>
    /// Subscribes to the Signals sequence without specifying any handlers.
    /// This method can be used to evaluate the Signals sequence for its side-effects only.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">Signals sequence to subscribe to.</param>
    /// <returns><see cref="IDisposable"/> object used to unsubscribe from the Signals sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <c>null</c>.</exception>
    public static IDisposable Subscribe<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return Subscribe(source, OnNextNoOpCache<T>.Instance, nop);
    }

    /// <summary>
    /// Subscribes to the Signals providing just the <paramref name="onNext" /> delegate.
    /// </summary>
    /// <typeparam name="T">The Type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="onNext">The on next.</param>
    /// <returns>A IDisposable.</returns>
    public static IDisposable Subscribe<T>(this IObservable<T> source, Action<T> onNext)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (onNext == null)
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

        return Subscribe(source, onNext, rethrow, nop);
    }

    /// <summary>
    /// Subscribes to the Signals providing both the <paramref name="onNext" /> and
    /// <paramref name="onError" /> delegates.
    /// </summary>
    /// <typeparam name="T">The Type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="onNext">The on next.</param>
    /// <param name="onError">The on error.</param>
    /// <returns>A IDisposable.</returns>
    public static IDisposable Subscribe<T>(this IObservable<T> source, Action<T> onNext, Action<Exception> onError)
    {
        if (onError == null)
        {
            throw new ArgumentNullException(nameof(onError));
        }

        return Subscribe(source, onNext, onError, nop);
    }

    /// <summary>
    /// Subscribes to the Signals providing both the <paramref name="onNext" /> and
    /// <paramref name="onCompleted" /> delegates.
    /// </summary>
    /// <typeparam name="T">The Type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="onNext">The on next.</param>
    /// <param name="onCompleted">The on completed.</param>
    /// <returns>A IDisposable.</returns>
    public static IDisposable Subscribe<T>(this IObservable<T> source, Action<T> onNext, Action onCompleted)
    {
        if (onCompleted == null)
        {
            throw new ArgumentNullException(nameof(onCompleted));
        }

        return Subscribe(source, onNext, rethrow, onCompleted);
    }

    /// <summary>
    /// Subscribes to the Signals providing all three <paramref name="onNext" />,
    /// <paramref name="onError" /> and <paramref name="onCompleted" /> delegates.
    /// </summary>
    /// <typeparam name="T">The Type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="onNext">The on next.</param>
    /// <param name="onError">The on error.</param>
    /// <param name="onCompleted">The on completed.</param>
    /// <returns>A IDisposable.</returns>
    public static IDisposable Subscribe<T>(this IObservable<T> source, Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (onNext == null)
        {
            throw new ArgumentNullException(nameof(onNext));
        }

        if (onError == null)
        {
            throw new ArgumentNullException(nameof(onError));
        }

        if (onCompleted == null)
        {
            throw new ArgumentNullException(nameof(onCompleted));
        }

        if (source is IInlineSignal<T> inline)
        {
            return inline.Subscribe(onNext, onError, onCompleted);
        }

        return source.Subscribe(new EmptyWitness<T>(onNext, onError, onCompleted));
    }

    /// <summary>
    /// Rethrows Exception.
    /// </summary>
    /// <param name="exception">The exception.</param>
    public static void Rethrow(this Exception? exception)
    {
        if (exception == null)
        {
            return;
        }

        throw exception;
    }

    /// <summary>
    /// Holds cached no-op value callbacks by value type.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    private static class OnNextNoOpCache<T>
    {
        /// <summary>
        /// Gets the cached no-op value callback.
        /// </summary>
        public static readonly Action<T> Instance = _ => { };
    }
}
