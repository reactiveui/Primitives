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
    private static readonly Action<Exception> rethrow = e => ExceptionDispatchInfo.Capture(e).Throw();
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

        return Subscribe(source, OnNextNoOp<T>(), nop);
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
        => Subscribe(source, onNext, onError, nop);

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
        => Subscribe(source, onNext, rethrow, onCompleted);

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
        if (source is IInlineSignal<T> inline)
        {
            return inline.Subscribe(onNext, onError, onCompleted);
        }

        return source?.Subscribe(new EmptyWitness<T>(onNext, onError, onCompleted))!;
    }

    /// <summary>
    /// Rethrows Exception.
    /// </summary>
    /// <param name="exception">The exception.</param>
    public static void Rethrow(this Exception? exception)
    {
        if (exception != null)
        {
            throw exception;
        }
    }

    private static Action<T> OnNextNoOp<T>() => _ => { };
}
