// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Threading;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives;

/// <summary>
/// Internal observable helpers for operators that ReactiveUI.Primitives needs but does not expose directly.
/// </summary>
internal static class ObservableMixins
{
    /// <summary>
    /// Forwards source values until <paramref name="other"/> emits a value. Completion of <paramref name="other"/> without
    /// a value does not stop the source.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <typeparam name="TOther">The cancellation value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="other">The observable that stops the source when it emits.</param>
    /// <returns>An observable that completes when the source completes or <paramref name="other"/> emits.</returns>
    public static IObservable<T> TakeUntil<T, TOther>(this IObservable<T> source, IObservable<TOther> other)
    {
#if NET8_0_OR_GREATER
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(other);
#else
        if (source is null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (other is null)
        {
            throw new ArgumentNullException(nameof(other));
        }
#endif

        return Signal.Create<T>(observer =>
        {
            var subscriptions = new MultipleDisposable();
            var gate = new Lock();
            var stopped = 0;

            void Complete()
            {
                if (Interlocked.Exchange(ref stopped, 1) != 0)
                {
                    return;
                }

                lock (gate)
                {
                    observer.OnCompleted();
                }

                subscriptions.Dispose();
            }

            void Error(Exception error)
            {
                if (Interlocked.Exchange(ref stopped, 1) != 0)
                {
                    return;
                }

                lock (gate)
                {
                    observer.OnError(error);
                }

                subscriptions.Dispose();
            }

            subscriptions.Add(other.Subscribe(_ => Complete(), Error));
            if (Volatile.Read(ref stopped) != 0)
            {
                return subscriptions;
            }

            subscriptions.Add(source.Subscribe(
                value =>
                {
                    lock (gate)
                    {
                        if (Volatile.Read(ref stopped) == 0)
                        {
                            observer.OnNext(value);
                        }
                    }
                },
                Error,
                Complete));

            return subscriptions;
        });
    }
}
