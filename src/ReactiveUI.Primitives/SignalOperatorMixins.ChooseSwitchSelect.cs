// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives;

/// <summary>
/// Fused projection operators: <c>Choose</c> (filter + map in one sink) and <c>SwitchSelect</c>
/// (filter-null + map-to-inner + switch-to-latest in one sink).
/// </summary>
public static partial class LinqMixins
{
    /// <summary>
    /// Maps each source value to a <c>(HasValue, Value)</c> pair and forwards only the values whose
    /// <c>HasValue</c> is <see langword="true"/> — a single fused sink in place of <c>Where(...).Select(...)</c>.
    /// Unlike a <c>TOut?</c>-returning projection, the explicit flag lets a non-nullable value type be skipped.
    /// </summary>
    /// <typeparam name="TIn">The source element type.</typeparam>
    /// <typeparam name="TOut">The forwarded element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="chooser">Maps a source value to <c>(HasValue, Value)</c>; the value is skipped when <c>HasValue</c> is <see langword="false"/>.</param>
    /// <returns>An observable of the chosen values.</returns>
    public static IObservable<TOut> Choose<TIn, TOut>(this IObservable<TIn> source, Func<TIn, (bool HasValue, TOut Value)> chooser)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (chooser == null)
        {
            throw new ArgumentNullException(nameof(chooser));
        }

        return new ChooseSignal<TIn, TOut>(source, chooser);
    }

    /// <summary>
    /// Filters out null source values, projects each remaining value to an inner observable, and mirrors only the
    /// latest inner observable — a single fused sink in place of <c>WhereNotNull().Select(selector).Switch()</c>.
    /// </summary>
    /// <typeparam name="TSource">The (nullable) source element type.</typeparam>
    /// <typeparam name="TResult">The element type of the projected inner observables.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="selector">Projects each non-null source value to an inner observable.</param>
    /// <returns>An observable that mirrors the latest projected inner observable.</returns>
    public static IObservable<TResult> SwitchSelect<TSource, TResult>(this IObservable<TSource?> source, Func<TSource, IObservable<TResult>> selector)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (selector == null)
        {
            throw new ArgumentNullException(nameof(selector));
        }

        return new SwitchSelectSignal<TSource, TResult>(source, selector);
    }

    /// <summary>A fused filter + map observable.</summary>
    /// <typeparam name="TIn">The source element type.</typeparam>
    /// <typeparam name="TOut">The forwarded element type.</typeparam>
    private sealed class ChooseSignal<TIn, TOut>(IObservable<TIn> source, Func<TIn, (bool HasValue, TOut Value)> chooser) : IObservable<TOut>
    {
        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TOut> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            return source.Subscribe(new Sink(observer, chooser));
        }

        /// <summary>Applies the chooser to each value and forwards only the chosen ones.</summary>
        private sealed class Sink(IObserver<TOut> downstream, Func<TIn, (bool HasValue, TOut Value)> chooser) : IObserver<TIn>
        {
            /// <inheritdoc/>
            public void OnNext(TIn value)
            {
                (bool HasValue, TOut Value) result;
                try
                {
                    result = chooser(value);
                }
                catch (Exception ex)
                {
                    downstream.OnError(ex);
                    return;
                }

                if (!result.HasValue)
                {
                    return;
                }

                downstream.OnNext(result.Value);
            }

            /// <inheritdoc/>
            public void OnError(Exception error) => downstream.OnError(error);

            /// <inheritdoc/>
            public void OnCompleted() => downstream.OnCompleted();
        }
    }

    /// <summary>A fused filter-null + map-to-inner + switch-to-latest observable.</summary>
    /// <typeparam name="TSource">The (nullable) source element type.</typeparam>
    /// <typeparam name="TResult">The element type of the projected inner observables.</typeparam>
    private sealed class SwitchSelectSignal<TSource, TResult>(IObservable<TSource?> source, Func<TSource, IObservable<TResult>> selector) : IObservable<TResult>
    {
        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new Sink(selector, observer);
            sink.Run(source);
            return sink;
        }

        /// <summary>Subscribes to the source, switching the active inner subscription on each non-null value.</summary>
        private sealed class Sink(Func<TSource, IObservable<TResult>> selector, IObserver<TResult> downstream)
            : IObserver<TSource?>, IDisposable
        {
            /// <summary>Guards the switching state so outer and inner notifications stay consistent.</summary>
            private readonly Lock _gate = new();

            /// <summary>The outer (source) subscription.</summary>
            private readonly OnceDisposable _outer = new();

            /// <summary>The active inner subscription; assigning a new value disposes the previous one.</summary>
            private readonly SwapDisposable _inner = new();

            /// <summary>Generation id of the most recent inner observable; stale inner notifications are ignored.</summary>
            private ulong _latest;

            /// <summary>Whether an inner subscription is currently active.</summary>
            private bool _hasInner;

            /// <summary>Whether the outer source has completed.</summary>
            private bool _outerCompleted;

            /// <summary>Whether this sink has been disposed.</summary>
            private bool _disposed;

            /// <summary>Begins observing the source.</summary>
            /// <param name="source">The source observable.</param>
            public void Run(IObservable<TSource?> source) => _outer.Disposable = source.Subscribe(this);

            /// <inheritdoc/>
            public void OnNext(TSource? value)
            {
                if (value is null)
                {
                    return;
                }

                IObservable<TResult> inner;
                try
                {
                    inner = selector(value);
                }
                catch (Exception ex)
                {
                    OnError(ex);
                    return;
                }

                ulong id;
                lock (_gate)
                {
                    if (_disposed)
                    {
                        return;
                    }

                    id = ++_latest;
                    _hasInner = true;
                }

                _inner.Disposable = inner.Subscribe(new InnerObserver(this, id));
            }

            /// <inheritdoc/>
            public void OnError(Exception error)
            {
                lock (_gate)
                {
                    if (_disposed)
                    {
                        return;
                    }
                }

                downstream.OnError(error);
                Dispose();
            }

            /// <inheritdoc/>
            public void OnCompleted()
            {
                bool complete;
                lock (_gate)
                {
                    if (_disposed)
                    {
                        return;
                    }

                    _outerCompleted = true;
                    complete = !_hasInner;
                }

                if (!complete)
                {
                    return;
                }

                downstream.OnCompleted();
                Dispose();
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                lock (_gate)
                {
                    if (_disposed)
                    {
                        return;
                    }

                    _disposed = true;
                }

                _outer.Dispose();
                _inner.Dispose();
            }

            /// <summary>Forwards an inner value to the downstream observer if it belongs to the active inner subscription.</summary>
            /// <param name="id">The generation id of the inner subscription that produced the value.</param>
            /// <param name="value">The value to forward.</param>
            private void InnerOnNext(ulong id, TResult value)
            {
                lock (_gate)
                {
                    if (_disposed || id != _latest)
                    {
                        return;
                    }
                }

                downstream.OnNext(value);
            }

            /// <summary>Forwards an inner error to the downstream observer if it belongs to the active inner subscription.</summary>
            /// <param name="id">The generation id of the inner subscription that errored.</param>
            /// <param name="error">The error to forward.</param>
            private void InnerOnError(ulong id, Exception error)
            {
                lock (_gate)
                {
                    if (_disposed || id != _latest)
                    {
                        return;
                    }
                }

                downstream.OnError(error);
                Dispose();
            }

            /// <summary>Clears the active inner subscription; completes downstream only if the outer has also completed.</summary>
            /// <param name="id">The generation id of the inner subscription that completed.</param>
            private void InnerOnCompleted(ulong id)
            {
                bool complete;
                lock (_gate)
                {
                    if (_disposed || id != _latest)
                    {
                        return;
                    }

                    _hasInner = false;
                    complete = _outerCompleted;
                }

                if (!complete)
                {
                    return;
                }

                downstream.OnCompleted();
                Dispose();
            }

            /// <summary>Observes a single inner observable and routes its notifications through the parent sink.</summary>
            /// <param name="parent">The owning sink.</param>
            /// <param name="id">The generation id of this inner subscription.</param>
            private sealed class InnerObserver(Sink parent, ulong id) : IObserver<TResult>
            {
                /// <inheritdoc/>
                public void OnNext(TResult value) => parent.InnerOnNext(id, value);

                /// <inheritdoc/>
                public void OnError(Exception error) => parent.InnerOnError(id, error);

                /// <inheritdoc/>
                public void OnCompleted() => parent.InnerOnCompleted(id);
            }
        }
    }
}
