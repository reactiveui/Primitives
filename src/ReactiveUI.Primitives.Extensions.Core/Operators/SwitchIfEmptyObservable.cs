// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Forwards the source's values, and when the source completes without having emitted any, subscribes
/// <paramref name="fallback"/> and forwards that sequence instead. A source error propagates without the fallback being
/// tried.
/// </summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="fallback">The fallback observable.</param>
public sealed class SwitchIfEmptyObservable<T>(
    IObservable<T> source,
    IObservable<T> fallback) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(fallback);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SwitchIfEmptySink sink = new(observer, fallback);
        var sub = source.Subscribe(sink);
        sink.SetSubscription(sub);
        return sink;
    }

    /// <summary>Observer that tracks whether the source emitted and swaps in the fallback subscription when it did not.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="fallback">The fallback observable.</param>
    private sealed class SwitchIfEmptySink(
        IObserver<T> downstream,
        IObservable<T> fallback) : IObserver<T>, IDisposable
    {
        /// <summary>The gate for state access.</summary>
        private readonly Lock _gate = new();

        /// <summary>The active subscription, replaced by the fallback's when the source turns out empty.</summary>
        private readonly MutableDisposable _subscription = new();

        /// <summary>Whether the source has emitted a value.</summary>
        private bool _hasValue;

        /// <summary>Whether the sink has completed or been disposed.</summary>
        private bool _done;

        /// <summary>Stores the source subscription so the fallback subscription can take its place.</summary>
        /// <param name="sub">The subscription.</param>
        public void SetSubscription(IDisposable sub) => _subscription.Disposable = sub;

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _hasValue = true;
            }

            downstream.OnNext(value);
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
                downstream.OnError(error);
            }
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                if (_hasValue)
                {
                    _done = true;
                    downstream.OnCompleted();
                }
                else
                {
                    _subscription.Disposable = fallback.Subscribe(downstream);
                }
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _done = true;
                _subscription.Dispose();
            }
        }
    }
}
