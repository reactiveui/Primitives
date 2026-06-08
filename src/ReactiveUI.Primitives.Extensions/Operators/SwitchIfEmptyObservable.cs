// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Provides a fallback observable if the source sequence completes without emitting any elements.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="fallback">The fallback observable.</param>
internal sealed class SwitchIfEmptyObservable<T>(
    IObservable<T> source,
    IObservable<T> fallback) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(fallback);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var sink = new SwitchIfEmptySink(observer, fallback);
        var sub = source.Subscribe(sink);
        sink.SetSubscription(sub);
        return sink;
    }

    /// <summary>The sink for the <see cref="SwitchIfEmptyObservable{T}"/>.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="fallback">The fallback observable.</param>
    private sealed class SwitchIfEmptySink(
        IObserver<T> downstream,
        IObservable<T> fallback) : IObserver<T>, IDisposable
    {
        /// <summary>The gate for state access.</summary>
        private readonly Lock _gate = new();

        /// <summary>The current subscription.</summary>
        private readonly MutableDisposable _subscription = new();

        /// <summary>Whether the source has emitted a value.</summary>
        private bool _hasValue;

        /// <summary>Whether the sink has completed or been disposed.</summary>
        private bool _done;

        /// <summary>Sets the subscription to the source observable.</summary>
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
                    // Source was empty, switch to fallback
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
