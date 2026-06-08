// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Re-subscribes to the source indefinitely on error. Forwards values verbatim and completes when
/// the source completes. Per-subscription state is held in a single sink; resubscription swaps the
/// inner disposable rather than allocating a new wrapper chain.
/// </summary>
/// <typeparam name="T">Element type.</typeparam>
/// <param name="source">Upstream source.</param>
internal sealed class RetryForeverObservable<T>(IObservable<T> source) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        var sink = new RetrySink(source, observer);
        sink.Start();
        return sink;
    }

    /// <summary>Sink that re-subscribes the source on every error.</summary>
    /// <param name="source">The source to re-subscribe on error.</param>
    /// <param name="downstream">The downstream observer.</param>
    private sealed class RetrySink(IObservable<T> source, IObserver<T> downstream) : IObserver<T>, IDisposable
    {
        /// <summary>Holds the current inner subscription; swapped on each resubscribe.</summary>
        private readonly MutableDisposable _inner = new();

        /// <summary>Latches to <c>1</c> on the first dispose so teardown is idempotent.</summary>
        private int _disposed;

        /// <summary>Begins the first subscription.</summary>
        public void Start() => _inner.Disposable = source.Subscribe(this);

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                return;
            }

            downstream.OnNext(value);
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                return;
            }

            _ = error;
            _inner.Disposable = source.Subscribe(this);
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                return;
            }

            downstream.OnCompleted();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _inner.Dispose();
        }
    }
}
