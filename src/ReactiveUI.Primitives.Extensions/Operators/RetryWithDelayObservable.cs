// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Retries the source observable sequence upon error, with a delay selected by a function.
/// </summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="retryCount">The maximum number of retries.</param>
/// <param name="delaySelector">A function to select the delay for each retry attempt.</param>
internal sealed class RetryWithDelayObservable<T>(
    IObservable<T> source,
    int retryCount,
    Func<int, TimeSpan> delaySelector) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(delaySelector);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var sink = new RetryWithDelaySink(observer, source, retryCount, delaySelector, Sequencer.Default);
        sink.Run();
        return sink;
    }

    /// <summary>
    /// Sink that manages retries with a custom delay selector.
    /// </summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="source">The source observable.</param>
    /// <param name="maxRetries">The maximum number of retries.</param>
    /// <param name="delaySelector">The delay selector.</param>
    /// <param name="scheduler">The scheduler used to time retry delays.</param>
    private sealed class RetryWithDelaySink(
        IObserver<T> downstream,
        IObservable<T> source,
        int maxRetries,
        Func<int, TimeSpan> delaySelector,
        ISequencer scheduler) : IObserver<T>, IDisposable
    {
        /// <summary>The gate for state access.</summary>
        private readonly Lock _gate = new();

        /// <summary>The subscription to the source sequence.</summary>
        private readonly MutableDisposable _subscription = new();

        /// <summary>The number of retries already attempted.</summary>
        private int _retries;

        /// <summary>Whether the sink has been disposed.</summary>
        private bool _disposed;

        /// <summary>Starts the retry process.</summary>
        public void Run() => SubscribeToSource();

        /// <inheritdoc/>
        public void OnNext(T value) => downstream.OnNext(value);

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_disposed)
                {
                    return;
                }

                if (_retries < maxRetries)
                {
                    var delay = delaySelector(_retries);
                    _retries++;

                    if (delay == TimeSpan.Zero)
                    {
                        SubscribeToSource();
                    }
                    else
                    {
                        _subscription.Disposable = scheduler.Schedule(this, delay, static (_, self) =>
                        {
                            self.SubscribeToSource();
                            return EmptyDisposable.Instance;
                        });
                    }
                }
                else
                {
                    downstream.OnError(error);
                }
            }
        }

        /// <inheritdoc/>
        public void OnCompleted() => downstream.OnCompleted();

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _disposed = true;
                _subscription.Dispose();
            }
        }

        /// <summary>Subscribes to the source sequence.</summary>
        private void SubscribeToSource()
        {
            lock (_gate)
            {
                if (_disposed)
                {
                    return;
                }

                _subscription.Disposable = source.Subscribe(this);
            }
        }
    }
}
