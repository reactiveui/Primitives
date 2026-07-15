// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>Retries the source observable sequence upon error, with optional delay, retry count, and backoff.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="policy">The retry / backoff configuration.</param>
internal sealed class RetryWithBackoffObservable<T>(
    IObservable<T> source,
    RetryBackoffPolicy policy) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(policy.Scheduler);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        RetryWithBackoffSink sink = new(observer, source, policy);
        sink.Run();
        return sink;
    }

    /// <summary>Sink that manages retries with exponential backoff.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="source">The source observable.</param>
    /// <param name="policy">The retry / backoff configuration.</param>
    private sealed class RetryWithBackoffSink(
        IObserver<T> downstream,
        IObservable<T> source,
        RetryBackoffPolicy policy) : IObserver<T>, IDisposable
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
            policy.OnError?.Invoke(error);

            lock (_gate)
            {
                if (_disposed)
                {
                    return;
                }

                if (_retries < policy.MaxRetries)
                {
                    var delay = TimeSpan.FromTicks((long)(policy.InitialDelay.Ticks *
                                                          Math.Pow(policy.BackoffFactor, _retries)));
                    if (policy.MaxDelay.HasValue && delay > policy.MaxDelay.Value)
                    {
                        delay = policy.MaxDelay.Value;
                    }

                    _retries++;

                    if (delay == TimeSpan.Zero)
                    {
                        SubscribeToSource();
                    }
                    else
                    {
                        _subscription.Disposable = policy.Scheduler.Schedule(delay, SubscribeToSource);
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
