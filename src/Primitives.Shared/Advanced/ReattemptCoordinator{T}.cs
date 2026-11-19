// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Coordinates retry-on-error resubscription for <c>Reattempt</c>.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="retryCount">The maximum number of retries.</param>
/// <param name="observer">The downstream observer.</param>
[System.Diagnostics.DebuggerDisplay("ReattemptCoordinator<{typeof(T).Name,nq}>")]
public sealed class ReattemptCoordinator<T>(IObservable<T> source, int retryCount, IObserver<T> observer) : IDisposable
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source = source;

    /// <summary>The maximum number of retries.</summary>
    private readonly int _retryCount = retryCount;

    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer = observer;

    /// <summary>Synchronizes retry state and active subscription handoff.</summary>
    private readonly Lock _gate = new();

    /// <summary>Reusable observer passed to every retry attempt.</summary>
    private AttemptWitness? _witness;

    /// <summary>Active source subscription for the current attempt.</summary>
    private IDisposable? _currentSubscription;

    /// <summary>The number of retries attempted so far.</summary>
    private int _attempts;

    /// <summary>Whether an attempt is currently inside <c>Subscribe</c>.</summary>
    private bool _subscribing;

    /// <summary>Whether the current <c>Subscribe</c> call faulted synchronously and needs another attempt.</summary>
    private bool _pendingRetry;

    /// <summary>Whether the coordinator has been disposed or terminated.</summary>
    private bool _disposed;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose()
    {
        IDisposable? subscription;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            subscription = _currentSubscription;
            _currentSubscription = null;
        }

        subscription?.Dispose();
    }

    /// <summary>Starts the first subscription attempt.</summary>
    /// <returns>The coordinator that owns the subscription cleanup.</returns>
    public ReattemptCoordinator<T> Run()
    {
        SubscribeLoop();
        return this;
    }

    /// <summary>Marks a subscription attempt as starting when the coordinator has not been disposed.</summary>
    /// <returns><see langword="true"/> when the attempt should proceed; otherwise, <see langword="false"/>.</returns>
    internal bool BeginAttempt()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return false;
            }

            _subscribing = true;
            _pendingRetry = false;
            return true;
        }
    }

    /// <summary>Forwards a value unless disposed.</summary>
    /// <param name="value">The value to forward.</param>
    internal void OnNext(T value)
    {
        if (Volatile.Read(ref _disposed))
        {
            return;
        }

        SinkDelivery.Next(_observer, value, this);
    }

    /// <summary>Completes downstream and releases the current subscription.</summary>
    internal void OnCompleted()
    {
        IDisposable? subscription;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            subscription = _currentSubscription;
            _currentSubscription = null;
        }

        try
        {
            _observer.OnCompleted();
        }
        finally
        {
            subscription?.Dispose();
        }
    }

    /// <summary>Retries the subscription, or forwards the error once retries are exhausted.</summary>
    /// <param name="error">The error raised by the source.</param>
    internal void OnError(Exception error)
    {
        IDisposable? previous = null;
        var shouldLoop = false;
        var exhausted = false;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            previous = _currentSubscription;
            _currentSubscription = null;
            if (_attempts < _retryCount)
            {
                _attempts++;
                if (_subscribing)
                {
                    _pendingRetry = true;
                }
                else
                {
                    shouldLoop = true;
                }
            }
            else
            {
                _disposed = true;
                exhausted = true;
            }
        }

        previous?.Dispose();
        if (exhausted)
        {
            _observer.OnError(error);
            return;
        }

        if (shouldLoop)
        {
            SubscribeLoop();
        }
    }

    /// <summary>Subscribes iteratively so synchronous retry storms execute in constant stack space.</summary>
    private void SubscribeLoop()
    {
        var witness = _witness ??= new(this);
        while (BeginAttempt())
        {
            var subscription = _source.Subscribe(witness);
            var retry = CompleteAttempt(subscription, out var releaseNow);
            if (releaseNow)
            {
                subscription.Dispose();
            }

            if (!retry)
            {
                return;
            }
        }
    }

    /// <summary>Finalizes an in-flight subscription attempt after <see cref="IObservable{T}.Subscribe"/> returns.</summary>
    /// <param name="subscription">The subscription returned by the source.</param>
    /// <param name="releaseNow">Receives <see langword="true"/> when <paramref name="subscription"/> must be disposed immediately.</param>
    /// <returns><see langword="true"/> when the attempt faulted synchronously and another retry should run; otherwise, <see langword="false"/>.</returns>
    private bool CompleteAttempt(IDisposable subscription, out bool releaseNow)
    {
        lock (_gate)
        {
            _subscribing = false;
            if (_disposed)
            {
                releaseNow = true;
                return false;
            }

            if (_pendingRetry)
            {
                _pendingRetry = false;
                releaseNow = true;
                return true;
            }

            _currentSubscription = subscription;
            releaseNow = false;
            return false;
        }
    }

    /// <summary>Reusable observer handed to each retry attempt.</summary>
    /// <param name="parent">The owning coordinator.</param>
    private sealed class AttemptWitness(ReattemptCoordinator<T> parent) : IObserver<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => parent.OnNext(value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => parent.OnError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => parent.OnCompleted();
    }
}
