// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>Resource-scoped signal.</summary>
/// <typeparam name="TResource">Resource type.</typeparam>
/// <typeparam name="T">Value type.</typeparam>
internal sealed class UseSignal<TResource, T> : IObservable<T>
    where TResource : IDisposable
{
    /// <summary>Resource factory.</summary>
    private readonly Func<TResource> _resourceFactory;

    /// <summary>Signal factory.</summary>
    private readonly Func<TResource, IObservable<T>> _signalFactory;

    /// <summary>Initializes a new instance of the <see cref="UseSignal{TResource,T}"/> class.</summary>
    /// <param name="resourceFactory">Resource factory.</param>
    /// <param name="signalFactory">Signal factory.</param>
    public UseSignal(Func<TResource> resourceFactory, Func<TResource, IObservable<T>> signalFactory)
    {
        _resourceFactory = resourceFactory;
        _signalFactory = signalFactory;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        if (observer is null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        TResource resource;
        IObservable<T> source;
        try
        {
            resource = _resourceFactory();
            source = _signalFactory(resource) ?? throw new InvalidOperationException("The signal factory returned null.");
        }
        catch (Exception error)
        {
            observer.OnError(error);
            return EmptyDisposable.Instance;
        }

        var sink = new UseObserver(observer, resource);
        try
        {
            sink.SetSubscription(source.Subscribe(sink));
        }
        catch
        {
            sink.Dispose();
            throw;
        }

        return sink;
    }

    /// <summary>Subscription sink that owns the resource and inner subscription.</summary>
    private sealed class UseObserver : IObserver<T>, IDisposable
    {
        /// <summary>Owned resource.</summary>
        private readonly IDisposable? _resource;

        /// <summary>Wrapped observer.</summary>
        private IObserver<T> _observer;

        /// <summary>Inner subscription.</summary>
        private IDisposable? _subscription;

        /// <summary>Non-zero once stopped.</summary>
        private int _stopped;

        /// <summary>Initializes a new instance of the <see cref="UseObserver"/> class.</summary>
        /// <param name="observer">Wrapped observer.</param>
        /// <param name="resource">Owned resource.</param>
        public UseObserver(IObserver<T> observer, TResource resource)
        {
            _observer = observer;
            _resource = resource;
        }

        /// <summary>Assigns the inner subscription.</summary>
        /// <param name="subscription">Inner subscription.</param>
        public void SetSubscription(IDisposable subscription)
        {
            if (subscription is null)
            {
                throw new ArgumentNullException(nameof(subscription));
            }

            if (Interlocked.CompareExchange(ref _subscription, subscription, null) is not null)
            {
                subscription.Dispose();
                return;
            }

            if (Volatile.Read(ref _stopped) == 0)
            {
                return;
            }

            Interlocked.Exchange(ref _subscription, null)?.Dispose();
        }

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            if (Volatile.Read(ref _stopped) != 0)
            {
                return;
            }

            try
            {
                _observer.OnNext(value);
            }
            catch
            {
                Stop();
                throw;
            }
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (error is null)
            {
                throw new ArgumentNullException(nameof(error));
            }

            if (Interlocked.Exchange(ref _stopped, 1) != 0)
            {
                return;
            }

            try
            {
                _observer.OnError(error);
            }
            finally
            {
                Release();
            }
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            if (Interlocked.Exchange(ref _stopped, 1) != 0)
            {
                return;
            }

            try
            {
                _observer.OnCompleted();
            }
            finally
            {
                Release();
            }
        }

        /// <inheritdoc/>
        public void Dispose() => Stop();

        /// <summary>Stops and releases owned resources.</summary>
        private void Stop()
        {
            if (Interlocked.Exchange(ref _stopped, 1) != 0)
            {
                return;
            }

            Release();
        }

        /// <summary>Releases owned resources.</summary>
        private void Release()
        {
            _observer = EmptyWitness<T>.Instance;
            Interlocked.Exchange(ref _subscription, null)?.Dispose();
            _resource?.Dispose();
        }
    }
}
