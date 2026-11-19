// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Coordinates chain-style FlatMap subscriptions.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("FlatMapCoordinator: Gate = {_gate}")]
public sealed class FlatMapCoordinator<TSource, TResult> : IObserver<TSource>, IDisposable
{
    /// <summary>Synchronizes subscription state.</summary>
    private readonly Lock _gate = new();

    /// <summary>The source observable.</summary>
    private readonly IObservable<TSource> _source;

    /// <summary>Projects source values to inner observables.</summary>
    private readonly Func<TSource, IObservable<TResult>> _selector;

    /// <summary>The downstream observer.</summary>
    private readonly IObserver<TResult> _observer;

    /// <summary>Observer used for active inner sources.</summary>
    private readonly InnerWitness _innerObserver;

    /// <summary>Queued inner sources waiting for the active inner source to complete.</summary>
    private Queue<IObservable<TResult>>? _queue;

    /// <summary>Outer subscription.</summary>
    private IDisposable? _outer;

    /// <summary>Active inner subscription.</summary>
    private IDisposable? _inner;

    /// <summary>Value indicating whether an inner source is active.</summary>
    private bool _active;

    /// <summary>Value indicating whether the outer source has completed.</summary>
    private bool _outerCompleted;

    /// <summary>Value indicating whether the coordinator has stopped.</summary>
    private bool _disposed;

    /// <summary>Value indicating whether the active inner source is currently subscribing.</summary>
    private bool _subscribingInner;

    /// <summary>Value indicating whether the active inner source completed during its own subscribe call.</summary>
    private bool _completedInnerWhileSubscribing;

    /// <summary>Initializes a new instance of the <see cref="FlatMapCoordinator{TSource, TResult}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="selector">Projects source values to inner observables.</param>
    /// <param name="observer">The downstream observer.</param>
    [SuppressMessage(
        "Correctness",
        "SST2403:Do not let 'this' escape from a constructor",
        Justification = "The witnesses are this coordinator's own fields and nothing notifies them until Run subscribes.")]
    public FlatMapCoordinator(
        IObservable<TSource> source,
        Func<TSource, IObservable<TResult>> selector,
        IObserver<TResult> observer)
    {
        _source = source;
        _selector = selector;
        _observer = observer;
        _innerObserver = new(this);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void IObserver<TSource>.OnNext(TSource value) => OnOuterNext(value);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void IObserver<TSource>.OnError(Exception error) => OnError(error);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void IObserver<TSource>.OnCompleted() => OnOuterCompleted();

    /// <inheritdoc/>
    public void Dispose()
    {
        IDisposable? outer;
        IDisposable? inner;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            outer = _outer;
            inner = _inner;
            _outer = null;
            _inner = null;
            _queue = null;
        }

        outer?.Dispose();
        inner?.Dispose();
    }

    /// <summary>Subscribes to the outer source.</summary>
    /// <returns>The subscription cleanup.</returns>
    internal IDisposable Run()
    {
        var outer = _source.Subscribe(this);
        lock (_gate)
        {
            if (_disposed)
            {
                outer.Dispose();
                return EmptyDisposable.Instance;
            }

            _outer = outer;
            return this;
        }
    }

    /// <summary>Subscribes to an inner source and drains any synchronously completed successors iteratively.</summary>
    /// <param name="inner">The initial inner source.</param>
    internal void SubscribeInner(IObservable<TResult> inner)
    {
        var current = inner;
        while (BeginSubscribe())
        {
            IDisposable subscription;
            try
            {
                subscription = current.Subscribe(_innerObserver);
            }
            catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
            {
                CompleteSubscribe(error);
                return;
            }

            if (!CompleteSubscribe(subscription, out var disposeNow))
            {
                if (disposeNow)
                {
                    subscription.Dispose();
                }

                return;
            }

            subscription.Dispose();
            if (!TryDequeueOrComplete(out var next))
            {
                return;
            }

            current = next!;
        }
    }

    /// <summary>Marks an inner subscribe call as starting when the coordinator is still active.</summary>
    /// <returns><c>true</c> when the coordinator is active; otherwise, <c>false</c>.</returns>
    private bool BeginSubscribe()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return false;
            }

            _subscribingInner = true;
            _completedInnerWhileSubscribing = false;
            return true;
        }
    }

    /// <summary>Dequeues the next waiting inner source or completes the downstream observer when done.</summary>
    /// <param name="next">The next inner source to subscribe, when available.</param>
    /// <returns><c>true</c> when another inner source was dequeued; otherwise, <c>false</c>.</returns>
    private bool TryDequeueOrComplete(out IObservable<TResult>? next)
    {
        next = null;
        IDisposable? outer = null;
        var complete = false;
        lock (_gate)
        {
            if (_disposed || _active)
            {
                return false;
            }

            if (_queue is { Count: > 0 } queue)
            {
                _active = true;
                next = queue.Dequeue();
                return true;
            }

            if (_outerCompleted)
            {
                _disposed = true;
                outer = _outer;
                _outer = null;
                complete = true;
            }
        }

        if (complete)
        {
            try
            {
                _observer.OnCompleted();
            }
            finally
            {
                outer?.Dispose();
            }
        }

        return false;
    }

    /// <summary>Handles a source value.</summary>
    /// <param name="value">The source value.</param>
    /// <exception cref="InvalidOperationException">The selector returned a <see langword="null"/> inner observable.</exception>
    private void OnOuterNext(TSource value)
    {
        IObservable<TResult> inner;
        try
        {
            inner = _selector(value) ?? throw new InvalidOperationException("The FlatMap selector returned null.");
        }
        catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
        {
            OnError(error);
            return;
        }

        if (!TryStartOrQueue(inner))
        {
            return;
        }

        SubscribeInner(inner);
    }

    /// <summary>Handles outer source completion.</summary>
    private void OnOuterCompleted()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _outerCompleted = true;
        }

        Drain();
    }

    /// <summary>Forwards an inner value.</summary>
    /// <param name="value">The inner value.</param>
    private void OnInnerNext(TResult value)
    {
        if (Volatile.Read(ref _disposed))
        {
            return;
        }

        _observer.OnNext(value);
    }

    /// <summary>Handles active inner source completion.</summary>
    private void OnInnerCompleted()
    {
        IDisposable? inner;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            if (_subscribingInner)
            {
                _completedInnerWhileSubscribing = true;
                return;
            }

            inner = _inner;
            _inner = null;
            _active = false;
        }

        inner?.Dispose();
        Drain();
    }

    /// <summary>Handles an outer or inner error.</summary>
    /// <param name="error">The error.</param>
    private void OnError(Exception error)
    {
        IDisposable? outer;
        IDisposable? inner;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            outer = _outer;
            inner = _inner;
            _outer = null;
            _inner = null;
            _queue = null;
        }

        outer?.Dispose();
        inner?.Dispose();
        _observer.OnError(error);
    }

    /// <summary>Starts an inner source immediately or queues it behind the active inner source.</summary>
    /// <param name="inner">The inner source.</param>
    /// <returns><c>true</c> when the source should be subscribed immediately; otherwise, <c>false</c>.</returns>
    private bool TryStartOrQueue(IObservable<TResult> inner)
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return false;
            }

            if (!_active)
            {
                _active = true;
                return true;
            }

            (_queue ??= new()).Enqueue(inner);
            return false;
        }
    }

    /// <summary>Completes an inner subscribe call that threw.</summary>
    /// <param name="error">The subscribe error.</param>
    private void CompleteSubscribe(Exception error)
    {
        lock (_gate)
        {
            _subscribingInner = false;
            _active = false;
        }

        OnError(error);
    }

    /// <summary>Completes an inner subscribe call.</summary>
    /// <param name="subscription">The inner subscription.</param>
    /// <param name="disposeNow">Receives <c>true</c> when <paramref name="subscription"/> must be disposed immediately.</param>
    /// <returns><c>true</c> when the inner source completed synchronously and the loop should advance; otherwise, <c>false</c>.</returns>
    private bool CompleteSubscribe(IDisposable subscription, out bool disposeNow)
    {
        lock (_gate)
        {
            _subscribingInner = false;
            if (_disposed)
            {
                disposeNow = true;
                return false;
            }

            if (_completedInnerWhileSubscribing)
            {
                _completedInnerWhileSubscribing = false;
                _active = false;
                disposeNow = true;
                return true;
            }

            _inner = subscription;
            disposeNow = false;
            return false;
        }
    }

    /// <summary>Drains queued inner sources and completes when the outer source and queue are finished.</summary>
    private void Drain()
    {
        if (TryDequeueOrComplete(out var next))
        {
            SubscribeInner(next!);
        }
    }

    /// <summary>Inner source observer.</summary>
    private sealed class InnerWitness : IObserver<TResult>
    {
        /// <summary>Owning coordinator.</summary>
        private readonly FlatMapCoordinator<TSource, TResult> _parent;

        /// <summary>Initializes a new instance of the <see cref="InnerWitness"/> class.</summary>
        /// <param name="parent">Owning coordinator.</param>
        internal InnerWitness(FlatMapCoordinator<TSource, TResult> parent) => _parent = parent;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => _parent.OnInnerCompleted();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => _parent.OnError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(TResult value) => _parent.OnInnerNext(value);
    }
}
