// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Operators;

using System;
using System.Threading;

/// <summary>
/// Partitions a sequence into two observables based on a predicate.
/// </summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
internal sealed class PartitionObservable<T>
{
    /// <summary>
    /// The source observable.
    /// </summary>
    private readonly IObservable<T> _source;

    /// <summary>
    /// The predicate to partition elements.
    /// </summary>
    private readonly Func<T, bool> _predicate;

    /// <summary>
    /// The gate for synchronization.
    /// </summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// The source subscription.
    /// </summary>
    private IDisposable? _sourceSubscription;

    /// <summary>
    /// The observer for the source.
    /// </summary>
    private PartitionSink? _sink;

    /// <summary>
    /// The number of subscriptions.
    /// </summary>
    private int _subscriptionCount;

    /// <summary>
    /// Initializes a new instance of the <see cref="PartitionObservable{T}"/> class.
    /// </summary>
    /// <param name="source">The source observable.</param>
    /// <param name="predicate">The predicate to partition elements.</param>
    public PartitionObservable(IObservable<T> source, Func<T, bool> predicate)
    {
        _source = source;
        _predicate = predicate;
        True = new PartitionSide(this, true);
        False = new PartitionSide(this, false);
    }

    /// <summary>Gets the observable emitting elements that satisfy the predicate.</summary>
    public IObservable<T> True { get; }

    /// <summary>Gets the observable emitting elements that do not satisfy the predicate.</summary>
    public IObservable<T> False { get; }

    /// <summary>
    /// Subscribes an observer to the specified side of the partition.
    /// </summary>
    /// <param name="observer">The observer.</param>
    /// <param name="side">The side (true or false).</param>
    /// <returns>A disposable to unsubscribe.</returns>
    private Subscription Subscribe(IObserver<T> observer, bool side)
    {
        lock (_gate)
        {
            if (_subscriptionCount == 0)
            {
                _sink = new(this);
                _sourceSubscription = _source.Subscribe(_sink);
            }

            _subscriptionCount++;
            _sink!.Add(observer, side);
        }

        return new(this, observer, side);
    }

    /// <summary>
    /// Represents a subscription to the partition.
    /// </summary>
    /// <param name="parent">The parent observable.</param>
    /// <param name="observer">The observer.</param>
    /// <param name="side">The side.</param>
    private sealed class Subscription(PartitionObservable<T> parent, IObserver<T> observer, bool side) : IDisposable
    {
        /// <summary>
        /// The parent observable.
        /// </summary>
        private readonly PartitionObservable<T> _parent = parent;

        /// <summary>
        /// The observer.
        /// </summary>
        private readonly IObserver<T> _observer = observer;

        /// <summary>
        /// The side.
        /// </summary>
        private readonly bool _side = side;

        /// <summary>
        /// Whether the subscription is disposed.
        /// </summary>
        private int _disposed;

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            lock (_parent._gate)
            {
                // Invariant: a Subscription whose Interlocked.Exchange just transitioned _disposed
                // from 0 to 1 was created by Subscribe under the same lock, which sets _sink
                // before returning — so _sink is non-null here by construction.
                _parent._sink!.Remove(_observer, _side);
                _parent._subscriptionCount--;
                if (_parent._subscriptionCount == 0)
                {
                    // Subscribe set _sourceSubscription alongside _sink under the same lock,
                    // so when the last branch disposes here it is non-null by construction.
                    _parent._sourceSubscription!.Dispose();
                    _parent._sourceSubscription = null;
                    _parent._sink = null;
                }
            }
        }
    }

    /// <summary>
    /// Represents a side of the partition.
    /// </summary>
    private sealed class PartitionSide : IObservable<T>
    {
        /// <summary>
        /// The parent observable.
        /// </summary>
        private readonly PartitionObservable<T> _parent;

        /// <summary>
        /// The side.
        /// </summary>
        private readonly bool _side;

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionSide"/> class.
        /// </summary>
        /// <param name="parent">The parent observable.</param>
        /// <param name="side">The side (true or false).</param>
        public PartitionSide(PartitionObservable<T> parent, bool side)
        {
            _parent = parent;
            _side = side;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer) => _parent.Subscribe(observer, _side);
    }

    /// <summary>
    /// Sink that partitions elements.
    /// </summary>
    /// <param name="parent">The parent observable.</param>
    private sealed class PartitionSink(PartitionObservable<T> parent) : IObserver<T>
    {
        /// <summary>
        /// The observers for the true side.
        /// </summary>
        private IObserver<T>[] _trueObservers = [];

        /// <summary>
        /// The observers for the false side.
        /// </summary>
        private IObserver<T>[] _falseObservers = [];

        /// <summary>
        /// Adds an observer to the specified side.
        /// </summary>
        /// <param name="observer">The observer to add.</param>
        /// <param name="side">The side.</param>
        public void Add(IObserver<T> observer, bool side)
        {
            if (side)
            {
                _trueObservers = [.. _trueObservers, observer];
            }
            else
            {
                _falseObservers = [.. _falseObservers, observer];
            }
        }

        /// <summary>
        /// Removes an observer from the specified side.
        /// </summary>
        /// <param name="observer">The observer to remove.</param>
        /// <param name="side">The side.</param>
        public void Remove(IObserver<T> observer, bool side)
        {
            if (side)
            {
                var index = Array.IndexOf(_trueObservers, observer);
                if (index >= 0)
                {
                    if (_trueObservers.Length == 1)
                    {
                        _trueObservers = [];
                    }
                    else
                    {
                        var newObservers = new IObserver<T>[_trueObservers.Length - 1];
                        Array.Copy(_trueObservers, 0, newObservers, 0, index);
                        Array.Copy(_trueObservers, index + 1, newObservers, index, _trueObservers.Length - index - 1);
                        _trueObservers = newObservers;
                    }
                }
            }
            else
            {
                var index = Array.IndexOf(_falseObservers, observer);
                if (index >= 0)
                {
                    if (_falseObservers.Length == 1)
                    {
                        _falseObservers = [];
                    }
                    else
                    {
                        var newObservers = new IObserver<T>[_falseObservers.Length - 1];
                        Array.Copy(_falseObservers, 0, newObservers, 0, index);
                        Array.Copy(_falseObservers, index + 1, newObservers, index, _falseObservers.Length - index - 1);
                        _falseObservers = newObservers;
                    }
                }
            }
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            IObserver<T>[] trueObservers;
            IObserver<T>[] falseObservers;

            lock (parent._gate)
            {
                trueObservers = _trueObservers;
                falseObservers = _falseObservers;
            }

            for (var i = 0; i < trueObservers.Length; i++)
            {
                trueObservers[i].OnCompleted();
            }

            for (var i = 0; i < falseObservers.Length; i++)
            {
                falseObservers[i].OnCompleted();
            }
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            IObserver<T>[] trueObservers;
            IObserver<T>[] falseObservers;

            lock (parent._gate)
            {
                trueObservers = _trueObservers;
                falseObservers = _falseObservers;
            }

            for (var i = 0; i < trueObservers.Length; i++)
            {
                trueObservers[i].OnError(error);
            }

            for (var i = 0; i < falseObservers.Length; i++)
            {
                falseObservers[i].OnError(error);
            }
        }

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            var result = parent._predicate(value);

            IObserver<T>[] observers;
            lock (parent._gate)
            {
                observers = result ? _trueObservers : _falseObservers;
            }

            for (var i = 0; i < observers.Length; i++)
            {
                observers[i].OnNext(value);
            }
        }
    }
}
