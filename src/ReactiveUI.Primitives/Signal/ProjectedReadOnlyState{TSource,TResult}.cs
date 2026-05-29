// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

#pragma warning disable S3366 // The source subscription synchronously replays into this fully initialized projection state.

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Read-only state projection backed directly by a source state signal.
/// </summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TResult">The projected value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("{Value}")]
public sealed class ProjectedReadOnlyState<TSource, TResult> : IObservable<TResult>, IObserver<TSource>, IDisposable
{
    /// <summary>
    /// Projection function.
    /// </summary>
    private readonly Func<TSource, TResult> _selector;

    /// <summary>
    /// Source subscription.
    /// </summary>
    private readonly IDisposable _subscription;

#pragma warning disable S3459 // Broadcaster<T> is a mutable struct whose default value is the empty broadcaster.
    /// <summary>
    /// Current subscribers.
    /// </summary>
    private Broadcaster<TResult> _broadcaster;
#pragma warning restore S3459

    /// <summary>
    /// Protects mutable state and subscriptions.
    /// </summary>
    private SpinLock _gate = new(false);

    /// <summary>
    /// Last projected value.
    /// </summary>
    private TResult? _lastValue;

    /// <summary>
    /// Last terminal error.
    /// </summary>
    private Exception? _lastError;

    /// <summary>
    /// Set after at least one value has been projected.
    /// </summary>
    private bool _hasValue;

    /// <summary>
    /// Non-zero after terminal or disposal.
    /// </summary>
    private bool _isStopped;

    /// <summary>
    /// Non-zero after disposal.
    /// </summary>
    private bool _isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="ProjectedReadOnlyState{TSource,TResult}"/> class.
    /// </summary>
    /// <param name="source">Source state.</param>
    /// <param name="selector">Projection function.</param>
    public ProjectedReadOnlyState(StateSignal<TSource> source, Func<TSource, TResult> selector)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        _selector = selector ?? throw new ArgumentNullException(nameof(selector));
        _subscription = source.Subscribe(this);
        _lastError.Rethrow();
        if (_hasValue)
        {
            return;
        }

        _lastValue = _selector(source.Value);
        _hasValue = true;
    }

    /// <summary>
    /// Gets the current projected value.
    /// </summary>
    public TResult Value
    {
        get
        {
            ThrowIfDisposed();
            _lastError.Rethrow();
            return _lastValue!;
        }
    }

    /// <summary>
    /// Gets the stream of current and subsequent values.
    /// </summary>
    public IObservable<TResult> Changed => this;

    /// <inheritdoc/>
    public void OnCompleted()
    {
        var lockTaken = false;
        try
        {
            _gate.Enter(ref lockTaken);
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            _isStopped = true;
        }
        finally
        {
            if (lockTaken)
            {
                _gate.Exit(false);
            }
        }

        _broadcaster.Completed();
        _broadcaster.Clear();
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        if (error == null)
        {
            throw new ArgumentNullException(nameof(error));
        }

        var lockTaken = false;
        try
        {
            _gate.Enter(ref lockTaken);
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            _isStopped = true;
            _lastError = error;
        }
        finally
        {
            if (lockTaken)
            {
                _gate.Exit(false);
            }
        }

        _broadcaster.Error(error);
        _broadcaster.Clear();
    }

    /// <inheritdoc/>
    public void OnNext(TSource value)
    {
        TResult result;
        try
        {
            result = _selector(value);
        }
        catch (Exception error)
        {
            OnError(error);
            return;
        }

        if (Volatile.Read(ref _isStopped))
        {
            return;
        }

        _lastValue = result;
        Volatile.Write(ref _hasValue, true);
        _broadcaster.Next(result);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        Exception? error;
        TResult? value;
        var stopped = false;
        var lockTaken = false;
        try
        {
            _gate.Enter(ref lockTaken);
            ThrowIfDisposed();
            error = _lastError;
            value = _lastValue;
            stopped = _isStopped;
            if (!stopped)
            {
                _broadcaster.Add(observer);
            }
        }
        finally
        {
            if (lockTaken)
            {
                _gate.Exit(false);
            }
        }

        if (error != null)
        {
            observer.OnError(error);
            return Disposable.Empty;
        }

        observer.OnNext(value!);
        if (stopped)
        {
            observer.OnCompleted();
            return Disposable.Empty;
        }

        return new Subscription(this, observer);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_isDisposed)
        {
            return;
        }

        _subscription.Dispose();
        var lockTaken = false;
        try
        {
            _gate.Enter(ref lockTaken);
            _broadcaster.Clear();
            _lastError = null;
            _lastValue = default;
            _isStopped = true;
            _isDisposed = true;
        }
        finally
        {
            if (lockTaken)
            {
                _gate.Exit(false);
            }
        }
    }

    /// <summary>
    /// Removes a subscriber.
    /// </summary>
    /// <param name="observer">Observer to remove.</param>
    private void Remove(IObserver<TResult> observer)
    {
        var lockTaken = false;
        try
        {
            _gate.Enter(ref lockTaken);
            _broadcaster.Remove(observer);
        }
        finally
        {
            if (lockTaken)
            {
                _gate.Exit(false);
            }
        }
    }

    /// <summary>
    /// Throws if disposed.
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (!_isDisposed)
        {
            return;
        }

        throw new ObjectDisposedException(nameof(ProjectedReadOnlyState<TSource, TResult>));
    }

    /// <summary>
    /// Projection subscription.
    /// </summary>
    private sealed class Subscription : IDisposable
    {
        /// <summary>
        /// Parent projection.
        /// </summary>
        private ProjectedReadOnlyState<TSource, TResult>? _parent;

        /// <summary>
        /// Observer to remove.
        /// </summary>
        private IObserver<TResult>? _observer;

        /// <summary>
        /// Initializes a new instance of the <see cref="Subscription"/> class.
        /// </summary>
        /// <param name="parent">Parent projection.</param>
        /// <param name="observer">Observer to remove.</param>
        public Subscription(ProjectedReadOnlyState<TSource, TResult> parent, IObserver<TResult> observer)
        {
            _parent = parent;
            _observer = observer;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            var parent = Interlocked.Exchange(ref _parent, null);
            var observer = Interlocked.Exchange(ref _observer, null);
            if (parent == null || observer == null)
            {
                return;
            }

            parent.Remove(observer);
        }
    }
}
