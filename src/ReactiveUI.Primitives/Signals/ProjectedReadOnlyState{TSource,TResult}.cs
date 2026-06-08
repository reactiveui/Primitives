// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Read-only state projection backed directly by a source state signal.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TResult">The projected value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("{Value}")]
public sealed class ProjectedReadOnlyState<TSource, TResult> : IObservable<TResult>, IObserver<TSource>, IDisposable
{
    /// <summary>Projection function.</summary>
    private readonly Func<TSource, TResult> _selector;

    /// <summary>Protects mutable state and subscriptions.</summary>
    private readonly Lock _gate = new();

    /// <summary>Source subscription, assigned by the factory after construction.</summary>
    private IDisposable? _subscription;

    /// <summary>Current subscribers.</summary>
    private Broadcaster<TResult> _broadcaster;

    /// <summary>Last projected value.</summary>
    private TResult? _lastValue;

    /// <summary>Last terminal error.</summary>
    private Exception? _lastError;

    /// <summary>Set after at least one value has been projected.</summary>
    private bool _hasValue;

    /// <summary>Non-zero after terminal or disposal.</summary>
    private bool _isStopped;

    /// <summary>Non-zero after disposal.</summary>
    private bool _isDisposed;

    /// <summary>Initializes a new instance of the <see cref="ProjectedReadOnlyState{TSource,TResult}"/> class.</summary>
    /// <param name="selector">Projection function.</param>
    private ProjectedReadOnlyState(Func<TSource, TResult> selector)
    {
        _selector = selector;
        _broadcaster = default;
    }

    /// <summary>Gets the current projected value.</summary>
    public TResult Value
    {
        get
        {
            ThrowIfDisposed();
            _lastError.Rethrow();
            return _lastValue!;
        }
    }

    /// <summary>Gets the stream of current and subsequent values.</summary>
    public IObservable<TResult> Changed => this;

    /// <summary>
    /// Creates a projected read-only state and subscribes it to the source after construction, so the
    /// instance is never exposed to the source while partially constructed.
    /// </summary>
    /// <param name="source">The source state signal.</param>
    /// <param name="selector">The projection applied to each source value.</param>
    /// <returns>The fully-initialized projected read-only state.</returns>
    public static ProjectedReadOnlyState<TSource, TResult> Create(StateSignal<TSource> source, Func<TSource, TResult> selector)
    {
        if (source is null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (selector is null)
        {
            throw new ArgumentNullException(nameof(selector));
        }

        var state = new ProjectedReadOnlyState<TSource, TResult>(selector);
        state._subscription = source.Subscribe(state);
        state._lastError.Rethrow();
        if (!state._hasValue)
        {
            state._lastValue = selector(source.Value);
            state._hasValue = true;
        }

        return state;
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            _isStopped = true;
        }

        _broadcaster.Completed();
        _broadcaster.Clear();
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        if (error is null)
        {
            throw new ArgumentNullException(nameof(error));
        }

        lock (_gate)
        {
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            _isStopped = true;
            _lastError = error;
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
        if (observer is null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        Exception? error;
        TResult? value;
        bool stopped;
        lock (_gate)
        {
            ThrowIfDisposed();
            error = _lastError;
            value = _lastValue;
            stopped = _isStopped;
            if (!stopped)
            {
                _broadcaster.Add(observer);
            }
        }

        if (error is not null)
        {
            observer.OnError(error);
            return EmptyDisposable.Instance;
        }

        observer.OnNext(value!);
        if (stopped)
        {
            observer.OnCompleted();
            return EmptyDisposable.Instance;
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

        _subscription?.Dispose();
        lock (_gate)
        {
            _broadcaster.Clear();
            _lastError = null;
            _lastValue = default;
            _isStopped = true;
            _isDisposed = true;
        }
    }

    /// <summary>Removes a subscriber.</summary>
    /// <param name="observer">Observer to remove.</param>
    private void Remove(IObserver<TResult> observer)
    {
        lock (_gate)
        {
            _broadcaster.Remove(observer);
        }
    }

    /// <summary>Throws if disposed.</summary>
    private void ThrowIfDisposed()
    {
        if (!_isDisposed)
        {
            return;
        }

        throw new ObjectDisposedException(nameof(ProjectedReadOnlyState<TSource, TResult>));
    }

    /// <summary>Projection subscription.</summary>
    private sealed class Subscription : IDisposable
    {
        /// <summary>Parent projection.</summary>
        private ProjectedReadOnlyState<TSource, TResult>? _parent;

        /// <summary>Observer to remove.</summary>
        private IObserver<TResult>? _observer;

        /// <summary>Initializes a new instance of the <see cref="Subscription"/> class.</summary>
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
            if (parent is null || observer is null)
            {
                return;
            }

            parent.Remove(observer);
        }
    }
}
