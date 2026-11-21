// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Read-only state projection backed directly by a source state signal.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TResult">The projected value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ProjectedReadOnlyState: {Value}")]
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
            lock (_gate)
            {
                ThrowIfDisposed();
                _lastError.Rethrow();
                return _lastValue!;
            }
        }
    }

    /// <summary>Gets the stream of current and subsequent values.</summary>
    public IObservable<TResult> Changed => this;

    /// <summary>Creates a projected read-only state and subscribes it to the source after construction, so the instance is never exposed to the source while partially constructed.</summary>
    /// <param name="source">The source state signal.</param>
    /// <param name="selector">The projection applied to each source value.</param>
    /// <returns>The fully-initialized projected read-only state.</returns>
    public static ProjectedReadOnlyState<TSource, TResult> Create(
        StateSignal<TSource> source,
        Func<TSource, TResult> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        ArgumentExceptionHelper.ThrowIfNull(selector);

        ProjectedReadOnlyState<TSource, TResult> state = new(selector);
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

        Interlocked.Exchange(ref _subscription, null)?.Dispose();
        _broadcaster.Completed();
        _broadcaster.Clear();
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);

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

        Interlocked.Exchange(ref _subscription, null)?.Dispose();
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
        catch (Exception error) when (!CoreFatalExceptionHelper.IsFatal(error))
        {
            OnError(error);
            return;
        }

        lock (_gate)
        {
            if (_isStopped)
            {
                return;
            }

            _lastValue = result;
            _hasValue = true;
        }

        _broadcaster.Next(result);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        Exception? error;
        TResult? value;
        DetachableObserver<ProjectedReadOnlyState<TSource, TResult>, TResult>? subscription = null;
        lock (_gate)
        {
            ThrowIfDisposed();
            error = _lastError;
            value = _lastValue;
            if (!_isStopped)
            {
                subscription = new(this, observer, static (parent, target) => parent.Remove(target));
                _broadcaster.Add(subscription);
            }
        }

        if (error is not null)
        {
            observer.OnError(error);
            return EmptyDisposable.Instance;
        }

        observer.OnNext(value!);
        if (subscription is null)
        {
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        return subscription;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (Volatile.Read(ref _isDisposed))
        {
            return;
        }

        DisposeCore();
    }

    /// <summary>Marks the state disposed under <see cref="_gate"/> and releases the upstream subscription.</summary>
    internal void DisposeCore()
    {
        lock (_gate)
        {
            if (_isDisposed)
            {
                return;
            }

            Volatile.Write(ref _isDisposed, true);
            _isStopped = true;
            _broadcaster.Clear();
            _lastError = null;
            _lastValue = default;
        }

        Interlocked.Exchange(ref _subscription, null)?.Dispose();
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
    /// <exception cref="ObjectDisposedException">The projection is disposed.</exception>
    private void ThrowIfDisposed()
    {
        if (!_isDisposed)
        {
            return;
        }

        throw new ObjectDisposedException(nameof(ProjectedReadOnlyState<,>));
    }
}
