// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals;

/// <summary>A signal that exposes its next value as an awaitable operation.</summary>
/// <typeparam name="T">The Type.</typeparam>
/// <seealso cref="ISignal&lt;T&gt;" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public class AsyncSignal<T> : IAwaitSignal<T>
{
    /// <summary>Executes the new operation.</summary>
    /// <returns>The result.</returns>
    private readonly Lock _observerLock = new();

    /// <summary>Stores state for the signal implementation.</summary>
    private T? _lastValue;

    /// <summary>Stores state for the signal implementation.</summary>
    private bool _hasValue;

    /// <summary>Stores state for the signal implementation.</summary>
    private Exception? _lastError;

    /// <summary>Stores state for the signal implementation.</summary>
    private IObserver<T> _outObserver = EmptyWitness<T>.Instance;

    /// <summary>Gets a value indicating whether this instance is disposed.</summary>
    /// <value>
    ///   <c>true</c> if this instance is disposed; otherwise, <c>false</c>.
    /// </value>
    public bool IsDisposed { get; private set; }

    /// <summary>Gets the value.</summary>
    /// <value>
    /// The value.
    /// </value>
    /// <exception cref="InvalidOperationException">The final signal is not completed yet.</exception>
    public T Value
    {
        get
        {
            ThrowIfDisposed();
            if (!IsCompleted)
            {
                throw new InvalidOperationException("FinalSignal is not completed yet");
            }

            _lastError.Rethrow();

            return _lastValue!;
        }
    }

    /// <summary>Gets a value indicating whether this instance has observers.</summary>
    /// <value>
    ///   <c>true</c> if this instance has observers; otherwise, <c>false</c>.
    /// </value>
    public bool HasObservers => _outObserver is not EmptyWitness<T> && !IsCompleted && !IsDisposed;

    /// <summary>Gets a value indicating whether this instance is completed.</summary>
    /// <value>
    ///   <c>true</c> if this instance is completed; otherwise, <c>false</c>.
    /// </value>
    public bool IsCompleted { get; private set; }

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Called when [completed].</summary>
    public void OnCompleted()
    {
        IObserver<T> observers;
        T? completedValue;
        bool hasCompletedValue;
        lock (_observerLock)
        {
            ThrowIfDisposed();
            if (IsCompleted)
            {
                return;
            }

            observers = _outObserver;
            _outObserver = EmptyWitness<T>.Instance;
            IsCompleted = true;
            completedValue = _lastValue;
            hasCompletedValue = _hasValue;
        }

        if (hasCompletedValue)
        {
            observers.OnNext(completedValue!);
            observers.OnCompleted();
        }
        else
        {
            observers.OnCompleted();
        }
    }

    /// <summary>Specifies a callback action that will be invoked when the subject completes.</summary>
    /// <param name="continuation">Callback action that will be invoked when the subject completes.</param>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="continuation"/> is null.</exception>
    public void OnCompleted(Action continuation)
    {
        ArgumentExceptionHelper.ThrowIfNull(continuation);

        SubscribeCompletion(continuation, true);
    }

    /// <summary>Called when [error].</summary>
    /// <param name="error">The error.</param>
    /// <exception cref="ArgumentExceptionHelper">error.</exception>
    public void OnError(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);

        IObserver<T> observers;
        lock (_observerLock)
        {
            ThrowIfDisposed();
            if (IsCompleted)
            {
                return;
            }

            observers = _outObserver;
            _outObserver = EmptyWitness<T>.Instance;
            IsCompleted = true;
            _lastError = error;
        }

        observers.OnError(error);
    }

    /// <summary>Called when [next].</summary>
    /// <param name="value">The value.</param>
    public void OnNext(T value)
    {
        lock (_observerLock)
        {
            ThrowIfDisposed();
            if (IsCompleted)
            {
                return;
            }

            _hasValue = true;
            _lastValue = value;
        }
    }

    /// <summary>Subscribes the specified observer.</summary>
    /// <param name="observer">The observer.</param>
    /// <returns>A Disposable.</returns>
    /// <exception cref="ArgumentExceptionHelper">observer.</exception>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        Exception? completionError;
        T? terminalValue;
        bool hasTerminalValue;

        lock (_observerLock)
        {
            ThrowIfDisposed();
            if (!IsCompleted)
            {
                if (_outObserver is ListWitness<T> listObserver)
                {
                    _outObserver = listObserver.Add(observer);
                }
                else
                {
                    var current = _outObserver;
                    _outObserver = current is EmptyWitness<T> ? new ListWitness<T>(new([observer])) : new ListWitness<T>(new([current, observer]));
                }

                return new ObserverHandler<T>(this, observer);
            }

            completionError = _lastError;
            terminalValue = _lastValue;
            hasTerminalValue = _hasValue;
        }

        if (completionError is not null)
        {
            observer.OnError(completionError);
        }
        else if (hasTerminalValue)
        {
            observer.OnNext(terminalValue!);
            observer.OnCompleted();
        }
        else
        {
            observer.OnCompleted();
        }

        return EmptyDisposable.Instance;
    }

    /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    /// <summary>Gets an awaitable object for the current final signal.</summary>
    /// <returns>Object that can be awaited.</returns>
    public IAwaitSignal<T> GetAwaiter() => this;

    /// <summary>Gets the last element of the subject, potentially blocking until the subject completes successfully or exceptionally.</summary>
    /// <returns>The last element of the subject. Throws an InvalidOperationException if no element was received.</returns>
    /// <exception cref="InvalidOperationException">The source sequence is empty.</exception>
    public T GetResult()
    {
        if (!IsCompleted)
        {
            ManualResetEvent completionEvent = new(false);
            SubscribeCompletion(() => completionEvent.Set(), false);
            completionEvent.WaitOne();
        }

        _lastError.Rethrow();

        if (!_hasValue)
        {
            throw new InvalidOperationException("NO_ELEMENTS");
        }

        return _lastValue!;
    }

    /// <summary>Removes an observer previously registered via <see cref="Subscribe"/>. Called by the observer's subscription handle when it is disposed.</summary>
    /// <param name="observer">The observer to remove.</param>
    internal void RemoveObserver(IObserver<T> observer)
    {
        lock (_observerLock)
        {
            _outObserver = _outObserver is ListWitness<T> listObserver
                ? listObserver.Remove(observer)
                : EmptyWitness<T>.Instance;
        }
    }

    /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
    /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (IsDisposed)
        {
            return;
        }

        if (disposing)
        {
            lock (_observerLock)
            {
                _outObserver = DisposedWitness<T>.Instance;
                _lastError = null;
                _lastValue = default;
            }
        }

        IsDisposed = true;
    }

    /// <summary>Executes the ThrowIfDisposed operation.</summary>
    private void ThrowIfDisposed()
    {
        if (!IsDisposed)
        {
            return;
        }

        throw new ObjectDisposedException(string.Empty);
    }

    /// <summary>Executes the SubscribeCompletion operation.</summary>
    /// <param name="continuation">The continuation value.</param>
    /// <param name="originalContext">The originalContext value.</param>
    private void SubscribeCompletion(Action continuation, bool originalContext) =>
        Subscribe(new AwaitWitness<T>(continuation, originalContext));
}
