// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals;

/// <summary>A signal that records the latest value and replays it to observers when it completes, so the completion can be awaited.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class AsyncSignal<T> : IAwaitSignal<T>
{
    /// <summary>Serializes observer changes and terminal-state transitions.</summary>
    private readonly Lock _observerLock = new();

    /// <summary>The most recent value, replayed when the signal completes.</summary>
    private T? _lastValue;

    /// <summary>Whether a value has been recorded.</summary>
    private bool _hasValue;

    /// <summary>The terminal error, when the signal faulted.</summary>
    private Exception? _lastError;

    /// <summary>The dispatch target: the empty witness or a <see cref="ListWitness{T}"/> fan-out.</summary>
    private IObserver<T> _outObserver = EmptyWitness<T>.Instance;

    /// <summary>Gets a value indicating whether this instance is disposed.</summary>
    public bool IsDisposed { get; private set; }

    /// <summary>Gets the value the signal completed with.</summary>
    /// <exception cref="InvalidOperationException">The signal has not completed.</exception>
    public T Value
    {
        get
        {
            ThrowIfDisposed();
            if (!IsCompleted)
            {
                throw new InvalidOperationException("AsyncSignal is not completed yet");
            }

            _lastError.Rethrow();

            return _lastValue!;
        }
    }

    /// <summary>Gets a value indicating whether an observer is subscribed and the signal has neither completed nor been disposed.</summary>
    public bool HasObservers => _outObserver is not EmptyWitness<T> && !IsCompleted && !IsDisposed;

    /// <summary>Gets a value indicating whether this instance is completed.</summary>
    public bool IsCompleted { get; private set; }

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Completes the signal, emitting the recorded value to the current observers first when one was recorded.</summary>
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

    /// <summary>Registers a callback to run when the signal terminates, on the captured synchronization context.</summary>
    /// <param name="continuation">The callback to run on completion or failure.</param>
    /// <exception cref="ArgumentNullException"><paramref name="continuation"/> is <see langword="null"/>.</exception>
    public void OnCompleted(Action continuation)
    {
        ArgumentExceptionHelper.ThrowIfNull(continuation);

        SubscribeCompletion(continuation, true);
    }

    /// <summary>Faults the signal and forwards <paramref name="error"/> to the current observers, discarding any recorded value.</summary>
    /// <param name="error">The terminal error.</param>
    /// <exception cref="ArgumentNullException"><paramref name="error"/> is <see langword="null"/>.</exception>
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

    /// <summary>Records <paramref name="value"/> as the value replayed on completion; observers are not notified here.</summary>
    /// <param name="value">The value to record.</param>
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

    /// <summary>Subscribes an observer, delivering the terminal value or error immediately when the signal has completed.</summary>
    /// <param name="observer">The observer to subscribe.</param>
    /// <returns>A handle that removes the observer when disposed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
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
                    // Removal collapses the fan-out to the empty witness, never to a lone observer.
                    _outObserver = new ListWitness<T>(new([observer]));
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

    /// <summary>Drops the observers and the recorded value, making every later notification throw.</summary>
    public void Dispose()
    {
        if (IsDisposed)
        {
            return;
        }

        lock (_observerLock)
        {
            _outObserver = DisposedWitness<T>.Instance;
            _lastError = null;
            _lastValue = default;
        }

        IsDisposed = true;
    }

    /// <summary>Gets the awaiter for this signal.</summary>
    /// <returns>The signal itself, which acts as its own awaiter.</returns>
    public IAwaitSignal<T> GetAwaiter() => this;

    /// <summary>Gets the completed value, blocking the calling thread until the signal completes or faults.</summary>
    /// <returns>The recorded value, after rethrowing the terminal error when the signal faulted.</returns>
    /// <exception cref="InvalidOperationException">The signal completed without recording a value.</exception>
    public T GetResult()
    {
        WaitIfPending(WaitForCompletion);
        _lastError.Rethrow();

        if (!_hasValue)
        {
            throw new InvalidOperationException("The source completed without producing a value.");
        }

        return _lastValue!;
    }

    /// <summary>Removes an observer registered via <see cref="Subscribe"/>; the observer's subscription handle calls this on disposal.</summary>
    /// <param name="observer">The observer to remove.</param>
    public void RemoveObserver(IObserver<T> observer)
    {
        lock (_observerLock)
        {
            _outObserver = _outObserver is ListWitness<T> listObserver
                ? listObserver.Remove(observer)
                : EmptyWitness<T>.Instance;
        }
    }

    /// <summary>Invokes the wait operation only while completion is pending.</summary>
    /// <param name="wait">The operation that waits for this signal to complete.</param>
    internal void WaitIfPending(Action<AsyncSignal<T>> wait)
    {
        if (IsCompleted)
        {
            return;
        }

        wait(this);
    }

    /// <summary>Blocks the calling thread until the signal completes.</summary>
    /// <param name="signal">The signal supplying the completion notification.</param>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private static void WaitForCompletion(AsyncSignal<T> signal)
    {
        using ManualResetEvent completionEvent = new(false);
        signal.SubscribeCompletion(() => completionEvent.Set(), false);
        _ = completionEvent.WaitOne();
    }

    /// <summary>Rejects operations after the signal has been disposed.</summary>
    /// <exception cref="ObjectDisposedException">The signal is disposed.</exception>
    private void ThrowIfDisposed()
    {
        if (!IsDisposed)
        {
            return;
        }

        throw new ObjectDisposedException(string.Empty);
    }

    /// <summary>Registers a callback for either terminal notification.</summary>
    /// <param name="continuation">The callback invoked on the terminal notification.</param>
    /// <param name="originalContext">Whether to resume the callback on the captured synchronization context.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SubscribeCompletion(Action continuation, bool originalContext) =>
        Subscribe(new AwaitWitness<T>(continuation, originalContext));
}
