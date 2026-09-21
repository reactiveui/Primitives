// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>One window of a sliced sequence: values published to it reach every current subscriber.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>
/// A subscriber that arrives after the window has terminated receives the terminal notification at once. Each
/// subscription holds a lease on the <see cref="SharedSubscription"/> that owns the source, so the source keeps running
/// while any window is subscribed. Publishing is not serialized; route notifications through a <see cref="SliceRouter{TOuter, T}"/>.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("SliceWindow: Terminated = {_terminated}")]
public sealed class SliceWindow<T> : IObservable<T>
{
    /// <summary>Serializes subscription against the terminal notification.</summary>
    private readonly Lock _gate = new();

    /// <summary>The subscription that owns the source this window reads from.</summary>
    private readonly SharedSubscription _owner;

    /// <summary>The current subscribers.</summary>
    private Broadcaster<T> _observers;

    /// <summary>The terminal error, or <see langword="null"/> when the window completed or has not terminated.</summary>
    private Exception? _error;

    /// <summary>Whether the window has terminated.</summary>
    private bool _terminated;

    /// <summary>Initializes a new instance of the <see cref="SliceWindow{T}"/> class.</summary>
    /// <param name="owner">The shared subscription that owns the source this window reads from.</param>
    /// <exception cref="ArgumentNullException"><paramref name="owner"/> is <see langword="null"/>.</exception>
    public SliceWindow(SharedSubscription owner) =>
        _owner = owner ?? throw new ArgumentNullException(nameof(owner));

    /// <summary>Publishes a value to the current subscribers; a terminated window ignores it.</summary>
    /// <param name="value">The value to publish.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Publish(T value) => _observers.Next(value);

    /// <summary>Completes the current subscribers and every later subscriber.</summary>
    public void Complete()
    {
        if (!TryTerminate(null))
        {
            return;
        }

        _observers.Completed();
        _observers.Clear();
    }

    /// <summary>Faults the current subscribers and every later subscriber.</summary>
    /// <param name="error">The terminal error.</param>
    /// <exception cref="ArgumentNullException"><paramref name="error"/> is <see langword="null"/>.</exception>
    public void Fault(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);

        if (!TryTerminate(error))
        {
            return;
        }

        _observers.Error(error);
        _observers.Clear();
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        Exception? error;
        bool terminated;
        lock (_gate)
        {
            terminated = _terminated;
            error = _error;
            if (!terminated)
            {
                _observers.Add(observer);
            }
        }

        if (!terminated)
        {
            return new Subscription(this, observer, _owner.Acquire());
        }

        if (error is null)
        {
            observer.OnCompleted();
        }
        else
        {
            observer.OnError(error);
        }

        return EmptyDisposable.Instance;
    }

    /// <summary>Records the terminal notification.</summary>
    /// <param name="error">The terminal error, or <see langword="null"/> for completion.</param>
    /// <returns><see langword="true"/> when this call terminated the window.</returns>
    private bool TryTerminate(Exception? error)
    {
        lock (_gate)
        {
            if (_terminated)
            {
                return false;
            }

            _terminated = true;
            _error = error;
            return true;
        }
    }

    /// <summary>Removes a subscriber.</summary>
    /// <param name="observer">The subscriber to remove.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Remove(IObserver<T> observer) => _observers.Remove(observer);

    /// <summary>One subscription to the window and its lease on the source.</summary>
    /// <param name="window">The window that owns the subscriber.</param>
    /// <param name="observer">The subscriber.</param>
    /// <param name="lease">The lease that keeps the source alive.</param>
    private sealed class Subscription(SliceWindow<T> window, IObserver<T> observer, IDisposable lease) : IDisposable
    {
        /// <summary>The window that owns the subscriber, cleared by the first disposal.</summary>
        private SliceWindow<T>? _window = window;

        /// <inheritdoc/>
        public void Dispose()
        {
            var owner = Interlocked.Exchange(ref _window, null);
            if (owner is null)
            {
                return;
            }

            owner.Remove(observer);
            lease.Dispose();
        }
    }
}
