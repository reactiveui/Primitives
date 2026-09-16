// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Extensions;

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// A cold signal that emits a value on subscription and again whenever it changes, reading it inside a serialized section
/// so notifications raised on any thread reach the observer one at a time without a lock.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>
/// Every subscription attaches its own change hook, reads its own initial value and keeps its own distinct state. The hook
/// is attached before the initial read, so a change raised while attaching is covered by that read. When the initial read
/// or the observer throws, the hook is detached and the exception propagates out of <see cref="Subscribe"/>.
/// </remarks>
[DebuggerDisplay("CurrentValueSignal: Distinct = {_comparer != null}")]
public sealed class CurrentValueSignal<T> : IObservable<T>
{
    /// <summary>Reads the current value.</summary>
    private readonly Func<T> _read;

    /// <summary>Attaches a change callback and returns the handle that detaches it.</summary>
    private readonly Func<Action, IDisposable> _attach;

    /// <summary>Skips a value equal to the last one delivered; <see langword="null"/> delivers every read.</summary>
    private readonly IEqualityComparer<T>? _comparer;

    /// <summary>Initializes a new instance of the <see cref="CurrentValueSignal{T}"/> class that emits every read.</summary>
    /// <param name="read">Reads the current value.</param>
    /// <param name="attach">Attaches a change callback and returns the handle that detaches it.</param>
    /// <exception cref="ArgumentNullException"><paramref name="read"/> or <paramref name="attach"/> is <see langword="null"/>.</exception>
    public CurrentValueSignal(Func<T> read, Func<Action, IDisposable> attach)
        : this(read, attach, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CurrentValueSignal{T}"/> class.</summary>
    /// <param name="read">Reads the current value.</param>
    /// <param name="attach">Attaches a change callback and returns the handle that detaches it.</param>
    /// <param name="comparer">Skips a value equal to the last one delivered; <see langword="null"/> delivers every read.</param>
    /// <exception cref="ArgumentNullException"><paramref name="read"/> or <paramref name="attach"/> is <see langword="null"/>.</exception>
    public CurrentValueSignal(Func<T> read, Func<Action, IDisposable> attach, IEqualityComparer<T>? comparer)
    {
        ArgumentExceptionHelper.ThrowIfNull(read);
        ArgumentExceptionHelper.ThrowIfNull(attach);
        _read = read;
        _attach = attach;
        _comparer = comparer;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        Subscription subscription = new(observer, _read, _comparer);
        subscription.Run(_attach);
        return subscription;
    }

    /// <summary>One observer's change hook, initial read and distinct state.</summary>
    private sealed class Subscription : ICurrentValueReader<T>, IDisposable
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>Reads the current value.</summary>
        private readonly Func<T> _read;

        /// <summary>Serializes reads and deliveries.</summary>
        private CurrentValueDelivery<T> _delivery;

        /// <summary>The handle that detaches the change hook.</summary>
        private IDisposable? _detach;

        /// <summary>Initializes a new instance of the <see cref="Subscription"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="read">Reads the current value.</param>
        /// <param name="comparer">Skips a value equal to the last one delivered.</param>
        public Subscription(IObserver<T> observer, Func<T> read, IEqualityComparer<T>? comparer)
        {
            _observer = observer;
            _read = read;
            _delivery = new(comparer);
        }

        /// <summary>Attaches the change hook, then reads and delivers the initial value.</summary>
        /// <param name="attach">Attaches a change callback and returns the handle that detaches it.</param>
        public void Run(Func<Action, IDisposable> attach)
        {
            SinkSubscription.Set(ref _detach, attach(Changed));
            try
            {
                _delivery.Start(new PendingDrain(this));
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Read() => _read();

        /// <inheritdoc/>
        public void Dispose()
        {
            _delivery.Stop();
            SinkSubscription.Dispose(ref _detach);
        }

        /// <summary>Records a change raised by the hook.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Changed() => _delivery.Changed(new PendingDrain(this));

        /// <summary>Drains this subscription's recorded work for the delivery gate.</summary>
        /// <param name="Owner">The subscription.</param>
        private readonly record struct PendingDrain(Subscription Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => _ = Owner._delivery.DrainTo(Owner._observer, Owner);
        }
    }
}
