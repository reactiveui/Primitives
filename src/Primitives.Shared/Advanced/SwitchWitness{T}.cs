// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Mediates latest-inner subscription switching for <see cref="SwitchSignal{T}"/>.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>
/// The gate only guards the generation bookkeeping: a notification is accepted or dropped and queued on a
/// <see cref="SerializedDelivery{T}"/> under it, then delivered after it is released, so no lock is held while the observer,
/// an inner subscription or its disposal runs. A notification raised by the delivering thread itself is delivered after
/// the observer returns. A terminal notification raised before <see cref="Dispose"/> is still delivered and, since disposal
/// does not wait, can reach the observer on the delivering thread just after <see cref="Dispose"/> returns.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("SwitchWitness: Version = {_version}, IsDone = {IsDone}")]
public sealed class SwitchWitness<T> : IDisposable
{
    /// <summary>Guards the generation bookkeeping; never held while user code runs.</summary>
    private readonly Lock _gate = new();

    /// <summary>Serializes downstream deliveries.</summary>
    private SerializedDelivery<T> _delivery = new();

    /// <summary>The current inner source version.</summary>
    private int _version;

    /// <summary>The subscription to the outer source.</summary>
    private IDisposable? _outer;

    /// <summary>The subscription to the current inner source; guarded by the gate.</summary>
    private IDisposable? _inner;

    /// <summary>Initializes a new instance of the <see cref="SwitchWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public SwitchWitness(IObserver<T> observer) => Observer = observer;

    /// <summary>Gets the gate guarding the generation bookkeeping.</summary>
    internal Lock Gate => _gate;

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<T> Observer { get; }

    /// <summary>Gets or sets a value indicating whether the outer source completed.</summary>
    private bool IsOuterCompleted { get; set; }

    /// <summary>Gets or sets a value indicating whether an inner source is active.</summary>
    private bool IsInnerActive { get; set; }

    /// <summary>Gets or sets a value indicating whether a terminal notification has been queued or the witness disposed.</summary>
    private bool IsDone { get; set; }

    /// <inheritdoc/>
    public void Dispose()
    {
        IDisposable? inner;
        lock (_gate)
        {
            IsDone = true;
            inner = _inner;
            _inner = null;
        }

        inner?.Dispose();
        SinkSubscription.Dispose(ref _outer);
    }

    /// <summary>Starts observing the outer source.</summary>
    /// <param name="sources">The outer source.</param>
    /// <returns>The observer that owns the subscriptions.</returns>
    public SwitchWitness<T> Run(IObservable<IObservable<T>> sources)
    {
        SinkSubscription.Set(ref _outer, sources.Subscribe(new OuterWitness(this)));
        return this;
    }

    /// <summary>Activates the next inner generation unless a terminal notification has been sent.</summary>
    /// <param name="version">The activated generation, or zero when the witness is done.</param>
    /// <returns><see langword="true"/> when an inner generation was activated; otherwise, <see langword="false"/>.</returns>
    internal bool TryBeginSource(out int version)
    {
        lock (_gate)
        {
            if (IsDone)
            {
                version = 0;
                return false;
            }

            version = _version + 1;
            Volatile.Write(ref _version, version);
            IsInnerActive = true;
            return true;
        }
    }

    /// <summary>Keeps the subscription for a generation when it is still current, otherwise disposes it.</summary>
    /// <param name="version">The generation the subscription belongs to.</param>
    /// <param name="subscription">The inner subscription.</param>
    internal void Install(int version, IDisposable subscription)
    {
        IDisposable? displaced;
        lock (_gate)
        {
            if (IsDone || version != _version)
            {
                displaced = subscription;
            }
            else
            {
                displaced = _inner;
                _inner = subscription;
            }
        }

        displaced?.Dispose();
    }

    /// <summary>Marks the outer source complete.</summary>
    internal void OnOuterCompleted()
    {
        lock (_gate)
        {
            if (IsDone)
            {
                return;
            }

            IsOuterCompleted = true;
            if (!TryPostCompletion())
            {
                return;
            }
        }

        _delivery.Flush(new PendingDrain(this));
    }

    /// <summary>Forwards the outer source error once.</summary>
    /// <param name="error">The error to forward.</param>
    internal void OnOuterError(Exception error)
    {
        lock (_gate)
        {
            if (IsDone)
            {
                return;
            }

            IsDone = true;
            _ = _delivery.PostError(error);
        }

        _delivery.Flush(new PendingDrain(this));
    }

    /// <summary>Forwards a current inner value.</summary>
    /// <param name="version">The inner source version.</param>
    /// <param name="value">The value to forward.</param>
    internal void OnNext(int version, T value)
    {
        lock (_gate)
        {
            if (IsDone || version != _version)
            {
                return;
            }

            _ = _delivery.Post(value);
        }

        _delivery.Flush(new PendingDrain(this));
    }

    /// <summary>Forwards a current inner error.</summary>
    /// <param name="version">The inner source version.</param>
    /// <param name="error">The error to forward.</param>
    internal void OnError(int version, Exception error)
    {
        lock (_gate)
        {
            if (IsDone || version != _version)
            {
                return;
            }

            IsDone = true;
            _ = _delivery.PostError(error);
        }

        _delivery.Flush(new PendingDrain(this));
    }

    /// <summary>Marks a current inner source complete.</summary>
    /// <param name="version">The inner source version.</param>
    internal void OnCompleted(int version)
    {
        lock (_gate)
        {
            if (IsDone || version != _version)
            {
                return;
            }

            IsInnerActive = false;
            if (!TryPostCompletion())
            {
                return;
            }
        }

        _delivery.Flush(new PendingDrain(this));
    }

    /// <summary>Switches to a new inner source.</summary>
    /// <param name="source">The new inner source.</param>
    private void OnSource(IObservable<T> source)
    {
        if (source is null)
        {
            OnOuterError(new InvalidOperationException("Switch source contained null."));
            return;
        }

        if (!TryBeginSource(out var current))
        {
            return;
        }

        Install(current, source.Subscribe(new InnerWitness(this, current)));
    }

    /// <summary>Queues completion once the outer source and current inner source are done; called under the gate.</summary>
    /// <returns><see langword="true"/> when completion was queued.</returns>
    private bool TryPostCompletion()
    {
        if (!IsOuterCompleted || IsInnerActive)
        {
            return false;
        }

        IsDone = true;
        return _delivery.PostCompleted();
    }

    /// <summary>Drains this witness's queued notifications for the delivery gate.</summary>
    /// <param name="Owner">The witness.</param>
    private readonly record struct PendingDrain(SwitchWitness<T> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        public void Drain()
        {
            if (Owner._delivery.DrainTo(Owner.Observer))
            {
                Owner.Dispose();
            }
        }
    }

    /// <summary>Observes the outer sequence of inner observables.</summary>
    /// <param name="parent">The coordinator.</param>
    private sealed class OuterWitness(SwitchWitness<T> parent) : IObserver<IObservable<T>>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(IObservable<T> value) => parent.OnSource(value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => parent.OnOuterError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => parent.OnOuterCompleted();
    }

    /// <summary>Receives the notifications of one source and passes them on, tagged with the version the source was subscribed at.</summary>
    /// <param name="parent">The coordinator.</param>
    /// <param name="version">The version of the source.</param>
    private sealed class InnerWitness(SwitchWitness<T> parent, int version) : IObserver<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => parent.OnNext(version, value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => parent.OnError(version, error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => parent.OnCompleted(version);
    }
}
