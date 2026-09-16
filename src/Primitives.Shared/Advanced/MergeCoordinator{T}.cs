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

/// <summary>Coordinates concurrent merge subscriptions.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>
/// Deliveries are serialized by a <see cref="SerializedDelivery{T}"/>, so no lock is held while the downstream observer
/// runs. Values that arrive while another thread is delivering are queued and delivered in arrival order; a value raised
/// by the delivering thread itself is delivered after the observer returns.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("MergeCoordinator: Active = {_active}, Done = {_delivery.IsTerminated}")]
public sealed class MergeCoordinator<T> : IDisposable
{
    /// <summary>Serializes downstream deliveries.</summary>
    private SerializedDelivery<T> _delivery = new();

    /// <summary>The number of active inner subscriptions.</summary>
    private int _active;

    /// <summary>Whether the outer source has completed, as 0 or 1.</summary>
    private int _outerCompleted;

    /// <summary>Initializes a new instance of the <see cref="MergeCoordinator{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
    public MergeCoordinator(IObserver<T> observer) =>
        Observer = observer ?? throw new ArgumentNullException(nameof(observer));

    /// <summary>Gets the active subscriptions.</summary>
    private MultipleDisposable Subscriptions { get; } = [];

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<T> Observer { get; }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => Subscriptions.Dispose();

    /// <summary>Subscribes to enumerable sources.</summary>
    /// <param name="sources">The sources to merge.</param>
    /// <returns>This coordinator as the subscription.</returns>
    public MergeCoordinator<T> Run(IEnumerable<IObservable<T>> sources)
    {
        foreach (var source in sources)
        {
            OnSource(source);
        }

        OnOuterCompleted();
        return this;
    }

    /// <summary>Subscribes to a pair of sources.</summary>
    /// <param name="first">The first source.</param>
    /// <param name="second">The second source.</param>
    /// <returns>This coordinator as the subscription.</returns>
    public MergeCoordinator<T> Run(IObservable<T> first, IObservable<T> second)
    {
        OnSource(first);
        OnSource(second);
        OnOuterCompleted();
        return this;
    }

    /// <summary>Subscribes a new inner source.</summary>
    /// <param name="source">The inner source.</param>
    public void OnSource(IObservable<T>? source)
    {
        if (source is null)
        {
            OnAnyError(new InvalidOperationException("Blend source contained null."));
            return;
        }

        _ = Interlocked.Increment(ref _active);
        var completed = 0;
        Subscriptions.Add(source.Subscribe(
            OnInnerNext,
            OnAnyError,
            () =>
            {
                if (Interlocked.Exchange(ref completed, 1) != 0)
                {
                    return;
                }

                OnInnerCompleted();
            }));
    }

    /// <summary>Forwards the first terminal error.</summary>
    /// <param name="error">The error to forward.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnAnyError(Exception error) => _delivery.OnError(error, new PendingDrain(this));

    /// <summary>Forwards an inner value, directly when nothing else is delivering.</summary>
    /// <param name="value">The value to forward.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnInnerNext(T value) => _delivery.OnNext(Observer, value, new PendingDrain(this));

    /// <summary>Marks one inner source complete.</summary>
    private void OnInnerCompleted()
    {
        _ = Interlocked.Decrement(ref _active);
        TryComplete();
    }

    /// <summary>Marks source enumeration complete.</summary>
    private void OnOuterCompleted()
    {
        Volatile.Write(ref _outerCompleted, 1);
        TryComplete();
    }

    /// <summary>Delivers completion once enumeration and all inners are done.</summary>
    private void TryComplete()
    {
        if (Volatile.Read(ref _outerCompleted) == 0 || Volatile.Read(ref _active) != 0)
        {
            return;
        }

        _delivery.OnCompleted(new PendingDrain(this));
    }

    /// <summary>Drains this coordinator's queued notifications for the delivery gate.</summary>
    /// <param name="Owner">The coordinator.</param>
    private readonly record struct PendingDrain(MergeCoordinator<T> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Drain() => _ = Owner._delivery.DrainTo(Owner.Observer);
    }
}
