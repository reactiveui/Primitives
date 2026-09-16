// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Forwards source notifications to one observer one at a time without holding a lock while it runs, and owns the upstream subscription.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>
/// A notification that arrives while another thread is delivering is queued and delivered in arrival order by that
/// thread, so a producer never blocks on the observer. The first terminal notification wins, and one queued before
/// disposal is still delivered.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("SerializeWitness: {_delivery}")]
public sealed class SerializeWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>Serializes downstream deliveries.</summary>
    private SerializedDelivery<T> _delivery = new();

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <summary>Initializes a new instance of the <see cref="SerializeWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
    public SerializeWitness(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        _observer = observer;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(T value) => _delivery.OnNext(_observer, value, new PendingDrain(this));

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => _delivery.OnError(error, new PendingDrain(this));

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => _delivery.OnCompleted(new PendingDrain(this));

    /// <summary>Assigns the upstream subscription, disposing it at once when this witness has been disposed.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);

    /// <summary>Drains this witness's queued notifications for the delivery gate.</summary>
    /// <param name="Owner">The witness.</param>
    private readonly record struct PendingDrain(SerializeWitness<T> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Drain() => _ = Owner._delivery.DrainTo(Owner._observer);
    }
}
