// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Delivers a value that is read again each time it changes to one observer, one delivery at a time and without a lock.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>
/// The class form of <see cref="CurrentValueDelivery{T}"/>. Attach the change hook, call <see cref="Start"/>, then call
/// <see cref="Changed"/> from the hook on any thread; changes raised under contention conflate to the latest value.
/// </remarks>
[DebuggerDisplay("CurrentValueWitness: {_delivery}")]
public sealed class CurrentValueWitness<T> : IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>Reads the current value.</summary>
    private readonly ICurrentValueReader<T> _reader;

    /// <summary>Serializes reads and deliveries.</summary>
    private CurrentValueDelivery<T> _delivery;

    /// <summary>Initializes a new instance of the <see cref="CurrentValueWitness{T}"/> class that delivers every read.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="reader">Reads the current value.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> or <paramref name="reader"/> is <see langword="null"/>.</exception>
    public CurrentValueWitness(IObserver<T> observer, ICurrentValueReader<T> reader)
        : this(observer, reader, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CurrentValueWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="reader">Reads the current value.</param>
    /// <param name="comparer">Skips a value equal to the last one delivered; <see langword="null"/> delivers every read.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> or <paramref name="reader"/> is <see langword="null"/>.</exception>
    public CurrentValueWitness(IObserver<T> observer, ICurrentValueReader<T> reader, IEqualityComparer<T>? comparer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        ArgumentExceptionHelper.ThrowIfNull(reader);
        _observer = observer;
        _reader = reader;
        _delivery = new(comparer);
    }

    /// <summary>Gets a value indicating whether the terminal notification has been delivered or the witness was disposed.</summary>
    public bool IsTerminated => _delivery.IsTerminated;

    /// <summary>Reads and delivers the initial value, then accepts changes.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Start() => _delivery.Start(new PendingDrain(this));

    /// <summary>Records a change and delivers the current value.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Changed() => _delivery.Changed(new PendingDrain(this));

    /// <summary>Delivers an error as the terminal notification, after a pending change.</summary>
    /// <param name="error">The error.</param>
    /// <exception cref="ArgumentNullException"><paramref name="error"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Fault(Exception error) => _delivery.Fault(error, new PendingDrain(this));

    /// <summary>Delivers completion as the terminal notification, after a pending change.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Complete() => _delivery.Complete(new PendingDrain(this));

    /// <summary>Stops delivery; nothing further is delivered.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => _delivery.Stop();

    /// <summary>Drains this witness's recorded work for the delivery gate.</summary>
    /// <param name="Owner">The witness.</param>
    private readonly record struct PendingDrain(CurrentValueWitness<T> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Drain() => _ = Owner._delivery.DrainTo(Owner._observer, Owner._reader);
    }
}
