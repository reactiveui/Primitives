// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>
/// Serialized, non-blocking delivery of a value that is read again each time it changes, held as a mutable field by the
/// subscription that observes it.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>
/// <para>
/// <see cref="Changed{TDrain}"/> records a change and delivers it directly when nothing else is delivering; otherwise the
/// delivering thread reads the value again, so changes raised under contention conflate to the latest value. The value is
/// read inside the serialized section, and with a comparer a value equal to the last one delivered is skipped, including
/// on the initial read.
/// </para>
/// <para>
/// Attach the change hook, then call <see cref="Start{TDrain}"/> to read and deliver the initial value. Changes raised
/// before <see cref="Start{TDrain}"/> are ignored because the initial read covers them. A terminal notification is
/// delivered after a pending change, and the first one wins.
/// </para>
/// <para>
/// A reader or observer that throws propagates to the thread delivering, and the next change is still delivered. Keep the
/// field non-readonly and call it in place, since a copy is a separate gate. The owner passes a drain whose
/// <see cref="IDrainTarget.Drain"/> calls <see cref="DrainTo{TReader}"/> with the same observer and reader.
/// </para>
/// </remarks>
[DebuggerDisplay("CurrentValueDelivery: HasValue = {_hasValue}, Started = {_started}, Stopped = {_stopped}, Terminal = {_terminal}")]
public record struct CurrentValueDelivery<T>
{
    /// <summary>Compares a value with the last one delivered; <see langword="null"/> delivers every read.</summary>
    private readonly IEqualityComparer<T>? _comparer;

    /// <summary>Serializes deliveries to the downstream observer.</summary>
    private DeliveryGateState _gate;

    /// <summary>The last value delivered, kept only when a comparer is set.</summary>
    private T _last;

    /// <summary>The recorded terminal error, or <see langword="null"/> for completion.</summary>
    private Exception? _error;

    /// <summary>1 when a change has been recorded and not yet read.</summary>
    private int _changed;

    /// <summary>1 once <see cref="Start{TDrain}"/> has been called.</summary>
    private int _started;

    /// <summary>1 once <see cref="Stop"/> has been called.</summary>
    private int _stopped;

    /// <summary>A <see cref="CurrentValueTerminalState"/>, stored as an integer so it can be changed atomically.</summary>
    private int _terminal;

    /// <summary>Whether a value has been delivered; read and written only inside the serialized section.</summary>
    private bool _hasValue;

    /// <summary>Initializes a new instance of the <see cref="CurrentValueDelivery{T}"/> struct.</summary>
    /// <param name="distinctUntilChanged">Whether a value equal to the last one delivered is skipped, using <see cref="EqualityComparer{T}.Default"/>.</param>
    public CurrentValueDelivery(bool distinctUntilChanged)
        : this(distinctUntilChanged ? EqualityComparer<T>.Default : null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CurrentValueDelivery{T}"/> struct.</summary>
    /// <param name="comparer">Compares a value with the last one delivered; <see langword="null"/> delivers every read.</param>
    public CurrentValueDelivery(IEqualityComparer<T>? comparer)
    {
        _comparer = comparer;
        _last = default!;
    }

    /// <summary>Gets a value indicating whether the terminal notification has been delivered or delivery was stopped.</summary>
    public readonly bool IsTerminated
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _terminal == (int)CurrentValueTerminalState.Delivered || _stopped != 0;
    }

    /// <summary>Reads and delivers the initial value, then accepts changes.</summary>
    /// <typeparam name="TDrain">The drain type.</typeparam>
    /// <param name="drain">Drains the recorded work into the downstream observer.</param>
    /// <remarks>Call it after attaching the change hook. A throwing reader or observer propagates out of this call.</remarks>
    public void Start<TDrain>(TDrain drain)
        where TDrain : IDrainTarget
    {
        Volatile.Write(ref _started, 1);
        Changed(drain);
    }

    /// <summary>Records a change and delivers the current value, directly when nothing else is delivering.</summary>
    /// <typeparam name="TDrain">The drain type.</typeparam>
    /// <param name="drain">Drains the recorded work into the downstream observer.</param>
    public void Changed<TDrain>(TDrain drain)
        where TDrain : IDrainTarget
    {
        if (Volatile.Read(ref _started) == 0 || Volatile.Read(ref _stopped) != 0)
        {
            return;
        }

        Volatile.Write(ref _changed, 1);
        DeliveryGate.Signal(ref _gate, drain);
    }

    /// <summary>Delivers an error as the terminal notification, after a pending change.</summary>
    /// <typeparam name="TDrain">The drain type.</typeparam>
    /// <param name="error">The error.</param>
    /// <param name="drain">Drains the recorded work into the downstream observer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="error"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Fault<TDrain>(Exception error, TDrain drain)
        where TDrain : IDrainTarget
    {
        ArgumentExceptionHelper.ThrowIfNull(error);
        RequestTerminal(error, drain);
    }

    /// <summary>Delivers completion as the terminal notification, after a pending change.</summary>
    /// <typeparam name="TDrain">The drain type.</typeparam>
    /// <param name="drain">Drains the recorded work into the downstream observer.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Complete<TDrain>(TDrain drain)
        where TDrain : IDrainTarget =>
        RequestTerminal(null, drain);

    /// <summary>Stops delivery; recorded work is dropped and nothing further is delivered.</summary>
    /// <remarks>
    /// It never waits, so a value already read on another thread can still arrive after it returns.
    /// </remarks>
    public void Stop()
    {
        Volatile.Write(ref _stopped, 1);
        Volatile.Write(ref _changed, 0);
    }

    /// <summary>Reads and delivers a recorded change, then a recorded terminal notification.</summary>
    /// <typeparam name="TReader">The reader type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="reader">Reads the current value.</param>
    /// <returns><see langword="true"/> when this call delivered the terminal notification.</returns>
    public bool DrainTo<TReader>(IObserver<T> observer, TReader reader)
        where TReader : ICurrentValueReader<T>
    {
        if (IsTerminated)
        {
            return false;
        }

        if (Interlocked.Exchange(ref _changed, 0) != 0)
        {
            DeliverCurrent(observer, reader);
        }

        if (Volatile.Read(ref _terminal) != (int)CurrentValueTerminalState.Requested || Volatile.Read(ref _stopped) != 0)
        {
            return false;
        }

        Volatile.Write(ref _terminal, (int)CurrentValueTerminalState.Delivered);
        if (_error is null)
        {
            observer.OnCompleted();
        }
        else
        {
            observer.OnError(_error);
        }

        return true;
    }

    /// <summary>Records the first terminal notification and delivers it once started.</summary>
    /// <typeparam name="TDrain">The drain type.</typeparam>
    /// <param name="error">The error, or <see langword="null"/> for completion.</param>
    /// <param name="drain">Drains the recorded work into the downstream observer.</param>
    private void RequestTerminal<TDrain>(Exception? error, TDrain drain)
        where TDrain : IDrainTarget
    {
        if (Interlocked.CompareExchange(ref _terminal, (int)CurrentValueTerminalState.Recording, (int)CurrentValueTerminalState.None)
            != (int)CurrentValueTerminalState.None)
        {
            return;
        }

        _error = error;
        Volatile.Write(ref _terminal, (int)CurrentValueTerminalState.Requested);
        if (Volatile.Read(ref _started) == 0)
        {
            return;
        }

        DeliveryGate.Signal(ref _gate, drain);
    }

    /// <summary>Reads the current value and delivers it unless it equals the last one delivered.</summary>
    /// <typeparam name="TReader">The reader type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="reader">Reads the current value.</param>
    private void DeliverCurrent<TReader>(IObserver<T> observer, TReader reader)
        where TReader : ICurrentValueReader<T>
    {
        var value = reader.Read();
        if (_comparer is not null)
        {
            if (_hasValue && _comparer.Equals(value, _last))
            {
                return;
            }

            _last = value;
        }

        _hasValue = true;
        if (Volatile.Read(ref _stopped) != 0)
        {
            return;
        }

        observer.OnNext(value);
    }
}
