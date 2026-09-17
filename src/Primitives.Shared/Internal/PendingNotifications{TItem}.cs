// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Internal;
#else
namespace ReactiveUI.Primitives.Internal;
#endif

/// <summary>
/// The work a coordinator queues for its delivery gate while another thread is delivering: items in arrival order,
/// then one terminal notification. Held as a mutable field and never locked while user code runs.
/// </summary>
/// <typeparam name="TItem">The queued item type.</typeparam>
[System.Diagnostics.DebuggerDisplay("PendingNotifications: Count = {_count}, Terminal = {_terminal}")]
internal record struct PendingNotifications<TItem>
{
    /// <summary>No terminal notification has been requested.</summary>
    private const int NoTerminal = 0;

    /// <summary>A terminal notification has been requested and not yet taken.</summary>
    private const int TerminalRequested = 1;

    /// <summary>The terminal notification has been taken for delivery.</summary>
    private const int TerminalTaken = 2;

    /// <summary>Guards the queue and the terminal fields for the few instructions each operation takes.</summary>
    private SpinLock _gate;

    /// <summary>Items waiting for delivery, created on first contention.</summary>
    private Queue<TItem>? _items;

    /// <summary>The number of queued items, readable without the gate.</summary>
    private int _count;

    /// <summary>The terminal error, or <see langword="null"/> for completion.</summary>
    private Exception? _error;

    /// <summary>One of <see cref="NoTerminal"/>, <see cref="TerminalRequested"/> or <see cref="TerminalTaken"/>.</summary>
    private int _terminal;

    /// <summary>Initializes a new instance of the <see cref="PendingNotifications{TItem}"/> struct.</summary>
    public PendingNotifications() => _gate = new(enableThreadOwnerTracking: false);

    /// <summary>Gets a value indicating whether any item is queued.</summary>
    internal readonly bool HasItems
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _count != 0;
    }

    /// <summary>Gets a value indicating whether the terminal notification has been taken for delivery.</summary>
    internal readonly bool IsTerminated
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _terminal == TerminalTaken;
    }

    /// <summary>Queues an item unless a terminal notification has been requested.</summary>
    /// <param name="item">The item to queue.</param>
    /// <returns><see langword="true"/> when the item was queued.</returns>
    internal bool TryEnqueue(TItem item)
    {
        var taken = false;
        _gate.Enter(ref taken);
        try
        {
            if (Volatile.Read(ref _terminal) != NoTerminal)
            {
                return false;
            }

            (_items ??= new()).Enqueue(item);
            Volatile.Write(ref _count, _count + 1);
            return true;
        }
        finally
        {
            _gate.Exit(useMemoryBarrier: false);
        }
    }

    /// <summary>Requests the terminal notification; only the first request counts.</summary>
    /// <param name="error">The error to deliver, or <see langword="null"/> to complete.</param>
    /// <returns><see langword="true"/> when this was the first request.</returns>
    internal bool TryRequestTerminal(Exception? error)
    {
        var taken = false;
        _gate.Enter(ref taken);
        try
        {
            if (Interlocked.CompareExchange(ref _terminal, TerminalRequested, NoTerminal) != NoTerminal)
            {
                return false;
            }

            _error = error;
            return true;
        }
        finally
        {
            _gate.Exit(useMemoryBarrier: false);
        }
    }

    /// <summary>Drops the queued items and any requested terminal notification, and refuses everything after.</summary>
    internal void Stop()
    {
        var taken = false;
        _gate.Enter(ref taken);
        try
        {
            Volatile.Write(ref _terminal, TerminalTaken);
            _items = null;
            Volatile.Write(ref _count, 0);
        }
        finally
        {
            _gate.Exit(useMemoryBarrier: false);
        }
    }

    /// <summary>Takes the next queued item, or the terminal notification once the queue is empty.</summary>
    /// <param name="item">The item, when one was taken.</param>
    /// <param name="error">The terminal error, when the terminal notification was taken.</param>
    /// <returns>What was taken.</returns>
    internal PendingDelivery TakeNext(out TItem item, out Exception? error)
    {
        var taken = false;
        _gate.Enter(ref taken);
        try
        {
            error = _error;
            if (_count != 0)
            {
                item = _items!.Dequeue();
                Volatile.Write(ref _count, _count - 1);
                return PendingDelivery.Value;
            }

            item = default!;
            if (Volatile.Read(ref _terminal) != TerminalRequested)
            {
                return PendingDelivery.None;
            }

            Volatile.Write(ref _terminal, TerminalTaken);
            _items = null;
            return PendingDelivery.Terminal;
        }
        finally
        {
            _gate.Exit(useMemoryBarrier: false);
        }
    }
}
