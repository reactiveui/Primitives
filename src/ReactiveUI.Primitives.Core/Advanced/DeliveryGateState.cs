// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>The state of one <see cref="DeliveryGate"/>, held as a mutable field by the type whose deliveries it serializes.</summary>
/// <remarks>Pass the field by reference to <see cref="DeliveryGate"/>; a copy is a separate gate.</remarks>
[DebuggerDisplay("DeliveryGateState: State = {_state}, Owner = {_owner}, Waiters = {_waiters}, Depth = {_depth}")]
public record struct DeliveryGateState
{
    /// <summary>Nothing is delivering.</summary>
    private const int Idle = 0;

    /// <summary>A thread is delivering.</summary>
    private const int Emitting = 1;

    /// <summary>A thread is delivering and work has been handed to it since it last drained.</summary>
    private const int EmittingDirty = 2;

    /// <summary>Nothing is delivering, but a failed delivery left handed-over work that the next signal must drain.</summary>
    private const int IdlePending = 3;

    /// <summary>One of <see cref="Idle"/>, <see cref="Emitting"/>, <see cref="EmittingDirty"/> or <see cref="IdlePending"/>.</summary>
    private int _state;

    /// <summary>The managed thread id of the delivering thread, or zero.</summary>
    private int _owner;

    /// <summary>The number of producers waiting to deliver on their own thread.</summary>
    private int _waiters;

    /// <summary>The number of nested enters by the delivering thread; read and written only by that thread.</summary>
    private int _depth;

    /// <summary>Gets the number of producers waiting to deliver on their own thread.</summary>
    internal readonly int Waiters => _waiters;

    /// <summary>Takes the gate when nothing is delivering and no producer is waiting.</summary>
    /// <returns><see langword="true"/> when the calling thread took the gate.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryEnter()
    {
        if (Volatile.Read(ref _waiters) != 0 || Interlocked.CompareExchange(ref _state, Emitting, Idle) != Idle)
        {
            return false;
        }

        Volatile.Write(ref _owner, Environment.CurrentManagedThreadId);
        return true;
    }

    /// <summary>Counts a nested enter when the calling thread is the one delivering.</summary>
    /// <returns><see langword="true"/> when the calling thread already owns the gate.</returns>
    internal bool TryEnterNested()
    {
        if (Volatile.Read(ref _owner) != Environment.CurrentManagedThreadId)
        {
            return false;
        }

        _depth++;
        return true;
    }

    /// <summary>Releases the gate, first draining any work handed over meanwhile.</summary>
    /// <typeparam name="TDrain">The drain type.</typeparam>
    /// <param name="drain">Delivers every piece of recorded work that has not been delivered yet.</param>
    internal void Exit<TDrain>(TDrain drain)
        where TDrain : IDrainTarget
    {
        if (_depth != 0)
        {
            _depth--;
            return;
        }

        if (Release())
        {
            return;
        }

        var me = Environment.CurrentManagedThreadId;
        if (!RetakeAfterHandOff(me))
        {
            return;
        }

        Drain(drain, me);
    }

    /// <summary>Drains on the calling thread, waiting up to the budget for another thread's delivery first.</summary>
    /// <typeparam name="TDrain">The drain type.</typeparam>
    /// <param name="drain">Delivers every piece of recorded work that has not been delivered yet.</param>
    /// <param name="waitBudgetTimestamp">The wait budget, in <see cref="Stopwatch"/> ticks.</param>
    internal void Signal<TDrain>(TDrain drain, long waitBudgetTimestamp)
        where TDrain : IDrainTarget
    {
        var me = Environment.CurrentManagedThreadId;
        if (!TryEnter())
        {
            if (!WaitOrHandOff(me, waitBudgetTimestamp))
            {
                return;
            }

            Volatile.Write(ref _owner, me);
        }

        Drain(drain, me);
    }

    /// <summary>Releases a gate whose delivery threw, keeping any handed-over work for the next signal.</summary>
    /// <returns><see langword="true"/> when handed-over work is still waiting to be drained.</returns>
    internal bool Reset()
    {
        if (_depth != 0)
        {
            _depth--;
            return false;
        }

        Volatile.Write(ref _owner, 0);
        if (Interlocked.Exchange(ref _state, Idle) != EmittingDirty)
        {
            return false;
        }

        _ = Interlocked.CompareExchange(ref _state, IdlePending, Idle);
        return true;
    }

    /// <summary>Runs the drain until no work was handed over during it, yielding to a waiting producer when there is one.</summary>
    /// <typeparam name="TDrain">The drain type.</typeparam>
    /// <param name="drain">Delivers every piece of recorded work that has not been delivered yet.</param>
    /// <param name="me">The managed thread id of the calling thread.</param>
    private void Drain<TDrain>(TDrain drain, int me)
        where TDrain : IDrainTarget
    {
        try
        {
            do
            {
                drain.Drain();
            }
            while (!Release() && RetakeAfterHandOff(me));
        }
        catch
        {
            _ = Reset();
            throw;
        }
    }

    /// <summary>Clears the owner and moves an undisturbed delivery back to idle.</summary>
    /// <returns><see langword="true"/> when no work was handed over during the delivery.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool Release()
    {
        Volatile.Write(ref _owner, 0);
        return Interlocked.CompareExchange(ref _state, Idle, Emitting) == Emitting;
    }

    /// <summary>
    /// Releases a delivery that was handed work, then takes it back unless a producer is waiting to deliver it on its
    /// own thread. The idle write comes before the waiter read, and a waiter leaves the count before it reads the
    /// state, so one of the two always drains the handed-over work.
    /// </summary>
    /// <param name="me">The managed thread id of the calling thread.</param>
    /// <returns><see langword="true"/> when the calling thread must drain again.</returns>
    private bool RetakeAfterHandOff(int me)
    {
        Volatile.Write(ref _state, Idle);
        if (Volatile.Read(ref _waiters) != 0 || Interlocked.CompareExchange(ref _state, Emitting, Idle) != Idle)
        {
            return false;
        }

        Volatile.Write(ref _owner, me);
        return true;
    }

    /// <summary>Takes a gate that is idle, including one left holding work by a failed delivery.</summary>
    /// <returns><see langword="true"/> when the calling thread took the gate.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TakeIdle()
    {
        var state = Volatile.Read(ref _state);
        return state is Idle or IdlePending && Interlocked.CompareExchange(ref _state, Emitting, state) == state;
    }

    /// <summary>
    /// Waits up to the budget for the delivering thread to finish, then marks the running delivery dirty. The delivering
    /// thread itself marks dirty at once.
    /// </summary>
    /// <param name="me">The managed thread id of the calling thread.</param>
    /// <param name="waitBudgetTimestamp">The wait budget, in <see cref="Stopwatch"/> ticks.</param>
    /// <returns><see langword="true"/> when the calling thread took the gate and must drain.</returns>
    private bool WaitOrHandOff(int me, long waitBudgetTimestamp) =>
        (Volatile.Read(ref _owner) != me && WaitForIdle(waitBudgetTimestamp)) || TakeIdleOrHandOff();

    /// <summary>
    /// Takes the gate if it has gone idle, otherwise marks the running delivery dirty. It loops only when another thread
    /// changes the state between two reads.
    /// </summary>
    /// <returns><see langword="true"/> when the calling thread took the gate and must drain.</returns>
    [ExcludeFromCodeCoverage]
    private bool TakeIdleOrHandOff()
    {
        while (true)
        {
            if (TakeIdle())
            {
                return true;
            }

            var state = Volatile.Read(ref _state);
            if (state == EmittingDirty
                || (state == Emitting && Interlocked.CompareExchange(ref _state, EmittingDirty, Emitting) == Emitting))
            {
                return false;
            }
        }
    }

    /// <summary>Joins the waiters and polls for an idle gate until the budget runs out.</summary>
    /// <param name="waitBudgetTimestamp">The wait budget, in <see cref="Stopwatch"/> ticks.</param>
    /// <returns><see langword="true"/> when the calling thread took the gate.</returns>
    private bool WaitForIdle(long waitBudgetTimestamp)
    {
        _ = Interlocked.Increment(ref _waiters);
        try
        {
            var deadline = Stopwatch.GetTimestamp() + waitBudgetTimestamp;
            SpinWait spinner = default;
            do
            {
                if (TakeIdle())
                {
                    return true;
                }

#if NETCOREAPP3_0_OR_GREATER
                // Sleep(1) overshoots the budget on a coarse OS timer, holding the producer about half as long again.
                spinner.SpinOnce(sleep1Threshold: -1);
#else
                spinner.SpinOnce();
#endif
            }
            while (Stopwatch.GetTimestamp() < deadline);

            return false;
        }
        finally
        {
            _ = Interlocked.Decrement(ref _waiters);
        }
    }
}
