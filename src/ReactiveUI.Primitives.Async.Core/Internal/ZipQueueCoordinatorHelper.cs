// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Internal;

/// <summary>Synchronized queue-pairing and completion transitions shared by both sides of <c>ZipState</c>.</summary>
internal static class ZipQueueCoordinatorHelper
{
    /// <summary>Dequeues a waiting counterpart element under <paramref name="gate"/> or enqueues <paramref name="value"/> when the counterpart queue is empty.</summary>
    /// <typeparam name="TSelf">The incoming element type.</typeparam>
    /// <typeparam name="TCounterpart">The counterpart element type.</typeparam>
    /// <param name="gate">The synchronization lock.</param>
    /// <param name="done">Whether the zip sequence has already terminated.</param>
    /// <param name="selfQueue">The queue for the incoming side.</param>
    /// <param name="value">The incoming element.</param>
    /// <param name="counterpartQueue">The queue for the counterpart side.</param>
    /// <param name="counterpartCompleted">The counterpart side's completion flag, read under <paramref name="gate"/>.</param>
    /// <returns>A <see cref="ZipDequeueOutcome{TCounterpart}"/> indicating whether a pair was matched and whether the sequence completes after emitting it.</returns>
    internal static ZipDequeueOutcome<TCounterpart> DequeueOrEnqueue<TSelf, TCounterpart>(
        Lock gate,
        ref bool done,
        Queue<TSelf> selfQueue,
        TSelf value,
        Queue<TCounterpart> counterpartQueue,
        ref readonly bool counterpartCompleted)
    {
        // The counterpart flag is taken by reference so it is read under the lock. A copy taken before the lock can miss a
        // completion that lands in between, and then the pair that drains the last counterpart value never completes the zip.
        lock (gate)
        {
            if (done)
            {
                return default;
            }

            if (counterpartQueue.Count == 0)
            {
                selfQueue.Enqueue(value);
                return default;
            }

            var counterpart = counterpartQueue.Dequeue();
            var completeAfterPair = false;
            if (counterpartCompleted && counterpartQueue.Count == 0)
            {
                done = true;
                selfQueue.Clear();
                completeAfterPair = true;
            }

            return new(true, counterpart, completeAfterPair);
        }
    }

    /// <summary>Marks one side completed under <paramref name="gate"/> and determines whether downstream completion should be sent now.</summary>
    /// <typeparam name="TSelf">The completing side's element type.</typeparam>
    /// <typeparam name="TCounterpart">The counterpart side's element type.</typeparam>
    /// <param name="gate">The synchronization lock.</param>
    /// <param name="done">Whether the zip sequence has already terminated.</param>
    /// <param name="sideCompleted">The completion flag for the completing side.</param>
    /// <param name="counterpartCompleted">The counterpart side's completion flag, read under <paramref name="gate"/>.</param>
    /// <param name="selfQueue">The completing side's buffered elements.</param>
    /// <param name="counterpartQueue">The counterpart side's buffered elements.</param>
    /// <param name="result">The completion result reported by the completing side.</param>
    /// <returns><see langword="true"/> when the zip sequence transitioned to terminated and should complete downstream now; otherwise, <see langword="false"/>.</returns>
    internal static bool MarkSideCompleted<TSelf, TCounterpart>(
        Lock gate,
        ref bool done,
        ref bool sideCompleted,
        ref readonly bool counterpartCompleted,
        Queue<TSelf> selfQueue,
        Queue<TCounterpart> counterpartQueue,
        Result result)
    {
        lock (gate)
        {
            if (done)
            {
                return false;
            }

            sideCompleted = true;
            var shouldComplete = result.IsFailure || counterpartCompleted || selfQueue.Count == 0;
            if (shouldComplete)
            {
                done = true;
                selfQueue.Clear();
                counterpartQueue.Clear();
            }

            return shouldComplete;
        }
    }
}
