// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Internal;
#else
namespace ReactiveUI.Primitives.Internal;
#endif

/// <summary>Releases a scheduled timer stored in a slot that its owner's disposal can race.</summary>
internal static class TimerSlotHelper
{
    /// <summary>Disposes the stored timer when disposal raced the schedule that stored it.</summary>
    /// <param name="timer">The timer slot.</param>
    /// <param name="disposed">The owner's disposed flag; non-zero once disposed.</param>
    /// <remarks>The owner's dispose clears the slot itself, so the slot can already be empty when disposal wins the race.</remarks>
    internal static void ReleaseIfDisposed(ref IDisposable? timer, ref int disposed)
    {
        if (Volatile.Read(ref disposed) == 0)
        {
            return;
        }

        Interlocked.Exchange(ref timer, null)?.Dispose();
    }
}
