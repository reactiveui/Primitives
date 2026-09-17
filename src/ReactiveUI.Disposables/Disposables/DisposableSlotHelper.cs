// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>Assigns and disposes slots according to their holder's disposal state.</summary>
internal static class DisposableSlotHelper
{
    /// <summary>Sentinel value indicating the holder has been disposed.</summary>
    internal const int DisposedSentinel = 1;

    /// <summary>Replaces a slot without disposing its previous value; a disposed holder disposes the incoming value.</summary>
    /// <param name="slot">The reference to the current-inner field.</param>
    /// <param name="disposed">The reference to the disposed-flag field.</param>
    /// <param name="value">The incoming value (or <see langword="null"/>).</param>
    internal static void AssignWithoutDisposingPrevious(
        ref IDisposable? slot,
        ref int disposed,
        IDisposable? value)
    {
        if (Volatile.Read(ref disposed) == DisposedSentinel)
        {
            value?.Dispose();
            return;
        }

        _ = Interlocked.Exchange(ref slot, value);
        DisposeIfRaced(ref slot, ref disposed);
    }

    /// <summary>Replaces a slot and disposes its previous value; a disposed holder also disposes the incoming value.</summary>
    /// <param name="slot">The reference to the current-inner field.</param>
    /// <param name="disposed">The reference to the disposed-flag field.</param>
    /// <param name="value">The incoming value (or <see langword="null"/>).</param>
    internal static void SwapAndDisposePrevious(
        ref IDisposable? slot,
        ref int disposed,
        IDisposable? value)
    {
        if (Volatile.Read(ref disposed) == DisposedSentinel)
        {
            value?.Dispose();
            return;
        }

        var previous = Interlocked.Exchange(ref slot, value);
        previous?.Dispose();
        DisposeIfRaced(ref slot, ref disposed);
    }

    /// <summary>Claims disposal and releases the inner value, returning true only for the first caller.</summary>
    /// <param name="slot">The reference to the current-inner field.</param>
    /// <param name="disposed">The reference to the disposed-flag field.</param>
    /// <returns>
    /// <see langword="true"/> if the current invocation latched the flag; otherwise
    /// <see langword="false"/>.
    /// </returns>
    internal static bool TryDispose(ref IDisposable? slot, ref int disposed)
    {
        if (Interlocked.Exchange(ref disposed, DisposedSentinel) == DisposedSentinel)
        {
            return false;
        }

        Interlocked.Exchange(ref slot, null)?.Dispose();
        return true;
    }

    /// <summary>Releases the stored value if disposal overlaps assignment.</summary>
    /// <param name="slot">The reference to the current-inner field.</param>
    /// <param name="disposed">The reference to the disposed-flag field.</param>
    internal static void DisposeIfRaced(ref IDisposable? slot, ref int disposed)
    {
        if (Volatile.Read(ref disposed) != DisposedSentinel)
        {
            return;
        }

        Interlocked.Exchange(ref slot, null)?.Dispose();
    }
}
