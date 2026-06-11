// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Disposables;

/// <summary>
/// Pure-plumbing helpers for the swap-disposable-slot pattern shared by
/// <see cref="MutableDisposable"/> and <see cref="SwapDisposable"/>. Centralizes the
/// pre-check / store / race-recheck flow so the call-site setters stay one-line delegations.
/// All testable branches (already-disposed pre-check, steady-state assign, idempotent dispose)
/// have direct RxVoid tests against this class. The single race-recheck step that fires only
/// when <c>Dispose()</c> runs concurrently between the helper's <c>Volatile.Read</c> pre-check
/// and the store is isolated in <see cref="DisposeIfRaced"/>, which is marked
/// <see cref="ExcludeFromCodeCoverageAttribute"/>. That step is unreachable without a real
/// concurrent thread, in the same spirit as the library's throw-helper methods.
/// </summary>
internal static class DisposableSlotHelper
{
    /// <summary>Sentinel value indicating the holder has been disposed.</summary>
    public const int DisposedSentinel = 1;

    /// <summary>
    /// Reassigns an inner disposable slot WITHOUT disposing the previous value (mutable-assign
    /// semantics, matching the <see cref="MutableDisposable"/> contract). If the holder is
    /// already disposed, the incoming value is disposed immediately; if Dispose races between
    /// the pre-check and the store, the just-stored value is disposed via
    /// <see cref="DisposeIfRaced"/>.
    /// </summary>
    /// <param name="slot">The reference to the current-inner field.</param>
    /// <param name="disposed">The reference to the disposed-flag field.</param>
    /// <param name="value">The incoming value (or <see langword="null"/>).</param>
    public static void AssignWithoutDisposingPrevious(
        ref IDisposable? slot,
        ref int disposed,
        IDisposable? value)
    {
        if (Volatile.Read(ref disposed) == DisposedSentinel)
        {
            value?.Dispose();
            return;
        }

        Interlocked.Exchange(ref slot, value);
        DisposeIfRaced(ref slot, ref disposed);
    }

    /// <summary>
    /// Reassigns an inner disposable slot and disposes the previous value (swap semantics,
    /// matching the <see cref="SwapDisposable"/> contract). If the holder is already disposed,
    /// the incoming value is disposed immediately; if Dispose races between the swap and the
    /// recheck, the just-stored value is disposed via <see cref="DisposeIfRaced"/>.
    /// </summary>
    /// <param name="slot">The reference to the current-inner field.</param>
    /// <param name="disposed">The reference to the disposed-flag field.</param>
    /// <param name="value">The incoming value (or <see langword="null"/>).</param>
    public static void SwapAndDisposePrevious(
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

    /// <summary>
    /// Performs the standard idempotent dispose step: latches the disposed flag and disposes
    /// the current inner (if any). Returns <see langword="true"/> if this was the first call
    /// and the caller should clean up; <see langword="false"/> if a prior dispose has already
    /// done the work.
    /// </summary>
    /// <param name="slot">The reference to the current-inner field.</param>
    /// <param name="disposed">The reference to the disposed-flag field.</param>
    /// <returns>
    /// <see langword="true"/> if the current invocation latched the flag; otherwise
    /// <see langword="false"/>.
    /// </returns>
    public static bool TryDispose(ref IDisposable? slot, ref int disposed)
    {
        if (Interlocked.Exchange(ref disposed, DisposedSentinel) == DisposedSentinel)
        {
            return false;
        }

        Interlocked.Exchange(ref slot, null)?.Dispose();
        return true;
    }

    /// <summary>
    /// Race-only cleanup: if <c>Dispose()</c> ran concurrently between the setter's pre-check
    /// and the slot store, swap the value out and dispose it to avoid leaking. The branch
    /// only fires when a real concurrent thread cancels in the TOCTOU window, which cannot
    /// be deterministically simulated in single-threaded RxVoid tests, hence the exclusion.
    /// </summary>
    /// <param name="slot">The reference to the current-inner field.</param>
    /// <param name="disposed">The reference to the disposed-flag field.</param>
    [ExcludeFromCodeCoverage]
    private static void DisposeIfRaced(ref IDisposable? slot, ref int disposed)
    {
        if (Volatile.Read(ref disposed) != DisposedSentinel)
        {
            return;
        }

        Interlocked.Exchange(ref slot, null)?.Dispose();
    }
}
