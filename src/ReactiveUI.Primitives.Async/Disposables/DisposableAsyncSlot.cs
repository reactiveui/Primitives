// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async.Disposables;

/// <summary>
/// Zero-allocation static helpers that implement <see cref="SingleReplaceableDisposableAsync"/>-style swap
/// and <see cref="SingleAssignmentDisposableAsync"/>-style single-assignment semantics directly
/// against a caller-owned <see cref="IAsyncDisposable"/> field. Use these when the wrapper-class
/// allocation that the convenience types incur is on a hot path.
/// </summary>
[SuppressMessage("Design", "CA1045:Do not pass types by reference", Justification = "Ref-on-field is the entire point — mirrors Interlocked/Volatile.")]
public static class DisposableAsyncSlot
{
    /// <summary>Swaps the slot's current contents with <paramref name="value"/> and asynchronously
    /// disposes the previous occupant. Equivalent to
    /// <see cref="SingleReplaceableDisposableAsync.SetDisposableAsync"/>, but operates on a caller-owned field
    /// so no wrapper instance is allocated.</summary>
    /// <param name="slot">Reference to the caller-owned <see cref="IAsyncDisposable"/> field.</param>
    /// <param name="value">The new value to store, or <see langword="null"/> to clear the slot.</param>
    /// <returns>A <see cref="ValueTask"/> that completes once the previous occupant (if any) has been disposed.</returns>
    /// <remarks>The compare-exchange retry (the loop back-edge) is only taken when a concurrent writer
    /// wins the race, so it is unreachable by single-threaded tests; excluded from coverage accordingly.</remarks>
    [DebuggerStepThrough]
    [ExcludeFromCodeCoverage]
    public static ValueTask SwapAsync(ref IAsyncDisposable? slot, IAsyncDisposable? value)
    {
        var current = Volatile.Read(ref slot);
        while (true)
        {
            if (ReferenceEquals(current, DisposedSlotMarker.Instance))
            {
                return value?.DisposeAsync() ?? default;
            }

            var exchanged = Interlocked.CompareExchange(ref slot, value, current);
            if (ReferenceEquals(exchanged, current))
            {
                return current?.DisposeAsync() ?? default;
            }

            current = exchanged;
        }
    }

    /// <summary>Atomically assigns <paramref name="value"/> to the slot exactly once. If the slot has
    /// already been disposed, <paramref name="value"/> is disposed immediately. If the slot already
    /// holds a non-null, non-disposed value, throws <see cref="InvalidOperationException"/>.
    /// Equivalent to <see cref="SingleAssignmentDisposableAsync.SetDisposableAsync(IAsyncDisposable?)"/>.</summary>
    /// <param name="slot">Reference to the caller-owned <see cref="IAsyncDisposable"/> field.</param>
    /// <param name="value">The value to assign, or <see langword="null"/>.</param>
    /// <returns>A <see cref="ValueTask"/> that completes once <paramref name="value"/> has been disposed
    /// (if the slot was already disposed); otherwise a completed task.</returns>
    [DebuggerStepThrough]
    public static ValueTask AssignAsync(ref IAsyncDisposable? slot, IAsyncDisposable? value)
    {
        var current = Interlocked.CompareExchange(ref slot, value, null);
        if (current is null)
        {
            return default;
        }

        if (ReferenceEquals(current, DisposedSlotMarker.Instance))
        {
            return value?.DisposeAsync() ?? default;
        }

        throw new InvalidOperationException("Disposable is already assigned.");
    }

    /// <summary>Asynchronously disposes the slot's current contents and marks the slot as disposed.
    /// Subsequent <see cref="SwapAsync"/> / <see cref="AssignAsync"/> calls will dispose their incoming
    /// value rather than store it. Idempotent.</summary>
    /// <param name="slot">Reference to the caller-owned <see cref="IAsyncDisposable"/> field.</param>
    /// <returns>A <see cref="ValueTask"/> that completes once the prior occupant has been disposed.</returns>
    [DebuggerStepThrough]
    public static ValueTask DisposeAsync(ref IAsyncDisposable? slot)
    {
        var current = Interlocked.Exchange(ref slot, DisposedSlotMarker.Instance);
        if (current is null || ReferenceEquals(current, DisposedSlotMarker.Instance))
        {
            return default;
        }

        return current.DisposeAsync();
    }

    /// <summary>Returns <see langword="true"/> if the slot has been disposed via <see cref="DisposeAsync"/>.</summary>
    /// <param name="slot">The slot field to inspect.</param>
    /// <returns><see langword="true"/> if the slot currently holds the disposed sentinel.</returns>
    public static bool IsDisposed(IAsyncDisposable? slot) =>
        ReferenceEquals(slot, DisposedSlotMarker.Instance);

    /// <summary>Shared sentinel marking a disposed slot. Distinct from the per-class sentinels in
    /// <see cref="SingleReplaceableDisposableAsync"/> and <see cref="SingleAssignmentDisposableAsync"/> so the
    /// slot helpers can be used independently of (and alongside) those wrapper classes.</summary>
    internal sealed class DisposedSlotMarker : IAsyncDisposable
    {
        /// <summary>Singleton sentinel instance.</summary>
        public static readonly DisposedSlotMarker Instance = new();

        /// <inheritdoc/>
        ValueTask IAsyncDisposable.DisposeAsync() => default;
    }
}
