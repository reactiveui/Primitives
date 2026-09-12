// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Disposables;

/// <summary>
/// Zero-allocation static helpers that implement <see cref="SingleReplaceableDisposableAsync"/>-style swap
/// and <see cref="SingleAssignmentDisposableAsync"/>-style single-assignment semantics directly
/// against a caller-owned <see cref="IAsyncDisposable"/> field. Use these when the wrapper-class
/// allocation that the convenience types incur is on a hot path.
/// </summary>
public static class DisposableAsyncSlot
{
    /// <summary>Shared marker used by all async-disposable slot implementations once a slot is closed.</summary>
    internal static readonly IAsyncDisposable DisposedSentinel = new DisposedAsyncDisposable();

    /// <summary>Swaps the slot's current contents with <paramref name="value"/> and asynchronously
    /// disposes the previous occupant. Equivalent to
    /// <see cref="SingleReplaceableDisposableAsync.SetDisposableAsync"/>, but operates on a caller-owned field
    /// so no wrapper instance is allocated.</summary>
    /// <param name="slot">Reference to the caller-owned <see cref="IAsyncDisposable"/> field.</param>
    /// <param name="value">The new value to store, or <see langword="null"/> to clear the slot.</param>
    /// <returns>A <see cref="ValueTask"/> that completes once the previous occupant (if any) has been disposed.</returns>
    [DebuggerStepThrough]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask SwapAsync(ref IAsyncDisposable? slot, IAsyncDisposable? value) =>
        SwapObservedAsync(ref slot, value, Volatile.Read(ref slot));

    /// <summary>Atomically fills an empty slot with <paramref name="value"/>. A closed slot disposes
    /// <paramref name="value"/> instead. Equivalent to
    /// <see cref="SingleAssignmentDisposableAsync.SetDisposableAsync(IAsyncDisposable?)"/>.</summary>
    /// <param name="slot">Reference to the caller-owned <see cref="IAsyncDisposable"/> field.</param>
    /// <param name="value">The value to assign, or <see langword="null"/>.</param>
    /// <returns>A <see cref="ValueTask"/> that completes once <paramref name="value"/> has been disposed when the slot
    /// was closed; otherwise a completed task.</returns>
    /// <exception cref="InvalidOperationException">The slot holds a live occupant.</exception>
    [DebuggerStepThrough]
    public static ValueTask AssignAsync(ref IAsyncDisposable? slot, IAsyncDisposable? value)
    {
        var current = Interlocked.CompareExchange(ref slot, value, null);
        if (current is null)
        {
            return default;
        }

        return ReferenceEquals(current, DisposedSentinel)
            ? value?.DisposeAsync() ?? default
            : throw CreateAlreadyAssignedException();
    }

    /// <summary>Asynchronously disposes the slot's current contents and marks the slot as disposed.
    /// Subsequent <see cref="SwapAsync"/> / <see cref="AssignAsync"/> calls will dispose their incoming
    /// value rather than store it. Idempotent.</summary>
    /// <param name="slot">Reference to the caller-owned <see cref="IAsyncDisposable"/> field.</param>
    /// <returns>A <see cref="ValueTask"/> that completes once the prior occupant has been disposed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [DebuggerStepThrough]
    public static ValueTask DisposeAsync(ref IAsyncDisposable? slot)
    {
        var current = Interlocked.Exchange(ref slot, DisposedSentinel);
        return current is null || ReferenceEquals(current, DisposedSentinel) ? default : current.DisposeAsync();
    }

    /// <summary>Returns <see langword="true"/> if the slot has been disposed via <see cref="DisposeAsync"/>.</summary>
    /// <param name="slot">The slot field to inspect.</param>
    /// <returns><see langword="true"/> if the slot currently holds the disposed sentinel.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsDisposed(IAsyncDisposable? slot) =>
        ReferenceEquals(slot, DisposedSentinel);

    /// <summary>Retries a swap when the slot changed after the caller's observation.</summary>
    /// <param name="slot">The slot to replace.</param>
    /// <param name="value">The incoming disposable.</param>
    /// <param name="current">The slot value the caller observed.</param>
    /// <returns>Disposal of the replaced value, or the incoming value when the slot is closed.</returns>
    internal static ValueTask SwapObservedAsync(ref IAsyncDisposable? slot, IAsyncDisposable? value, IAsyncDisposable? current)
    {
        while (true)
        {
            if (ReferenceEquals(current, DisposedSentinel))
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

    /// <summary>Creates the exception for a second assignment into a single-assignment slot.</summary>
    /// <returns>The invalid-operation exception to throw from the assignment path.</returns>
    internal static InvalidOperationException CreateAlreadyAssignedException() =>
        new("Disposable is already assigned.");

    /// <summary>Sentinel no-op disposable that marks a closed slot.</summary>
    private sealed class DisposedAsyncDisposable : IAsyncDisposable
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IAsyncDisposable.DisposeAsync() => default;
    }
}
