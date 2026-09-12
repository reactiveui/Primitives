// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Disposables;

/// <summary>
/// Represents an asynchronously disposable resource that allows a single assignment of its underlying disposable. Once
/// disposed, further assignments will dispose the assigned resource immediately.
/// </summary>
/// <remarks>A second assignment throws <see cref="InvalidOperationException"/>, so this type suits the common shape
/// where a subscription handle has to be stored before the work it cancels can produce it.</remarks>
[System.Diagnostics.DebuggerDisplay("SingleAssignmentDisposableAsync: IsDisposed = {IsDisposed}, Current = {_current}")]
public sealed class SingleAssignmentDisposableAsync : IAsyncDisposable
{
    /// <summary>The assigned disposable, or the sentinel that marks the slot closed.</summary>
    private IAsyncDisposable? _current;

    /// <summary>Gets a value indicating whether the object has been disposed.</summary>
    public bool IsDisposed => DisposableAsyncSlot.IsDisposed(Volatile.Read(ref _current));

    /// <summary>Gets the assigned disposable, hiding the internal sentinel behind <see cref="DisposableAsync.Empty"/>.</summary>
    /// <returns>The assigned resource, <see cref="DisposableAsync.Empty"/> after disposal, or <see langword="null"/>
    /// when nothing has been assigned.</returns>
    public IAsyncDisposable? GetDisposable()
    {
        var field = Volatile.Read(ref _current);
        return DisposableAsyncSlot.IsDisposed(field) ? DisposableAsync.Empty : field;
    }

    /// <summary>Assigns the resource this instance owns, disposing <paramref name="value"/> on the spot when this instance has been disposed.</summary>
    /// <param name="value">The <see cref="IAsyncDisposable"/> to take ownership of, or <see langword="null"/>.</param>
    /// <returns>A <see cref="ValueTask"/> that completes once any disposal this call triggered has finished.</returns>
    /// <exception cref="InvalidOperationException">A resource has been assigned by an earlier call.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask SetDisposableAsync(IAsyncDisposable? value) => AssignDisposableAsync(ref _current, value);

    /// <summary>Disposes the assigned resource and closes the slot, so a later assignment disposes its argument.</summary>
    /// <returns>A <see cref="ValueTask"/> that completes once the assigned resource has been disposed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => DisposeAsync(ref _current);

    /// <summary>Assigns <paramref name="value"/> into an empty caller-owned field, with no wrapper instance.</summary>
    /// <param name="field">A reference to the field that takes ownership of <paramref name="value"/>.</param>
    /// <param name="value">The <see cref="IAsyncDisposable"/> to assign, or <see langword="null"/>.</param>
    /// <returns>A <see cref="ValueTask"/> that completes once any disposal this call triggered has finished.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ValueTask AssignDisposableAsync(ref IAsyncDisposable? field, IAsyncDisposable? value) =>
        DisposableAsyncSlot.AssignAsync(ref field, value);

    /// <summary>Disposes a caller-owned field's occupant once and leaves the field holding the closed sentinel.</summary>
    /// <param name="field">A reference to the <see cref="IAsyncDisposable"/> field to close.</param>
    /// <returns>A <see cref="ValueTask"/> that completes once the occupant has been disposed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [DebuggerStepThrough]
    internal static ValueTask DisposeAsync(ref IAsyncDisposable? field) =>
        DisposableAsyncSlot.DisposeAsync(ref field);

    /// <summary>Creates the exception for a second assignment.</summary>
    /// <returns>The <see cref="InvalidOperationException"/> to throw from the assignment path.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static InvalidOperationException CreateAlreadyAssignedException() =>
        DisposableAsyncSlot.CreateAlreadyAssignedException();
}
