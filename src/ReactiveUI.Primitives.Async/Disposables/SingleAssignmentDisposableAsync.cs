// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.Async.Disposables;

/// <summary>
/// Represents an asynchronously disposable resource that allows a single assignment of its underlying disposable. Once
/// disposed, further assignments will dispose the assigned resource immediately.
/// </summary>
/// <remarks>This type is useful for scenarios where an asynchronous disposable resource must be assigned exactly
/// once, and where disposal may occur before or after the assignment. If disposed before assignment, any subsequently
/// assigned resource will be disposed immediately. This class is not thread-safe for concurrent assignment and
/// disposal; external synchronization is required if used from multiple threads.</remarks>
public sealed class SingleAssignmentDisposableAsync : IAsyncDisposable
{
    /// <summary>
    /// The currently assigned disposable resource, or the disposed sentinel if already disposed.
    /// </summary>
    private IAsyncDisposable? _current;

    /// <summary>
    /// Gets a value indicating whether the object has been disposed.
    /// </summary>
    public bool IsDisposed => ReferenceEquals(Volatile.Read(ref _current), DisposedSlotMarker.Instance);

    /// <summary>
    /// Gets the current asynchronous disposable resource, or an empty disposable if the resource has already been
    /// disposed.
    /// </summary>
    /// <returns>An <see cref="IAsyncDisposable"/> representing the current resource, or <see cref="DisposableAsync.Empty"/> if
    /// the resource has been disposed. Returns <see langword="null"/> if no resource is set.</returns>
    public IAsyncDisposable? GetDisposable()
    {
        var field = Volatile.Read(ref _current);
        if (ReferenceEquals(field, DisposedSlotMarker.Instance))
        {
            return DisposableAsync.Empty;
        }

        return field;
    }

    /// <summary>
    /// Asynchronously sets the current disposable resource to the specified value, replacing any previously set
    /// resource.
    /// </summary>
    /// <param name="value">The new <see cref="IAsyncDisposable"/> instance to set as the current resource, or <see langword="null"/> to
    /// clear the current resource.</param>
    /// <returns>A <see cref="ValueTask"/> that represents the asynchronous operation.</returns>
    public ValueTask SetDisposableAsync(IAsyncDisposable? value) => AssignDisposableAsync(ref _current, value);

    /// <summary>
    /// Asynchronously releases the unmanaged resources used by the object.
    /// </summary>
    /// <returns>A ValueTask that represents the asynchronous dispose operation.</returns>
    public ValueTask DisposeAsync() => DisposeAsync(ref _current);

    /// <summary>
    /// Atomically assigns an asynchronous disposable object to the specified field if it has not already been set.
    /// </summary>
    /// <remarks>If the field has already been assigned or disposed, the method either throws an exception or
    /// disposes the provided value, as appropriate. This method is intended for use in thread-safe scenarios where a
    /// disposable resource should only be set once.</remarks>
    /// <param name="field">A reference to the field that will hold the assigned <see cref="IAsyncDisposable"/> instance. The field must
    /// initially be null.</param>
    /// <param name="value">The <see cref="IAsyncDisposable"/> instance to assign to the field, or null to leave the field unset.</param>
    /// <returns>A <see cref="ValueTask"/> that represents the asynchronous dispose operation if the field was already disposed;
    /// otherwise, a default <see cref="ValueTask"/>.</returns>
    internal static ValueTask AssignDisposableAsync(ref IAsyncDisposable? field, IAsyncDisposable? value)
    {
        var current = Interlocked.CompareExchange(ref field, value, null);
        if (current == null)
        {
            // ok to set.
            return default;
        }

        if (ReferenceEquals(current, DisposedSlotMarker.Instance))
        {
            if (value is not null)
            {
                return value.DisposeAsync();
            }

            return default;
        }

        throw CreateAlreadyAssignedException();
    }

    /// <summary>
    /// Asynchronously disposes the object referenced by the specified field, if it has not already been disposed.
    /// </summary>
    /// <remarks>This method is intended for use in thread-safe disposal patterns to ensure that the
    /// referenced object is disposed only once. After calling this method, the field will reference a sentinel value
    /// indicating it has been disposed.</remarks>
    /// <param name="field">A reference to an <see cref="IAsyncDisposable"/> field to be disposed. The field will be set to a sentinel value
    /// to prevent multiple disposals.</param>
    /// <returns>A <see cref="ValueTask"/> that represents the asynchronous dispose operation. The returned task is completed if
    /// the field was already disposed or null.</returns>
    [DebuggerStepThrough]
    internal static ValueTask DisposeAsync(ref IAsyncDisposable? field)
    {
        var current = Interlocked.Exchange(ref field, DisposedSlotMarker.Instance);
        if (ReferenceEquals(current, DisposedSlotMarker.Instance) || current is null)
        {
            return default;
        }

        return current.DisposeAsync();
    }

    /// <summary>
    /// Creates an exception indicating that the disposable has already been assigned.
    /// </summary>
    /// <returns>An <see cref="InvalidOperationException"/> with the already-assigned message.</returns>
    internal static InvalidOperationException CreateAlreadyAssignedException() =>
        new("Disposable is already assigned.");

    /// <summary>
    /// A sentinel object used to indicate that the <see cref="SingleAssignmentDisposableAsync"/> has been disposed.
    /// </summary>
    internal sealed class DisposedSlotMarker : IAsyncDisposable
    {
        /// <summary>
        /// Gets the singleton instance of <see cref="DisposedSlotMarker"/>.
        /// </summary>
        public static readonly DisposedSlotMarker Instance = new();

        /// <inheritdoc/>
        ValueTask IAsyncDisposable.DisposeAsync() => default;
    }
}
