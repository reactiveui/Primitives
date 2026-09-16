// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>The state of a disposable slot that accepts one assignment, embedded as a mutable field by the slot that owns it.</summary>
/// <remarks>A second assignment throws, and a value assigned after disposal is disposed immediately. Keep the field non-readonly.</remarks>
[System.Diagnostics.DebuggerDisplay("AssignmentState: IsDisposed = {IsDisposed}")]
internal record struct AssignmentState : IDisposable
{
    /// <summary>Marker used once the slot has been disposed.</summary>
    private static readonly IDisposable DisposedSentinel = new DisposedMarker();

    /// <summary>Action invoked before disposal.</summary>
    private readonly Action? _action;

    /// <summary>Assigned disposable or the disposed marker.</summary>
    private IDisposable? _disposable;

    /// <summary>Initializes a new instance of the <see cref="AssignmentState"/> struct.</summary>
    /// <param name="action">Action to invoke before the assigned disposable is disposed.</param>
    public AssignmentState(Action? action) => _action = action;

    /// <summary>Gets a value indicating whether the slot is disposed.</summary>
    public bool IsDisposed => ReferenceEquals(Volatile.Read(ref _disposable), DisposedSentinel);

    /// <summary>Runs the action and then disposes the assigned value, once.</summary>
    public void Dispose()
    {
        var disposable = Interlocked.Exchange(ref _disposable, DisposedSentinel);
        if (ReferenceEquals(disposable, DisposedSentinel))
        {
            return;
        }

        // The action runs whether or not a value was ever assigned; only the value is conditional.
        _action?.Invoke();
        disposable?.Dispose();
    }

    /// <summary>Assigns the disposable held by the slot.</summary>
    /// <param name="disposable">The disposable.</param>
    /// <exception cref="ArgumentNullException"><paramref name="disposable"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">The slot holds an earlier assignment.</exception>
    internal void Create(IDisposable disposable)
    {
        ArgumentExceptionHelper.ThrowIfNull(disposable);

        var current = Interlocked.CompareExchange(ref _disposable, disposable, null);
        if (current is null)
        {
            return;
        }

        if (ReferenceEquals(current, DisposedSentinel))
        {
            disposable.Dispose();
            return;
        }

        throw new InvalidOperationException($"The {nameof(disposable)} slot has already been assigned.");
    }

    /// <summary>Disposable marker for disposed slots.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private sealed class DisposedMarker : IDisposable
    {
        /// <inheritdoc/>
        public void Dispose()
        {
            // Disposing the terminal marker has no effect.
        }
    }
}
