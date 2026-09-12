// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>A disposable slot whose inner disposable can be replaced, disposing the previous one.</summary>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public class SingleReplaceableDisposable : IsDisposed
{
    /// <summary>Marker used once the slot has been disposed.</summary>
    private static readonly IDisposable DisposedSentinel = new DisposedMarker();

    /// <summary>Action invoked before disposal.</summary>
    private readonly Action? _action;

    /// <summary>Current disposable or the disposed marker.</summary>
    private IDisposable? _disposable;

    /// <summary>Initializes a new instance of the <see cref="SingleReplaceableDisposable"/> class.</summary>
    public SingleReplaceableDisposable()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SingleReplaceableDisposable"/> class.</summary>
    /// <param name="action">The action.</param>
    public SingleReplaceableDisposable(Action? action) =>
        _action = action;

    /// <summary>Initializes a new instance of the <see cref="SingleReplaceableDisposable"/> class.</summary>
    /// <param name="disposable">The disposable.</param>
    public SingleReplaceableDisposable(IDisposable disposable)
        : this(disposable, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SingleReplaceableDisposable"/> class.</summary>
    /// <param name="disposable">The disposable.</param>
    /// <param name="action">The action to call before disposal.</param>
    public SingleReplaceableDisposable(IDisposable disposable, Action? action)
    {
        _action = action;
        Create(disposable);
    }

    /// <summary>Gets a value indicating whether this instance is disposed.</summary>
    public bool IsDisposed => ReferenceEquals(Volatile.Read(ref _disposable), DisposedSentinel);

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Assigns the inner disposable and disposes the value it displaces; once this slot is disposed the incoming value is disposed instead.</summary>
    /// <param name="disposable">The disposable to take as the new inner value.</param>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="disposable"/> is <see langword="null"/>.</exception>
    public void Create(IDisposable disposable)
    {
        ArgumentExceptionHelper.ThrowIfNull(disposable);
        CreateWithRetry(disposable);
    }

    /// <summary>Disposes the inner value and blocks further assignments; repeated calls have no further effect.</summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>Replaces the observed value, or disposes the incoming value when the slot is closed.</summary>
    /// <param name="current">The observed slot value.</param>
    /// <param name="disposable">The incoming disposable.</param>
    /// <returns>True when the incoming value was handled; false when the observed slot was stale.</returns>
    internal bool TryCreate(IDisposable? current, IDisposable disposable)
    {
        if (ReferenceEquals(current, DisposedSentinel))
        {
            disposable.Dispose();
            _action?.Invoke();
            return true;
        }

        if (!ReferenceEquals(Interlocked.CompareExchange(ref _disposable, disposable, current), current))
        {
            return false;
        }

        current?.Dispose();
        return true;
    }

    /// <summary>Disposes the inner value and then invokes the constructor-supplied action, once.</summary>
    /// <param name="disposing"><see langword="true"/> when invoked from <see cref="Dispose()"/>.</param>
    protected virtual void Dispose(bool disposing)
    {
        var old = Interlocked.Exchange(ref _disposable, DisposedSentinel);
        if (ReferenceEquals(old, DisposedSentinel))
        {
            return;
        }

        old?.Dispose();
        _action?.Invoke();
    }

    /// <summary>Retries replacement until the observed slot is current.</summary>
    /// <param name="disposable">The incoming disposable.</param>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private void CreateWithRetry(IDisposable disposable)
    {
        while (true)
        {
            if (TryCreate(Volatile.Read(ref _disposable), disposable))
            {
                return;
            }
        }
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
