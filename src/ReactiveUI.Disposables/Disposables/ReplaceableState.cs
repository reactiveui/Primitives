// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>The state of a disposable slot whose inner value can be replaced, embedded as a mutable field by the slot that owns it.</summary>
/// <remarks>A replacement disposes the displaced value, and a value assigned after disposal is disposed immediately. Keep the field non-readonly.</remarks>
[System.Diagnostics.DebuggerDisplay("ReplaceableState: IsDisposed = {IsDisposed}")]
internal record struct ReplaceableState : IDisposable
{
    /// <summary>Marker used once the slot has been disposed.</summary>
    private static readonly IDisposable DisposedSentinel = new DisposedMarker();

    /// <summary>Action invoked on disposal.</summary>
    private readonly Action? _action;

    /// <summary>Current disposable or the disposed marker.</summary>
    private IDisposable? _disposable;

    /// <summary>Initializes a new instance of the <see cref="ReplaceableState"/> struct.</summary>
    /// <param name="action">The action to call on disposal.</param>
    public ReplaceableState(Action? action) => _action = action;

    /// <summary>Gets a value indicating whether the slot is disposed.</summary>
    public bool IsDisposed => ReferenceEquals(Volatile.Read(ref _disposable), DisposedSentinel);

    /// <summary>Disposes the inner value and then invokes the action, once.</summary>
    public void Dispose()
    {
        var old = Interlocked.Exchange(ref _disposable, DisposedSentinel);
        if (ReferenceEquals(old, DisposedSentinel))
        {
            return;
        }

        old?.Dispose();
        _action?.Invoke();
    }

    /// <summary>Assigns the inner disposable and disposes the value it displaces; once disposed the incoming value is disposed instead.</summary>
    /// <param name="disposable">The disposable to take as the new inner value.</param>
    /// <exception cref="ArgumentNullException"><paramref name="disposable"/> is <see langword="null"/>.</exception>
    internal void Create(IDisposable disposable)
    {
        ArgumentExceptionHelper.ThrowIfNull(disposable);
        CreateWithRetry(disposable);
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
