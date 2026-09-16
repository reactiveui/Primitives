// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Disposables;

/// <summary>A disposable slot whose inner disposable can be replaced, disposing the previous one.</summary>
[System.Diagnostics.DebuggerDisplay("SingleReplaceableDisposable: IsDisposed = {IsDisposed}")]
public sealed class SingleReplaceableDisposable : IsDisposed
{
    /// <summary>The slot state.</summary>
    private ReplaceableState _state;

    /// <summary>Initializes a new instance of the <see cref="SingleReplaceableDisposable"/> class.</summary>
    public SingleReplaceableDisposable()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SingleReplaceableDisposable"/> class.</summary>
    /// <param name="action">The action.</param>
    public SingleReplaceableDisposable(Action? action) => _state = new(action);

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
        : this(action) => _state.Create(disposable);

    /// <summary>Gets a value indicating whether this instance is disposed.</summary>
    public bool IsDisposed => _state.IsDisposed;

    /// <summary>Assigns the inner disposable and disposes the value it displaces; once this slot is disposed the incoming value is disposed instead.</summary>
    /// <param name="disposable">The disposable to take as the new inner value.</param>
    /// <exception cref="ArgumentNullException"><paramref name="disposable"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Create(IDisposable disposable) => _state.Create(disposable);

    /// <summary>Disposes the inner value, invokes the constructor-supplied action and blocks further assignments; repeated calls have no further effect.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => _state.Dispose();

    /// <summary>Replaces the observed value, or disposes the incoming value when the slot is closed.</summary>
    /// <param name="current">The observed slot value.</param>
    /// <param name="disposable">The incoming disposable.</param>
    /// <returns>True when the incoming value was handled; false when the observed slot was stale.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryCreate(IDisposable? current, IDisposable disposable) => _state.TryCreate(current, disposable);
}
