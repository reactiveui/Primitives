// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Disposables;

/// <summary>Primitives alias for a replaceable disposable slot.</summary>
[System.Diagnostics.DebuggerDisplay("Slot: IsDisposed = {IsDisposed}")]
public sealed class Slot : IsDisposed
{
    /// <summary>The slot state.</summary>
    private ReplaceableState _state;

    /// <summary>Initializes a new instance of the <see cref="Slot"/> class.</summary>
    public Slot()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="Slot"/> class.</summary>
    /// <param name="action">Action to call when the slot is disposed.</param>
    public Slot(Action? action) => _state = new(action);

    /// <summary>Initializes a new instance of the <see cref="Slot"/> class.</summary>
    /// <param name="disposable">Initial disposable.</param>
    public Slot(IDisposable disposable)
        : this(disposable, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="Slot"/> class.</summary>
    /// <param name="disposable">Initial disposable.</param>
    /// <param name="action">Action to call when the slot is disposed.</param>
    public Slot(IDisposable disposable, Action? action)
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
}
