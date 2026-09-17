// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Disposables;

/// <summary>A disposable slot that accepts one assignment: a second assignment throws, and a value assigned after disposal is disposed immediately.</summary>
[System.Diagnostics.DebuggerDisplay("SingleDisposable: IsDisposed = {IsDisposed}")]
public sealed class SingleDisposable : IsDisposed
{
    /// <summary>The slot state.</summary>
    private AssignmentState _state;

    /// <summary>Initializes a new instance of the <see cref="SingleDisposable"/> class.</summary>
    public SingleDisposable()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SingleDisposable"/> class.</summary>
    /// <param name="action">Action to invoke before the assigned disposable is disposed.</param>
    public SingleDisposable(Action? action) => _state = new(action);

    /// <summary>Initializes a new instance of the <see cref="SingleDisposable"/> class.</summary>
    /// <param name="disposable">The disposable.</param>
    public SingleDisposable(IDisposable disposable)
        : this(disposable, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SingleDisposable"/> class.</summary>
    /// <param name="disposable">The disposable.</param>
    /// <param name="action">Action to invoke before the assigned disposable is disposed.</param>
    public SingleDisposable(IDisposable disposable, Action? action)
        : this(action) => _state.Create(disposable);

    /// <summary>Gets a value indicating whether this instance is disposed.</summary>
    public bool IsDisposed => _state.IsDisposed;

    /// <summary>Assigns the disposable held by this slot.</summary>
    /// <param name="disposable">The disposable.</param>
    /// <exception cref="ArgumentNullException"><paramref name="disposable"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">The slot holds an earlier assignment.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Create(IDisposable disposable) => _state.Create(disposable);

    /// <summary>Runs the constructor-supplied action, then disposes the assigned value and blocks further assignments; repeated calls have no further effect.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => _state.Dispose();
}
