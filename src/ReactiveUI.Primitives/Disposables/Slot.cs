// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>
/// Primitives alias for a replaceable disposable slot.
/// </summary>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class Slot : SingleReplaceableDisposable
{
    /// <summary>
    /// Initializes a new instance of the <see cref="Slot"/> class.
    /// </summary>
    public Slot()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Slot"/> class.
    /// </summary>
    /// <param name="action">Action to call when the slot is disposed.</param>
    public Slot(Action? action)
        : base(action)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Slot"/> class.
    /// </summary>
    /// <param name="disposable">Initial disposable.</param>
    public Slot(IDisposable disposable)
        : base(disposable)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Slot"/> class.
    /// </summary>
    /// <param name="disposable">Initial disposable.</param>
    /// <param name="action">Action to call when the slot is disposed.</param>
    public Slot(IDisposable disposable, Action? action)
        : base(disposable, action)
    {
    }

    /// <summary>
    /// Gets the debugger display text.
    /// </summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;
}
