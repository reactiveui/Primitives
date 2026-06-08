// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>Primitives alias for a single-assignment disposable slot.</summary>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class AssignmentSlot : SingleDisposable
{
    /// <summary>Initializes a new instance of the <see cref="AssignmentSlot"/> class.</summary>
    public AssignmentSlot()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="AssignmentSlot"/> class.</summary>
    /// <param name="action">Action to invoke before the assigned disposable is disposed.</param>
    public AssignmentSlot(Action? action)
        : base(action)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="AssignmentSlot"/> class.</summary>
    /// <param name="disposable">Initial assignment.</param>
    public AssignmentSlot(IDisposable disposable)
        : base(disposable)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="AssignmentSlot"/> class.</summary>
    /// <param name="disposable">Initial assignment.</param>
    /// <param name="action">Action to invoke before the assigned disposable is disposed.</param>
    public AssignmentSlot(IDisposable disposable, Action? action)
        : base(disposable, action)
    {
    }

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;
}
