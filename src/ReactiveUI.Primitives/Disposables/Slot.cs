// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>
/// Primitives alias for a replaceable disposable slot.
/// </summary>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
public sealed class Slot : SingleReplaceableDisposable
{
    /// <summary>
    /// Initializes a new instance of the <see cref="Slot"/> class.
    /// </summary>
    /// <param name="action">Action to call when the slot is disposed.</param>
    public Slot(Action? action = null)
        : base(action)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Slot"/> class.
    /// </summary>
    /// <param name="disposable">Initial disposable.</param>
    /// <param name="action">Action to call when the slot is disposed.</param>
    public Slot(IDisposable disposable, Action? action = null)
        : base(disposable, action)
    {
    }
}
