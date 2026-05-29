// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>
/// BooleanDisposable.
/// </summary>
/// <seealso cref="Disposables.IsDisposed" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed partial class BooleanDisposable : IsDisposed
{
    /// <summary>
    /// Disposed latch; 0 when alive, 1 once disposed.
    /// </summary>
    private int _isDisposed;

    /// <summary>
    /// Gets a value indicating whether this instance is disposed.
    /// </summary>
    /// <value>
    ///   <c>true</c> if this instance is disposed; otherwise, <c>false</c>.
    /// </value>
    public bool IsDisposed => Volatile.Read(ref _isDisposed) != 0;

    /// <summary>
    /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
    /// </summary>
    public void Dispose() => Interlocked.Exchange(ref _isDisposed, 1);
}
