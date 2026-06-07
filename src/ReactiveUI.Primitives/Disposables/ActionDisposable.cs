// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>
/// An <see cref="IDisposable"/> that runs the supplied <see cref="Action"/> exactly once on
/// <see cref="Dispose"/>. Replaces <c>new ActionDisposable(Action)</c>.
/// </summary>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class ActionDisposable : IsDisposed
{
    /// <summary>
    /// The action to invoke once on dispose.
    /// </summary>
    private Action? _action;

    /// <summary>
    /// Initializes a new instance of the <see cref="ActionDisposable"/> class.
    /// </summary>
    /// <param name="action">The action to invoke once on dispose.</param>
    public ActionDisposable(Action action) => _action = action;

    /// <summary>
    /// Gets a value indicating whether this instance has been disposed.
    /// </summary>
    public bool IsDisposed => Volatile.Read(ref _action) is null;

    /// <summary>
    /// Gets the debugger display text.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    public void Dispose() => Interlocked.Exchange(ref _action, null)?.Invoke();
}
