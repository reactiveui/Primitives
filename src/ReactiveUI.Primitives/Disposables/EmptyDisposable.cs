// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>A no-op <see cref="IDisposable"/> singleton used in place of <c>EmptyDisposable.Instance</c>.</summary>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class EmptyDisposable : IDisposable
{
    /// <summary>Initializes a new instance of the <see cref="EmptyDisposable"/> class.</summary>
    private EmptyDisposable()
    {
    }

    /// <summary>Gets the shared singleton instance.</summary>
    public static EmptyDisposable Instance { get; } = new();

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    public void Dispose()
    {
    }
}
