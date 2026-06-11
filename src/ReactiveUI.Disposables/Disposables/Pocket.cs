// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>Primitives alias for a group of disposables that are disposed together.</summary>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class Pocket : MultipleDisposable
{
    /// <summary>Initializes a new instance of the <see cref="Pocket"/> class.</summary>
    public Pocket()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="Pocket"/> class.</summary>
    /// <param name="first">The first disposable.</param>
    /// <param name="second">The second disposable.</param>
    public Pocket(IDisposable first, IDisposable second)
        : base(first, second)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="Pocket"/> class.</summary>
    /// <param name="first">The first disposable.</param>
    /// <param name="second">The second disposable.</param>
    /// <param name="third">The third disposable.</param>
    public Pocket(IDisposable first, IDisposable second, IDisposable third)
        : base(first, second, third)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="Pocket"/> class.</summary>
    /// <param name="disposables">Initial disposables.</param>
    public Pocket(params IDisposable[] disposables)
        : base(disposables)
    {
    }

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;
}
