// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Disposables;

/// <summary>Holds a replaceable disposable and disposes each displaced value.</summary>
/// <remarks>Assignments after disposal are disposed immediately.</remarks>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class SwapDisposable : IsDisposed
{
    /// <summary>The current inner disposable.</summary>
    private IDisposable? _current;

    /// <summary>Indicates whether the object has been disposed (0 = open, 1 = disposed).</summary>
    private int _disposed;

    /// <summary>Gets a value indicating whether this instance has been disposed.</summary>
    public bool IsDisposed => Volatile.Read(ref _disposed) == DisposableSlotHelper.DisposedSentinel;

    /// <summary>Gets or sets the current inner disposable, disposing the previous value on assignment.</summary>
    public IDisposable? Disposable
    {
        get => Volatile.Read(ref _current);
        set => DisposableSlotHelper.SwapAndDisposePrevious(ref _current, ref _disposed, value);
    }

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => DisposableSlotHelper.TryDispose(ref _current, ref _disposed);
}
