// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>CancellationDisposable.</summary>
/// <seealso cref="IDisposable" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class CancellationDisposable : IsDisposed
{
    /// <summary>Cancellation source owned by this disposable.</summary>
    private readonly CancellationTokenSource _cts;

    /// <summary>Disposed latch; 0 when alive, 1 once disposed.</summary>
    private int _isDisposed;

    /// <summary>Initializes a new instance of the <see cref="CancellationDisposable"/> class.</summary>
    /// <param name="cts">The CTS.</param>
    /// <exception cref="ArgumentNullException">cts.</exception>
    public CancellationDisposable(CancellationTokenSource cts) => _cts = cts ?? throw new ArgumentNullException(nameof(cts));

    /// <summary>Initializes a new instance of the <see cref="CancellationDisposable"/> class.</summary>
    public CancellationDisposable()
      : this(new())
    {
    }

    /// <summary>Gets the token.</summary>
    /// <value>
    /// The token.
    /// </value>
    public CancellationToken Token => _cts.Token;

    /// <summary>Gets a value indicating whether this instance is disposed.</summary>
    /// <value>
    /// <c>true</c> if this instance is disposed; otherwise, <c>false</c>.
    /// </value>
    public bool IsDisposed => Volatile.Read(ref _isDisposed) != 0;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.</summary>
    public void Dispose()
    {
        // Atomic run-once latch so concurrent disposal cannot cancel the source twice.
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
        {
            return;
        }

        _cts.Cancel();
    }
}
