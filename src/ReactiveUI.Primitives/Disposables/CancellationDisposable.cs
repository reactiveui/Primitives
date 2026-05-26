// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>
/// CancellationDisposable.
/// </summary>
/// <seealso cref="IDisposable" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed partial class CancellationDisposable : IsDisposed
{
    /// <summary>
    /// Cancellation source owned by this disposable.
    /// </summary>
    private readonly CancellationTokenSource _cts;

    /// <summary>
    /// Initializes a new instance of the <see cref="CancellationDisposable"/> class.
    /// </summary>
    /// <param name="cts">The CTS.</param>
    /// <exception cref="ArgumentNullException">cts.</exception>
    public CancellationDisposable(CancellationTokenSource cts) => _cts = cts ?? throw new ArgumentNullException(nameof(cts));

    /// <summary>
    /// Initializes a new instance of the <see cref="CancellationDisposable"/> class.
    /// </summary>
    public CancellationDisposable()
      : this(new CancellationTokenSource())
    {
    }

    /// <summary>
    /// Gets the token.
    /// </summary>
    /// <value>
    /// The token.
    /// </value>
    public CancellationToken Token => _cts.Token;

    /// <summary>
    /// Gets a value indicating whether this instance is disposed.
    /// </summary>
    /// <value>
    /// <c>true</c> if this instance is disposed; otherwise, <c>false</c>.
    /// </value>
    public bool IsDisposed { get; private set; }

    /// <summary>
    /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
    /// </summary>
    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
    }

    /// <summary>
    /// Releases resources used by this disposable.
    /// </summary>
    /// <param name="disposing"><see langword="true"/> when called from <see cref="Dispose()"/>.</param>
    private void Dispose(bool disposing)
    {
        if (IsDisposed)
        {
            return;
        }

        if (disposing)
        {
            _cts.Cancel();
        }

        IsDisposed = true;
    }
}
