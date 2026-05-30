// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Windows Forms sequencer that coalesces scheduled work through a UI control.
/// </summary>
/// <seealso cref="DispatchSequencerBase" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class ControlSequencer : DispatchSequencerBase
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ControlSequencer"/> class.
    /// </summary>
    /// <param name="control">The control used to marshal work to the UI thread.</param>
    /// <exception cref="ArgumentNullException"><paramref name="control"/> is <see langword="null"/>.</exception>
    public ControlSequencer(Control control)
    {
        Control = control ?? throw new ArgumentNullException(nameof(control));
        Control.HandleCreated += OnHandleCreated;
    }

    /// <summary>
    /// Gets the control used to marshal work to the UI thread.
    /// </summary>
    public Control Control { get; }

    /// <summary>
    /// Gets the debugger display text.
    /// </summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    protected override bool Post(Action drain)
    {
        if (Control.IsDisposed)
        {
            throw new ObjectDisposedException(Control.GetType().FullName);
        }

        if (!Control.IsHandleCreated)
        {
            return false;
        }

        try
        {
            Control.BeginInvoke(drain);
            return true;
        }
        catch (InvalidOperationException) when (!Control.IsDisposed)
        {
            return false;
        }
    }

    /// <summary>
    /// Posts queued work once the control handle becomes available.
    /// </summary>
    /// <param name="sender">The event source.</param>
    /// <param name="e">The event arguments.</param>
    private void OnHandleCreated(object? sender, EventArgs e) => PostDrain();
}
