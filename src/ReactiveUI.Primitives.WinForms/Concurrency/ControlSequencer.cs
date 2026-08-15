// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Windows Forms sequencer that coalesces scheduled work through a UI control.</summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class ControlSequencer : ISequencer
{
    /// <summary>Coalescing dispatch engine.</summary>
    private DispatchSequencerState _state;

    /// <summary>Initializes a new instance of the <see cref="ControlSequencer"/> class.</summary>
    /// <param name="control">The control used to marshal work to the UI thread.</param>
    /// <exception cref="ArgumentNullException"><paramref name="control"/> is <see langword="null"/>.</exception>
    public ControlSequencer(Control control)
    {
        Control = control ?? throw new ArgumentNullException(nameof(control));
        _state = new(this, Post, RunDrain);
        Control.HandleCreated += OnHandleCreated;
    }

    /// <summary>Gets the control used to marshal work to the UI thread.</summary>
    public Control Control { get; }

    /// <inheritdoc/>
    public DateTimeOffset Now => DispatchSequencerState.Now;

    /// <inheritdoc/>
    public long Timestamp => DispatchSequencerState.Timestamp;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Schedule(IWorkItem item) => _state.Schedule(item);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Schedule(IWorkItem item, long dueTimestamp) => _state.Schedule(item, dueTimestamp);

    /// <summary>Marshals the cached drain callback onto the control's UI thread.</summary>
    /// <param name="drain">The drain callback.</param>
    /// <returns><see langword="true"/> when the drain was posted; otherwise, <see langword="false"/>.</returns>
    /// <exception cref="ObjectDisposedException">The control has been disposed.</exception>
    private bool Post(Action drain)
    {
        ObjectDisposedExceptionHelper.ThrowIf(Control.IsDisposed, Control);

        if (!Control.IsHandleCreated)
        {
            return false;
        }

        try
        {
            _ = Control.BeginInvoke(drain);
            return true;
        }
        catch (InvalidOperationException) when (!Control.IsDisposed)
        {
            return false;
        }
    }

    /// <summary>Forwards the cached drain callback to the engine.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RunDrain() => _state.RunDrain();

    /// <summary>Posts queued work once the control handle becomes available.</summary>
    /// <param name="sender">The event source.</param>
    /// <param name="e">The event arguments.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnHandleCreated(object? sender, EventArgs e) => _state.PostDrain();
}
