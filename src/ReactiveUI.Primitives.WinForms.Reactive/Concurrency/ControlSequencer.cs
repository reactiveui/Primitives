// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>Windows Forms scheduler that coalesces scheduled work through a UI control.</summary>
/// <remarks>Work runs on the control's UI thread. Work scheduled before the control has a handle stays queued until
/// handle creation posts a drain, and delayed work waits on a background scheduler that marshals it back through the
/// control when due.</remarks>
/// <seealso cref="System.Reactive.Concurrency.IScheduler" />
[System.Diagnostics.DebuggerDisplay("ControlSequencer: Control = {Control}")]
public sealed class ControlSequencer : CoalescingDispatchScheduler
{
    /// <summary>Initializes a new instance of the <see cref="ControlSequencer"/> class.</summary>
    /// <param name="control">The control used to marshal work to the UI thread.</param>
    /// <exception cref="ArgumentNullException"><paramref name="control"/> is <see langword="null"/>.</exception>
    public ControlSequencer(Control control)
    {
        Control = control ?? throw new ArgumentNullException(nameof(control));
        Control.HandleCreated += OnHandleCreated;
    }

    /// <summary>Gets the control used to marshal work to the UI thread.</summary>
    public Control Control { get; }

    /// <inheritdoc/>
    /// <exception cref="ObjectDisposedException">The control has been disposed.</exception>
    protected override bool Post(Action drain)
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

    /// <summary>Posts queued work once the control handle becomes available.</summary>
    /// <param name="sender">The event source.</param>
    /// <param name="e">The event arguments.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnHandleCreated(object? sender, EventArgs e) => RequestDrain();
}
