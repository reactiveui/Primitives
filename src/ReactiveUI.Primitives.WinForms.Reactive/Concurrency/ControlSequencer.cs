// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>Windows Forms scheduler that coalesces scheduled work through a UI control.</summary>
/// <remarks>Callbacks run on the control thread; work queued before handle creation waits for the handle.</remarks>
/// <seealso cref="System.Reactive.Concurrency.IScheduler" />
[System.Diagnostics.DebuggerDisplay("ControlSequencer: Control = {Control}")]
public sealed class ControlSequencer : CoalescingDispatchScheduler
{
    /// <summary>Optional callback for posting ready work.</summary>
    private readonly Func<Action, bool>? _post;

    /// <summary>Initializes a new instance of the <see cref="ControlSequencer"/> class.</summary>
    /// <param name="control">The control used to marshal work to the UI thread.</param>
    /// <exception cref="ArgumentNullException"><paramref name="control"/> is <see langword="null"/>.</exception>
    public ControlSequencer(Control control)
        : this(control, null, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ControlSequencer"/> class.</summary>
    /// <param name="control">The control associated with this scheduler.</param>
    /// <param name="post">Posts ready work, or null to use the control.</param>
    /// <param name="delayScheduler">Schedules delayed work, or null to use the default scheduler.</param>
    /// <exception cref="ArgumentNullException">The control is null.</exception>
    internal ControlSequencer(Control control, Func<Action, bool>? post, IScheduler? delayScheduler)
        : base(delayScheduler ?? DefaultScheduler.Instance)
    {
        Control = control ?? throw new ArgumentNullException(nameof(control));
        _post = post;
        Control.HandleCreated += OnHandleCreated;
    }

    /// <summary>Gets the control used to marshal work to the UI thread.</summary>
    public Control Control { get; }

    /// <summary>Posts work when the control is live and its handle is available.</summary>
    /// <param name="control">The control whose disposal state is checked.</param>
    /// <param name="handleCreated">Whether the control handle is available.</param>
    /// <param name="drain">The callback to post.</param>
    /// <param name="beginInvoke">Posts the callback to the control.</param>
    /// <returns>False if the handle is unavailable or disappears while posting.</returns>
    internal static bool TryPost(Control control, bool handleCreated, Action drain, Action<Control, Action> beginInvoke)
    {
        ObjectDisposedExceptionHelper.ThrowIf(control.IsDisposed, control);

        if (!handleCreated)
        {
            return false;
        }

        try
        {
            beginInvoke(control, drain);
            return true;
        }
        catch (InvalidOperationException) when (!control.IsDisposed)
        {
            return false;
        }
    }

    /// <summary>Posts queued work once the control handle becomes available.</summary>
    /// <param name="sender">The event source.</param>
    /// <param name="e">The event arguments.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void OnHandleCreated(object? sender, EventArgs e) => RequestDrain();

    /// <inheritdoc/>
    /// <exception cref="ObjectDisposedException">The control has been disposed.</exception>
    protected override bool Post(Action drain) =>
        _post is null ? TryPost(Control, Control.IsHandleCreated, drain, BeginInvoke) : _post(drain);

    /// <summary>Invokes the platform's asynchronous control dispatch.</summary>
    /// <param name="control">The target control.</param>
    /// <param name="drain">The callback to post.</param>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private static void BeginInvoke(Control control, Action drain) => _ = control.BeginInvoke(drain);
}
