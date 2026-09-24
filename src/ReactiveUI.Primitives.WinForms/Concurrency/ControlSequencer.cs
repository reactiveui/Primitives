// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Windows Forms sequencer that coalesces scheduled work through a UI control.</summary>
/// <remarks>
/// Callbacks run in posted control-thread batches without inline reentrancy; work queued before handle creation waits for the handle.
/// </remarks>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class ControlSequencer : ISequencer
{
    /// <summary>The shared main-thread sequencer, set once a UI thread first reads it.</summary>
    private static ControlSequencer? _main;

    /// <summary>Optional callback for posting ready work.</summary>
    private readonly Func<Action, bool>? _post;

    /// <summary>Coalescing dispatch engine.</summary>
    private DispatchSequencerState _state;

    /// <summary>Initializes a new instance of the <see cref="ControlSequencer"/> class.</summary>
    /// <param name="control">The control used to marshal work to the UI thread.</param>
    /// <exception cref="ArgumentNullException"><paramref name="control"/> is <see langword="null"/>.</exception>
    public ControlSequencer(Control control)
        : this(control, null, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ControlSequencer"/> class.</summary>
    /// <param name="control">The control associated with this sequencer.</param>
    /// <param name="post">Posts ready work, or null to use the control.</param>
    /// <param name="scheduleDelayed">Schedules delayed work, or null to use the shared timer.</param>
    /// <exception cref="ArgumentNullException">The control is null.</exception>
    internal ControlSequencer(Control control, Func<Action, bool>? post, Action<IWorkItem, long>? scheduleDelayed)
    {
        Control = control ?? throw new ArgumentNullException(nameof(control));
        _post = post;
        _state = new(this, Post, RunDrain, scheduleDelayed);
        Control.HandleCreated += OnHandleCreated;
    }

    /// <summary>Gets the shared sequencer for the Windows Forms main (UI) thread.</summary>
    /// <exception cref="InvalidOperationException">Main is not bound yet and the calling thread is not an STA thread.</exception>
    /// <remarks>
    /// The first read binds Main to a hidden control whose handle belongs to the calling thread. Windows Forms UI
    /// threads are always STA and thread-pool threads never are, so a read from any other thread throws and nothing is
    /// cached. Read it first on the thread that runs the message loop, such as in <c>Main</c> before
    /// <see cref="Application.Run()"/>.
    /// </remarks>
    public static ControlSequencer Main =>
        Volatile.Read(ref _main) ?? BindMain(ref _main, Thread.CurrentThread.GetApartmentState());

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

    /// <summary>Caches the shared main-thread sequencer for a hidden control on the calling thread, keeping the first one bound.</summary>
    /// <param name="slot">The field that holds the shared sequencer.</param>
    /// <param name="apartment">The calling thread's apartment state.</param>
    /// <returns>The shared sequencer.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="apartment"/> is not <see cref="ApartmentState.STA"/>.</exception>
    internal static ControlSequencer BindMain(ref ControlSequencer? slot, ApartmentState apartment)
    {
        if (apartment != ApartmentState.STA)
        {
            throw new InvalidOperationException(
                "The calling thread is not a Windows Forms UI thread. Read ControlSequencer.Main first on the STA thread that runs the message loop.");
        }

        Control control = new();
        _ = control.Handle;
        ControlSequencer created = new(control);
        var bound = Interlocked.CompareExchange(ref slot, created, null);
        if (bound is null)
        {
            return created;
        }

        control.Dispose();
        return bound;
    }

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
    internal void OnHandleCreated(object? sender, EventArgs e) => _state.PostDrain();

    /// <summary>Invokes the platform's asynchronous control dispatch.</summary>
    /// <param name="control">The target control.</param>
    /// <param name="drain">The callback to post.</param>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private static void BeginInvoke(Control control, Action drain) => _ = control.BeginInvoke(drain);

    /// <summary>Marshals the cached drain callback onto the control's UI thread.</summary>
    /// <param name="drain">The drain callback.</param>
    /// <returns>True when the drain was posted.</returns>
    /// <exception cref="ObjectDisposedException">The control has been disposed.</exception>
    private bool Post(Action drain) =>
        _post is null ? TryPost(Control, Control.IsHandleCreated, drain, BeginInvoke) : _post(drain);

    /// <summary>Runs one queued batch on the coalescing engine.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RunDrain() => _state.RunDrain();
}
