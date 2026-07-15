// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Android.OS;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Android sequencer that coalesces scheduled work onto the thread backing a <see cref="Handler"/> (typically the
/// main/UI looper). Immediate work is batched through a single cached <see cref="Java.Lang.IRunnable"/> drain, so the
/// per-post path allocates nothing; delayed work uses the native <see cref="Handler.PostDelayed(Java.Lang.IRunnable, long)"/>.
/// </summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class HandlerSequencer : ISequencer
{
    /// <summary>Coalescing dispatch engine.</summary>
    private DispatchSequencerState _state;

    /// <summary>
    /// Cached runnable wrapping the drain. The drain callback is invariant for the lifetime of the sequencer,
    /// so the JNI runnable bridge is built once and reused for every posted batch rather than per post.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Maintainability",
        "SST1422:Move this field into the method that uses it",
        Justification =
            "Persistent lazy cache: the JNI runnable bridge is built once and reused across every Post call, so it cannot be a method local.")]
    private Java.Lang.IRunnable? _drainRunnable;

    /// <summary>Initializes a new instance of the <see cref="HandlerSequencer"/> class.</summary>
    /// <param name="handler">The handler used to marshal work onto its looper thread.</param>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="handler"/> is <see langword="null"/>.</exception>
    public HandlerSequencer(Handler handler)
    {
        Handler = handler ?? throw new ArgumentNullException(nameof(handler));
        _state = new(this, Post, RunDrain, ScheduleDelayed);
    }

    /// <summary>Gets a sequencer that marshals work onto the application's main (UI) looper.</summary>
    public static HandlerSequencer Main { get; } = new(new(Looper.MainLooper!));

    /// <summary>Gets the handler used to marshal work onto its looper thread.</summary>
    public Handler Handler { get; }

    /// <inheritdoc/>
    public DateTimeOffset Now => DispatchSequencerState.Now;

    /// <inheritdoc/>
    public long Timestamp => DispatchSequencerState.Timestamp;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    public override string ToString() => $"HandlerSequencer({Handler})";

    /// <inheritdoc/>
    public void Schedule(IWorkItem item) => _state.Schedule(item);

    /// <inheritdoc/>
    public void Schedule(IWorkItem item, long dueTimestamp) => _state.Schedule(item, dueTimestamp);

    /// <summary>Marshals the cached drain callback onto the handler's looper thread.</summary>
    /// <param name="drain">The drain callback.</param>
    /// <returns><see langword="true"/> when the handler accepted the work.</returns>
    private bool Post(Action drain)
    {
        _drainRunnable ??= new Java.Lang.Runnable(drain);
        return Handler.Post(_drainRunnable);
    }

    /// <summary>Runs delayed work through the handler's native delayed post.</summary>
    /// <param name="item">Work item to execute at the due time.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    private void ScheduleDelayed(IWorkItem item, long dueTimestamp) =>
        Handler.PostDelayed(
            () => DispatchSequencerState.RunIfActive(item),
            (long)DispatchSequencerState.DelayUntil(dueTimestamp).TotalMilliseconds);

    /// <summary>Forwards the cached drain callback to the engine.</summary>
    private void RunDrain() => _state.RunDrain();
}
