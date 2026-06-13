// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Android.OS;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Android sequencer that coalesces scheduled work onto the thread backing a <see cref="Handler"/> (typically the
/// main/UI looper). Immediate work is batched through a single cached <see cref="Java.Lang.IRunnable"/> drain, so the
/// per-post path allocates nothing; delayed work uses the native <see cref="Handler.PostDelayed(Java.Lang.IRunnable, long)"/>.
/// </summary>
/// <seealso cref="DispatchSequencerBase" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class HandlerSequencer : DispatchSequencerBase
{
    /// <summary>
    /// Cached runnable wrapping the base drain. The drain callback is invariant for the lifetime of the sequencer,
    /// so the JNI runnable bridge is built once and reused for every posted batch rather than per post.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Maintainability",
        "SST1422:Move this field into the method that uses it",
        Justification = "Persistent lazy cache: the JNI runnable bridge is built once and reused across every Post call, so it cannot be a method local.")]
    private Java.Lang.IRunnable? _drainRunnable;

    /// <summary>Initializes a new instance of the <see cref="HandlerSequencer"/> class.</summary>
    /// <param name="handler">The handler used to marshal work onto its looper thread.</param>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="handler"/> is <see langword="null"/>.</exception>
    public HandlerSequencer(Handler handler) =>
        Handler = handler ?? throw new ArgumentNullException(nameof(handler));

    /// <summary>Gets a sequencer that marshals work onto the application's main (UI) looper.</summary>
    public static HandlerSequencer Main { get; } = new(new(Looper.MainLooper!));

    /// <summary>Gets the handler used to marshal work onto its looper thread.</summary>
    public Handler Handler { get; }

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    public override string ToString() => $"HandlerSequencer({Handler})";

    /// <inheritdoc/>
    protected override bool Post(Action drain)
    {
        _drainRunnable ??= new Java.Lang.Runnable(drain);
        return Handler.Post(_drainRunnable);
    }

    /// <inheritdoc/>
    protected override void ScheduleDelayed(IWorkItem item, long dueTimestamp) =>
        Handler.PostDelayed(() => RunIfActive(item), (long)DelayUntil(dueTimestamp).TotalMilliseconds);
}
