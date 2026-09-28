// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Android.OS;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Schedules immediate and delayed work on the Android handler thread.</summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class HandlerSequencer : ISequencer, IThreadAffineSequencer
{
    /// <summary>The sequencer created for each looper other than <see cref="Main"/>'s, kept while its looper lives.</summary>
    private static readonly ConditionalWeakTable<Looper, HandlerSequencer> ByLooper = new();

    /// <summary>Coalescing dispatch engine.</summary>
    private DispatchSequencerState _state;

    /// <summary>JNI drain callback reused across posted batches.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Maintainability",
        "SST1422:Move this field into the method that uses it",
        Justification = "The JNI runnable bridge is built once and reused across every posted batch.")]
    private Java.Lang.IRunnable? _drainRunnable;

    /// <summary>Initializes a new instance of the <see cref="HandlerSequencer"/> class.</summary>
    /// <param name="handler">The handler used to marshal work onto its looper thread.</param>
    /// <exception cref="ArgumentNullException"><paramref name="handler"/> is <see langword="null"/>.</exception>
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

    /// <summary>Returns the sequencer for <paramref name="looper"/>, created once per looper.</summary>
    /// <param name="looper">The looper whose thread runs the scheduled work.</param>
    /// <returns><see cref="Main"/> for the main looper; otherwise one sequencer per looper, kept while the looper lives.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="looper"/> is <see langword="null"/>.</exception>
    public static HandlerSequencer For(Looper looper)
    {
        ArgumentExceptionHelper.ThrowIfNull(looper);

        return looper.Equals(Main.Handler.Looper)
            ? Main
            : ByLooper.GetValue(looper, static owner => new(new(owner)));
    }

    /// <summary>Returns whether the calling thread runs <see cref="Handler"/>'s looper.</summary>
    /// <returns><see langword="true"/> when the calling thread may run work for this sequencer inline.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool CheckAccess() => Handler.Looper.Equals(Looper.MyLooper());

    /// <inheritdoc/>
    public override string ToString() => $"HandlerSequencer({Handler})";

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Schedule(IWorkItem item) => _state.Schedule(item);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Schedule(IWorkItem item, long dueTimestamp) => _state.Schedule(item, dueTimestamp);

    /// <summary>Marshals the cached drain callback onto the handler's looper thread.</summary>
    /// <param name="drain">The drain callback.</param>
    /// <returns><see langword="true"/> when the handler accepted the work.</returns>
    private bool Post(Action drain)
    {
        _drainRunnable ??= new Java.Lang.Runnable(drain);
        return PostToHandler(_drainRunnable);
    }

    /// <summary>Posts the runnable through the native handler.</summary>
    /// <param name="runnable">The runnable to post.</param>
    /// <returns>Whether the handler accepted the runnable.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool PostToHandler(Java.Lang.IRunnable runnable) => Handler.Post(runnable);

    /// <summary>Runs delayed work through the handler's native delayed post.</summary>
    /// <param name="item">Work item to execute at the due time.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ScheduleDelayed(IWorkItem item, long dueTimestamp) =>
        Handler.PostDelayed(
            () => DispatchSequencerState.RunIfActive(item),
            (long)DispatchSequencerState.DelayUntil(dueTimestamp).TotalMilliseconds);

    /// <summary>Forwards the cached drain callback to the engine.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RunDrain() => _state.RunDrain();
}
