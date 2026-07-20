// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using CoreFoundation;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Apple sequencer that coalesces scheduled work onto the main <see cref="DispatchQueue"/> (the UI thread on
/// iOS, tvOS, Mac Catalyst, and macOS). Immediate work is batched through a single cached <see cref="DispatchBlock"/>
/// drain, so the per-post path allocates nothing; delayed work uses <see cref="DispatchQueue.DispatchAfter(DispatchTime, Action)"/>.
/// </summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class NSRunloopSequencer : ISequencer
{
    /// <summary>Nanoseconds per millisecond, used to convert a managed delay into a <see cref="DispatchTime"/> offset.</summary>
    private const long NanosecondsPerMillisecond = 1_000_000;

    /// <summary>Coalescing dispatch engine.</summary>
    private DispatchSequencerState _state;

    /// <summary>
    /// Cached dispatch block wrapping the drain. The drain callback is invariant for the lifetime of the
    /// sequencer, so the block is created once and re-enqueued for every posted batch rather than per post.
    /// </summary>
    private DispatchBlock? _drainBlock;

    /// <summary>Initializes a new instance of the <see cref="NSRunloopSequencer"/> class.</summary>
    private NSRunloopSequencer() => _state = new(this, Post, RunDrain, ScheduleDelayed);

    /// <summary>Gets a sequencer that marshals work onto the main dispatch queue.</summary>
    public static NSRunloopSequencer Main { get; } = new();

    /// <inheritdoc/>
    public DateTimeOffset Now => DispatchSequencerState.Now;

    /// <inheritdoc/>
    public long Timestamp => DispatchSequencerState.Timestamp;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    public override string ToString() => "NSRunloopSequencer(main queue)";

    /// <inheritdoc/>
    public void Schedule(IWorkItem item) => _state.Schedule(item);

    /// <inheritdoc/>
    public void Schedule(IWorkItem item, long dueTimestamp) => _state.Schedule(item, dueTimestamp);

    /// <summary>Marshals the cached drain callback onto the main dispatch queue.</summary>
    /// <param name="drain">The drain callback.</param>
    /// <returns><see langword="true"/>, since the main queue always accepts the work.</returns>
    private bool Post(Action drain)
    {
        _drainBlock ??= new DispatchBlock(drain);
        DispatchQueue.MainQueue.DispatchAsync(_drainBlock);
        return true;
    }

    /// <summary>Runs delayed work through the main queue's native delayed dispatch.</summary>
    /// <param name="item">Work item to execute at the due time.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    private static void ScheduleDelayed(IWorkItem item, long dueTimestamp)
    {
        var nanoseconds = (long)DispatchSequencerState.DelayUntil(dueTimestamp).TotalMilliseconds * NanosecondsPerMillisecond;
        DispatchQueue.MainQueue.DispatchAfter(new DispatchTime(DispatchTime.Now, nanoseconds), () => DispatchSequencerState.RunIfActive(item));
    }

    /// <summary>Forwards the cached drain callback to the engine.</summary>
    private void RunDrain() => _state.RunDrain();
}
