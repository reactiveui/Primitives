// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using CoreFoundation;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Schedules immediate and delayed work on the Apple main dispatch queue.</summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class NSRunloopSequencer : ISequencer
{
    /// <summary>Nanoseconds per millisecond, used to convert a managed delay into a <see cref="DispatchTime"/> offset.</summary>
    private const long NanosecondsPerMillisecond = 1_000_000;

    /// <summary>Coalescing dispatch engine.</summary>
    private DispatchSequencerState _state;

    /// <summary>Native drain callback reused across posted batches.</summary>
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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Schedule(IWorkItem item) => _state.Schedule(item);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Schedule(IWorkItem item, long dueTimestamp) => _state.Schedule(item, dueTimestamp);

    /// <summary>Runs delayed work through the main queue's native delayed dispatch.</summary>
    /// <param name="item">Work item to execute at the due time.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private static void ScheduleDelayed(IWorkItem item, long dueTimestamp)
    {
        var nanoseconds = (long)DispatchSequencerState.DelayUntil(dueTimestamp).TotalMilliseconds * NanosecondsPerMillisecond;
        DispatchQueue.MainQueue.DispatchAfter(new(DispatchTime.Now, nanoseconds), () => DispatchSequencerState.RunIfActive(item));
    }

    /// <summary>Posts the block through the native main queue.</summary>
    /// <param name="block">The callback block to post.</param>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void DispatchOnMainQueue(DispatchBlock block) => DispatchQueue.MainQueue.DispatchAsync(block);

    /// <summary>Marshals the cached drain callback onto the main dispatch queue.</summary>
    /// <param name="drain">The drain callback.</param>
    /// <returns><see langword="true"/>, since the main queue always accepts the work.</returns>
    private bool Post(Action drain)
    {
        _drainBlock ??= new DispatchBlock(drain);
        DispatchOnMainQueue(_drainBlock);
        return true;
    }

    /// <summary>Forwards the cached drain callback to the engine.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RunDrain() => _state.RunDrain();
}
