// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;

using CoreFoundation;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>
/// System.Reactive-flavoured Apple scheduler that coalesces scheduled work onto the main <see cref="DispatchQueue"/>
/// (the UI thread on iOS, tvOS, Mac Catalyst, and macOS). Immediate work is batched through a single cached
/// <see cref="DispatchBlock"/> drain, so the per-post path allocates nothing; delayed work uses
/// <see cref="DispatchQueue.DispatchAfter(DispatchTime, DispatchBlock)"/>.
/// </summary>
/// <seealso cref="System.Reactive.Concurrency.IScheduler" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class NSRunloopSequencer : CoalescingDispatchScheduler
{
    /// <summary>Nanoseconds per millisecond, used to convert a managed delay into a <see cref="DispatchTime"/> offset.</summary>
    private const long NanosecondsPerMillisecond = 1_000_000;

    /// <summary>
    /// Cached dispatch block wrapping the drain. The drain callback is invariant for the lifetime of the
    /// sequencer, so the block is created once and re-enqueued for every posted batch rather than per post.
    /// </summary>
    private DispatchBlock? _drainBlock;

    /// <summary>Initializes a new instance of the <see cref="NSRunloopSequencer"/> class.</summary>
    private NSRunloopSequencer()
    {
    }

    /// <summary>Gets a sequencer that marshals work onto the main dispatch queue.</summary>
    public static NSRunloopSequencer Main { get; } = new();

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    public override string ToString() => "NSRunloopSequencer(main queue)";

    /// <inheritdoc/>
    protected override bool Post(Action drain)
    {
        _drainBlock ??= new DispatchBlock(drain);
        DispatchQueue.MainQueue.DispatchAsync(_drainBlock);
        return true;
    }

    /// <inheritdoc/>
    protected override IDisposable ScheduleOnDispatcher(Action work, TimeSpan dueTime)
    {
        var block = new DispatchBlock(work);
        var nanoseconds = (long)dueTime.TotalMilliseconds * NanosecondsPerMillisecond;
        DispatchQueue.MainQueue.DispatchAfter(new DispatchTime(DispatchTime.Now, nanoseconds), block);
        return Disposable.Create(block, static b => b.Cancel());
    }
}
