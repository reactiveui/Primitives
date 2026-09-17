// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Runtime.CompilerServices;

using CoreFoundation;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>Schedules immediate and delayed work on the Apple main dispatch queue.</summary>
/// <seealso cref="System.Reactive.Concurrency.IScheduler" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class NSRunloopSequencer : LocalScheduler
{
    /// <summary>Nanoseconds per millisecond, used to convert a managed delay into a <see cref="DispatchTime"/> offset.</summary>
    private const long NanosecondsPerMillisecond = 1_000_000;

    /// <summary>Queues work and coalesces main-queue drains.</summary>
    private CoalescingDispatchState _dispatch;

    /// <summary>Native drain callback reused across posted batches.</summary>
    private DispatchBlock? _drainBlock;

    /// <summary>Initializes a new instance of the <see cref="NSRunloopSequencer"/> class.</summary>
    private NSRunloopSequencer() => _dispatch = new(RunDrain, DefaultScheduler.Instance);

    /// <summary>Gets a sequencer that marshals work onto the main dispatch queue.</summary>
    public static NSRunloopSequencer Main { get; } = new();

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    public override string ToString() => "NSRunloopSequencer(main queue)";

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action) =>
        _dispatch.Schedule(new DispatchHost(this), this, state, action);

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<IScheduler, TState, IDisposable> action) =>
        _dispatch.Schedule(new DispatchHost(this), this, state, dueTime, action);

    /// <summary>Posts the block through the native main queue.</summary>
    /// <param name="block">The callback block to post.</param>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void DispatchOnMainQueue(DispatchBlock block) => DispatchQueue.MainQueue.DispatchAsync(block);

    /// <summary>Schedules a cancellable block through the native main queue.</summary>
    /// <param name="block">The callback block to run.</param>
    /// <param name="dueTime">The requested delay.</param>
    /// <returns>The block cancellation handle.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private static IDisposable ScheduleOnMainQueue(DispatchBlock block, TimeSpan dueTime)
    {
        var nanoseconds = (long)dueTime.TotalMilliseconds * NanosecondsPerMillisecond;
        DispatchQueue.MainQueue.DispatchAfter(new(DispatchTime.Now, nanoseconds), block);
        return Disposable.Create(block, static b => b.Cancel());
    }

    /// <summary>Posts the drain callback through the main queue, building its block once.</summary>
    /// <param name="drain">The drain callback.</param>
    /// <returns>Always <see langword="true"/>.</returns>
    private bool Post(Action drain)
    {
        _drainBlock ??= new DispatchBlock(drain);
        DispatchOnMainQueue(_drainBlock);
        return true;
    }

    /// <summary>Runs one main-queue batch.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RunDrain() => _dispatch.RunDrain(new DispatchHost(this));

    /// <summary>Reaches the main queue for this scheduler's dispatch state.</summary>
    /// <param name="Owner">The scheduler.</param>
    private readonly record struct DispatchHost(NSRunloopSequencer Owner) : IDispatchHost
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Post(Action drain) => Owner.Post(drain);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable ScheduleOnDispatcher(Action work, TimeSpan dueTime) =>
            ScheduleOnMainQueue(new(work), dueTime);
    }
}
