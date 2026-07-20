// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Reactive.Disposables;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>
/// Shared run/cancel core for scheduled, cancellable work items carrying closure-free state and the scheduler passed
/// back to the action. It owns the atomic start-versus-cancel handshake so every dispatcher and event-loop scheduler
/// implements it exactly once; derived types add only the cancellation resources specific to how the work was queued
/// (for example a one-shot timer).
/// </summary>
/// <typeparam name="TState">The scheduled state type.</typeparam>
internal class DispatchWorkItemBase<TState>
{
    /// <summary>The scheduler passed back to the scheduled action.</summary>
    private readonly IScheduler _scheduler;

    /// <summary>Scheduled state.</summary>
    private readonly TState _state;

    /// <summary>Scheduled action.</summary>
    private readonly Func<IScheduler, TState, IDisposable> _action;

    /// <summary>Disposable returned by the scheduled action after it starts.</summary>
    private IDisposable? _disposable;

    /// <summary>Tracks cancellation.</summary>
    private int _isDisposed;

    /// <summary>Initializes a new instance of the <see cref="DispatchWorkItemBase{TState}"/> class.</summary>
    /// <param name="scheduler">The scheduler passed back to the scheduled action.</param>
    /// <param name="state">Scheduled state.</param>
    /// <param name="action">Scheduled action.</param>
    /// <remarks>
    /// Written out rather than made a primary constructor so it can stay <c>protected</c>: a primary
    /// constructor on a concrete class is public, which would let anything construct the base directly
    /// instead of going through a derived work item.
    /// </remarks>
    protected DispatchWorkItemBase(
        IScheduler scheduler,
        TState state,
        Func<IScheduler, TState, IDisposable> action)
    {
        _scheduler = scheduler;
        _state = state;
        _action = action;
    }

    /// <summary>Gets a value indicating whether the work item has been cancelled.</summary>
    internal bool IsDisposed => Volatile.Read(ref _isDisposed) != 0;

    /// <summary>Runs the scheduled action unless it has already been cancelled, disposing its result if a cancel races the start.</summary>
    internal void Run()
    {
        if (IsDisposed)
        {
            return;
        }

        var disposable = _action(_scheduler, _state) ?? Disposable.Empty;
        var previous = Interlocked.CompareExchange(ref _disposable, disposable, null);
        if (previous is not null)
        {
            disposable.Dispose();
            return;
        }

        if (!IsDisposed)
        {
            return;
        }

        disposable.Dispose();
    }

    /// <summary>Atomically claims the single cancellation transition for this work item.</summary>
    /// <returns><see langword="true"/> for the first caller, which owns releasing the item's resources.</returns>
    protected bool TryClaimDispose() => Interlocked.Exchange(ref _isDisposed, 1) == 0;

    /// <summary>Releases the disposable the action returned once it has started, so a late cancel still tears it down.</summary>
    protected void ReleaseStartedWork() => Interlocked.Exchange(ref _disposable, Disposable.Empty)?.Dispose();
}
