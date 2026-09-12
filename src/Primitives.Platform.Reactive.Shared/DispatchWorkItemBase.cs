// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>Coordinates work execution and cancellation; derived items own scheduling resources.</summary>
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

    /// <summary>Runs the scheduled action unless it has been cancelled, disposing its result when a cancel races the start.</summary>
    public void Run()
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

        ReleaseCanceledResult();
    }

    /// <summary>Releases the published result if the work item is cancelled.</summary>
    internal void ReleaseCanceledResult()
    {
        if (!IsDisposed)
        {
            return;
        }

        ReleaseStartedWork();
    }

    /// <summary>Atomically claims the single cancellation transition for this work item.</summary>
    /// <returns><see langword="true"/> for the first caller, which owns releasing the item's resources.</returns>
    protected bool TryClaimDispose() => Interlocked.Exchange(ref _isDisposed, 1) == 0;

    /// <summary>Disposes whatever the started action returned, so a cancel arriving after the start tears it down.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected void ReleaseStartedWork() => Interlocked.Exchange(ref _disposable, Disposable.Empty)?.Dispose();
}
