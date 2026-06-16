// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Reactive.Disposables;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>A scheduled work item carrying closure-free state and the scheduler passed back to the action.</summary>
/// <typeparam name="TState">The scheduled state type.</typeparam>
internal sealed class DispatchWorkItem<TState> : IDispatchWorkItem
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

    /// <summary>Initializes a new instance of the <see cref="DispatchWorkItem{TState}"/> class.</summary>
    /// <param name="scheduler">The scheduler passed back to the scheduled action.</param>
    /// <param name="state">Scheduled state.</param>
    /// <param name="action">Scheduled action.</param>
    public DispatchWorkItem(IScheduler scheduler, TState state, Func<IScheduler, TState, IDisposable> action)
    {
        _scheduler = scheduler;
        _state = state;
        _action = action;
    }

    /// <summary>Gets a value indicating whether the work item has been cancelled.</summary>
    public bool IsDisposed => Volatile.Read(ref _isDisposed) != 0;

    /// <inheritdoc/>
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

        if (!IsDisposed)
        {
            return;
        }

        disposable.Dispose();
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
        {
            return;
        }

        Interlocked.Exchange(ref _disposable, Disposable.Empty)?.Dispose();
    }
}
