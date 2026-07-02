// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>A scheduled work item carrying closure-free state and the scheduler passed back to the action.</summary>
/// <typeparam name="TState">The scheduled state type.</typeparam>
internal sealed class DispatchWorkItem<TState> : DispatchWorkItemBase<TState>, IDispatchWorkItem
{
    /// <summary>Initializes a new instance of the <see cref="DispatchWorkItem{TState}"/> class.</summary>
    /// <param name="scheduler">The scheduler passed back to the scheduled action.</param>
    /// <param name="state">Scheduled state.</param>
    /// <param name="action">Scheduled action.</param>
    public DispatchWorkItem(IScheduler scheduler, TState state, Func<IScheduler, TState, IDisposable> action)
        : base(scheduler, state, action)
    {
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (!TryClaimDispose())
        {
            return;
        }

        ReleaseStartedWork();
    }
}
