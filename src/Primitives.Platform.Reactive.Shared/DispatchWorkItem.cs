// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>A scheduled work item carrying closure-free state and the scheduler passed back to the action.</summary>
/// <typeparam name="TState">The scheduled state type.</typeparam>
/// <param name="scheduler">The scheduler passed back to the scheduled action.</param>
/// <param name="state">Scheduled state.</param>
/// <param name="action">Scheduled action.</param>
internal sealed class DispatchWorkItem<TState>(IScheduler scheduler, TState state, Func<IScheduler, TState, IDisposable> action) : IDispatchWorkItem
{
    /// <summary>The run and cancel state.</summary>
    private DispatchWorkState<TState> _work = new(scheduler, state, action);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Run() => _work.Run();

    /// <inheritdoc/>
    public void Dispose()
    {
        if (!_work.TryClaimDispose())
        {
            return;
        }

        _work.ReleaseStartedWork();
    }
}
