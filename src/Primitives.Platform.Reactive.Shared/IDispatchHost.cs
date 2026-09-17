// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>The platform dispatcher operations a <see cref="CoalescingDispatchState"/> needs from the scheduler that embeds it.</summary>
/// <remarks>Implement it on a private struct wrapping the scheduler, so the calls need no delegate.</remarks>
public interface IDispatchHost
{
    /// <summary>Posts the drain callback to the platform dispatcher.</summary>
    /// <param name="drain">The drain callback to marshal to the UI thread.</param>
    /// <returns><see langword="true"/> when the dispatcher accepted the work.</returns>
    bool Post(Action drain);

    /// <summary>Schedules delayed work for dispatcher delivery.</summary>
    /// <param name="work">Callback to invoke on the dispatcher thread when due.</param>
    /// <param name="dueTime">Relative time after which to invoke <paramref name="work"/>.</param>
    /// <returns>The disposable used to cancel the delayed dispatch.</returns>
    IDisposable ScheduleOnDispatcher(Action work, TimeSpan dueTime);
}
