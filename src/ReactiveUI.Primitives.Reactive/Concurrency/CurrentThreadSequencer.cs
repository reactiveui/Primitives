// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>Maps the current-thread sequencer onto System.Reactive's <see cref="CurrentThreadScheduler"/>.</summary>
internal static class CurrentThreadSequencer
{
    /// <summary>Gets the singleton current-thread scheduler.</summary>
    internal static IScheduler Instance => CurrentThreadScheduler.Instance;

    /// <summary>Gets a value indicating whether the caller must schedule work instead of running it inline.</summary>
    internal static bool IsScheduleRequired => CurrentThreadScheduler.IsScheduleRequired;

    /// <summary>Runs <paramref name="action"/> on the current-thread trampoline, scheduling only when required.</summary>
    /// <typeparam name="TState">The state type passed to <paramref name="action"/>.</typeparam>
    /// <param name="state">The state forwarded to <paramref name="action"/>.</param>
    /// <param name="action">The callback that performs the work and returns its subscription.</param>
    /// <returns>The subscription returned by <paramref name="action"/>, or a wrapper populated once scheduled work runs.</returns>
    internal static IDisposable RunTrampoline<TState>(TState state, Func<TState, IDisposable> action)
    {
        if (!CurrentThreadScheduler.IsScheduleRequired)
        {
            return action(state);
        }

        SingleDisposable subscription = new();
        _ = CurrentThreadScheduler.Instance.Schedule(
            (state, subscription, action),
            static (_, s) =>
            {
                s.subscription.Create(s.action(s.state));
                return EmptyDisposable.Instance;
            });
        return subscription;
    }
}
