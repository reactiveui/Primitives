// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>
/// Shared subscription-time scheduling for advanced signals. A cold signal that must observe the
/// current-thread sequencer cannot simply subscribe inline: when the current-thread sequencer is already
/// draining work, the subscription has to be queued behind it so the emissions arrive in trampoline order.
/// The two shapes below capture that dance once so each signal keeps only its own <c>Run</c> body.
/// </summary>
internal static class SubscriptionScheduling
{
    /// <summary>
    /// Subscribes immediately when the current-thread sequencer is idle, otherwise queues the subscription
    /// on it and hands back a placeholder that is filled in once the subscription actually runs.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the subscribe callback.</typeparam>
    /// <param name="state">The state carried to <paramref name="subscribe"/>.</param>
    /// <param name="subscribe">Performs the subscription and returns it.</param>
    /// <returns>The subscription, or a placeholder that receives it once the queued work runs.</returns>
    public static IDisposable OnCurrentThread<TState>(TState state, Func<TState, IDisposable> subscribe)
    {
        if (!CurrentThreadSequencer.IsScheduleRequired)
        {
            return subscribe(state);
        }

        SingleDisposable subscription = new();
        _ = Sequencer.CurrentThread.Schedule(
            (state, subscription, subscribe),
            static (_, s) =>
            {
                s.subscription.Create(s.subscribe(s.state));
                return EmptyDisposable.Instance;
            });
        return subscription;
    }

    /// <summary>
    /// Runs <paramref name="run"/> on <paramref name="sequencer"/>: inline for the immediate sequencer, and
    /// queued behind the current-thread sequencer when that one is the target and is already draining work.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled work.</typeparam>
    /// <param name="sequencer">The sequencer that runs the work.</param>
    /// <param name="state">The state carried to <paramref name="run"/>.</param>
    /// <param name="run">The work to run, which emits to the downstream observer.</param>
    /// <returns>The disposable used to cancel the scheduled work (best effort).</returns>
    public static IDisposable RunOn<TState>(ISequencer sequencer, TState state, Action<TState> run)
    {
        if (sequencer == Sequencer.Immediate)
        {
            run(state);
            return EmptyDisposable.Instance;
        }

        if (sequencer != Sequencer.CurrentThread || !CurrentThreadSequencer.IsScheduleRequired)
        {
            return sequencer.Schedule(
                (state, run),
                static (_, s) =>
                {
                    s.run(s.state);
                    return EmptyDisposable.Instance;
                });
        }

        SingleDisposable subscription = new();
        _ = Sequencer.CurrentThread.Schedule(
            (sequencer, state, subscription, run),
            static (_, s) =>
            {
                s.subscription.Create(
                    s.sequencer.Schedule(
                        (s.state, s.run),
                        static (_, inner) =>
                        {
                            inner.run(inner.state);
                            return EmptyDisposable.Instance;
                        }));
                return EmptyDisposable.Instance;
            });
        return subscription;
    }
}
