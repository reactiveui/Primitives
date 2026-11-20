// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using RxScheduler = System.Reactive.Concurrency.Scheduler;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>Maps built-in sequencers and scheduling operations onto System.Reactive schedulers.</summary>
public static class Sequencer
{
    /// <summary>Gets a scheduler that schedules work as soon as possible on the current thread.</summary>
    public static IScheduler CurrentThread => CurrentThreadScheduler.Instance;

    /// <summary>Gets a scheduler that schedules work immediately on the current thread.</summary>
    public static IScheduler Immediate => ImmediateScheduler.Instance;

    /// <summary>Gets the default queueing scheduler for background work.</summary>
    public static IScheduler Default => TaskPoolScheduler.Default;

    /// <summary>Normalizes the specified <see cref="TimeSpan"/> value to a positive value.</summary>
    /// <param name="timeSpan">The value to normalize.</param>
    /// <returns><paramref name="timeSpan"/> when zero or positive; otherwise <see cref="TimeSpan.Zero"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static TimeSpan Normalize(TimeSpan timeSpan) => RxScheduler.Normalize(timeSpan);

    /// <summary>Determines whether <paramref name="scheduler"/> executes work immediately on the calling thread.</summary>
    /// <param name="scheduler">The scheduler to test; may be <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when <paramref name="scheduler"/> is an <see cref="ImmediateScheduler"/>; otherwise, <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsImmediate(IScheduler? scheduler) => scheduler is ImmediateScheduler;

    /// <summary>Schedules a work item for execution on <paramref name="scheduler"/>.</summary>
    /// <param name="scheduler">Scheduler to execute work on.</param>
    /// <param name="item">Work item to execute.</param>
    public static void Schedule(IScheduler scheduler, IWorkItem item)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(item);

        _ = scheduler.Schedule(item, static (_, w) =>
        {
            w.Execute();
            return EmptyDisposable.Instance;
        });
    }

    /// <summary>Schedules an action on <paramref name="scheduler"/>.</summary>
    /// <param name="scheduler">Scheduler to execute work on.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable used to cancel the scheduled action.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IDisposable Schedule(IScheduler scheduler, Action action) => RxScheduler.Schedule(scheduler, action);

    /// <summary>Schedules an action on <paramref name="scheduler"/> after a relative due time.</summary>
    /// <param name="scheduler">Scheduler to execute work on.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable used to cancel the scheduled action.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IDisposable Schedule(IScheduler scheduler, TimeSpan dueTime, Action action) =>
        RxScheduler.Schedule(scheduler, dueTime, action);

    /// <summary>Schedules an action on <paramref name="scheduler"/> at an absolute due time.</summary>
    /// <param name="scheduler">Scheduler to execute work on.</param>
    /// <param name="dueTime">Absolute time at which to execute the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable used to cancel the scheduled action.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IDisposable Schedule(IScheduler scheduler, DateTimeOffset dueTime, Action action) =>
        RxScheduler.Schedule(scheduler, dueTime, action);

    /// <summary>Schedules a recursive action on <paramref name="scheduler"/>.</summary>
    /// <param name="scheduler">Scheduler to execute work on.</param>
    /// <param name="action">Recursive action; invoke its argument to schedule the next iteration.</param>
    /// <returns>The disposable used to cancel recursive work.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IDisposable Schedule(IScheduler scheduler, Action<Action> action) => RxScheduler.Schedule(scheduler, action);

    /// <summary>Schedules a stateful action on <paramref name="scheduler"/> without capturing state in a closure.</summary>
    /// <typeparam name="TState">The type of the state passed to the action.</typeparam>
    /// <param name="scheduler">Scheduler to execute work on.</param>
    /// <param name="state">State passed to the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable used to cancel the scheduled action.</returns>
    public static IDisposable Schedule<TState>(IScheduler scheduler, TState state, Action<TState> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(action);

        return scheduler.Schedule((state, action), static (_, tuple) =>
        {
            tuple.action(tuple.state);
            return EmptyDisposable.Instance;
        });
    }

    /// <summary>Schedules a stateful action on <paramref name="scheduler"/> after a relative due time.</summary>
    /// <typeparam name="TState">The type of the state passed to the action.</typeparam>
    /// <param name="scheduler">Scheduler to execute work on.</param>
    /// <param name="state">State passed to the action.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable used to cancel the scheduled action.</returns>
    public static IDisposable Schedule<TState>(IScheduler scheduler, TState state, TimeSpan dueTime, Action<TState> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(action);

        return scheduler.Schedule((state, action), dueTime, static (_, tuple) =>
        {
            tuple.action(tuple.state);
            return EmptyDisposable.Instance;
        });
    }
}
