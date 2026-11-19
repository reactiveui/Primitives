// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Static scheduling helpers for <see cref="ISequencer"/>.</summary>
public static partial class Sequencer
{
    /// <summary>Schedules an action to be executed on <paramref name="scheduler"/>.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
    public static IDisposable Schedule<TState>(ISequencer scheduler, TState state, Func<ISequencer, TState, IDisposable> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        ArgumentExceptionHelper.ThrowIfNull(action);

        if (scheduler is ImmediateSequencer)
        {
            return action(scheduler, state) ?? EmptyDisposable.Instance;
        }

        DelegateWorkItem<TState> item = new(scheduler, state, action);
        scheduler.Schedule(item);
        return item;
    }

    /// <summary>Schedules a stateful action on <paramref name="scheduler"/> without capturing state in a closure.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
    public static IDisposable Schedule<TState>(ISequencer scheduler, TState state, Action<TState> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        ArgumentExceptionHelper.ThrowIfNull(action);

        if (scheduler is ImmediateSequencer)
        {
            action(state);
            return EmptyDisposable.Instance;
        }

        ActionWorkItem<TState> item = new(state, action);
        scheduler.Schedule(item);
        return item;
    }

    /// <summary>Schedules an action on <paramref name="scheduler"/> after a relative due time.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
    public static IDisposable Schedule<TState>(
        ISequencer scheduler,
        TState state,
        TimeSpan dueTime,
        Func<ISequencer, TState, IDisposable> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        ArgumentExceptionHelper.ThrowIfNull(action);

        var normalized = Normalize(dueTime);
        if (normalized == TimeSpan.Zero)
        {
            return Schedule(scheduler, state, action);
        }

        DelegateWorkItem<TState> item = new(scheduler, state, action);
        scheduler.Schedule(item, AddTimestamp(scheduler.Timestamp, normalized));
        return item;
    }

    /// <summary>Schedules a stateful action on <paramref name="scheduler"/> after a relative due time.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
    public static IDisposable Schedule<TState>(ISequencer scheduler, TState state, TimeSpan dueTime, Action<TState> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        ArgumentExceptionHelper.ThrowIfNull(action);

        var normalized = Normalize(dueTime);
        return normalized == TimeSpan.Zero
            ? Schedule(scheduler, state, action)
            : Schedule(
                scheduler,
                state,
                AddTimestamp(scheduler.Timestamp, normalized),
                action);
    }

    /// <summary>Schedules a stateful action on <paramref name="scheduler"/> at a monotonic timestamp.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
    public static IDisposable Schedule<TState>(ISequencer scheduler, TState state, long dueTimestamp, Action<TState> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        ArgumentExceptionHelper.ThrowIfNull(action);

        ActionWorkItem<TState> item = new(state, action);
        scheduler.Schedule(item, dueTimestamp);
        return item;
    }

    /// <summary>Schedules an action on <paramref name="scheduler"/> at an absolute wall-clock due time.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Absolute time at which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
    public static IDisposable Schedule<TState>(
        ISequencer scheduler,
        TState state,
        DateTimeOffset dueTime,
        Func<ISequencer, TState, IDisposable> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        ArgumentExceptionHelper.ThrowIfNull(action);

        return Schedule(scheduler, state, Normalize(dueTime - scheduler.Now), action);
    }

    /// <summary>Schedules an action on <paramref name="scheduler"/>.</summary>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
    public static IDisposable Schedule(ISequencer scheduler, Action action)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        ArgumentExceptionHelper.ThrowIfNull(action);

        if (scheduler is ImmediateSequencer)
        {
            action();
            return EmptyDisposable.Instance;
        }

        return scheduler is CurrentThreadSequencer currentThread
            ? currentThread.Schedule(action)
            : Schedule(scheduler, action, static (_, a) => Invoke(a));
    }

    /// <summary>Schedules an action on <paramref name="scheduler"/> after the specified relative due time.</summary>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
    public static IDisposable Schedule(ISequencer scheduler, TimeSpan dueTime, Action action)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        ArgumentExceptionHelper.ThrowIfNull(action);

        return Schedule(scheduler, action, dueTime, static (_, a) => Invoke(a));
    }

    /// <summary>Schedules an action on <paramref name="scheduler"/> at the specified absolute due time.</summary>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="dueTime">Absolute time at which to execute the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
    public static IDisposable Schedule(ISequencer scheduler, DateTimeOffset dueTime, Action action)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        ArgumentExceptionHelper.ThrowIfNull(action);

        return Schedule(scheduler, action, dueTime, static (_, a) => Invoke(a));
    }

    /// <summary>Schedules a recursive action on <paramref name="scheduler"/>.</summary>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="action">Action to execute; its argument schedules the next iteration.</param>
    /// <returns>The disposable object used to cancel the recursive work (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
    public static IDisposable Schedule(ISequencer scheduler, Action<Action> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        ArgumentExceptionHelper.ThrowIfNull(action);

        return new RecursiveScheduleState(scheduler, action).Start();
    }

    /// <summary>Schedules a stateful action on <paramref name="scheduler"/>.</summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">A state object to be passed to <paramref name="action"/>.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IDisposable ScheduleAction<TState>(ISequencer scheduler, TState state, Action<TState> action) =>
        Schedule(scheduler, state, action);

    /// <summary>Schedules a stateful disposable-returning action on <paramref name="scheduler"/>.</summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">A state object to be passed to <paramref name="action"/>.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
    public static IDisposable ScheduleAction<TState>(ISequencer scheduler, TState state, Func<TState, IDisposable> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        ArgumentExceptionHelper.ThrowIfNull(action);

        return Schedule(
            scheduler,
            (action, state),
            static (_, tuple) => tuple.action(tuple.state));
    }

    /// <summary>Schedules a stateful action on <paramref name="scheduler"/> after the specified relative due time.</summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">A state object to be passed to <paramref name="action"/>.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
    public static IDisposable ScheduleAction<TState>(ISequencer scheduler, TState state, TimeSpan dueTime, Action<TState> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        ArgumentExceptionHelper.ThrowIfNull(action);

        return Schedule(scheduler, (state, action), dueTime, static (_, tuple) => Invoke(tuple));
    }

    /// <summary>Schedules a stateful disposable-returning action on <paramref name="scheduler"/> after the specified relative due time.</summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">A state object to be passed to <paramref name="action"/>.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
    public static IDisposable ScheduleAction<TState>(ISequencer scheduler, TState state, TimeSpan dueTime, Func<TState, IDisposable> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        ArgumentExceptionHelper.ThrowIfNull(action);

        return Schedule(scheduler, (state, action), dueTime, static (_, tuple) => Invoke(tuple));
    }

    /// <summary>Schedules a stateful action on <paramref name="scheduler"/> at the specified absolute due time.</summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">A state object to be passed to <paramref name="action"/>.</param>
    /// <param name="dueTime">Absolute time at which to execute the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
    public static IDisposable ScheduleAction<TState>(ISequencer scheduler, TState state, DateTimeOffset dueTime, Action<TState> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        ArgumentExceptionHelper.ThrowIfNull(action);

        return Schedule(scheduler, (state, action), dueTime, static (_, tuple) => Invoke(tuple));
    }

    /// <summary>Schedules a stateful disposable-returning action on <paramref name="scheduler"/> at the specified absolute due time.</summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">A state object to be passed to <paramref name="action"/>.</param>
    /// <param name="dueTime">Absolute time at which to execute the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
    public static IDisposable ScheduleAction<TState>(
        ISequencer scheduler,
        TState state,
        DateTimeOffset dueTime,
        Func<TState, IDisposable> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        ArgumentExceptionHelper.ThrowIfNull(action);

        return Schedule(scheduler, (state, action), dueTime, static (_, tuple) => Invoke(tuple));
    }
}
