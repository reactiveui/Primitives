// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
#if REACTIVE_SHIM
using ReactiveUI.Primitives.Extensions.Reactive.Internal;

namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>Safe scheduling helpers that fall back to inline execution when no sequencer is supplied.</summary>
public static class SafeScheduler
{
    /// <summary>Schedules <paramref name="action"/> immediately when <paramref name="scheduler"/> is <see langword="null"/>, or on <paramref name="scheduler"/> otherwise.</summary>
    /// <param name="scheduler">Optional scheduler.</param>
    /// <param name="action">The action to invoke.</param>
    /// <returns>A disposable representing the scheduled work.</returns>
    public static IDisposable ScheduleSafe(ISequencer? scheduler, Action action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        if (scheduler is not null)
        {
            return scheduler.Schedule(action);
        }

        action();
        return EmptyDisposable.Instance;
    }

    /// <summary>Schedules <paramref name="action"/> after <paramref name="dueTime"/>, blocking synchronously when <paramref name="scheduler"/> is <see langword="null"/>.</summary>
    /// <param name="scheduler">Optional scheduler.</param>
    /// <param name="dueTime">The delay before invoking <paramref name="action"/>.</param>
    /// <param name="action">The action to invoke.</param>
    /// <returns>A disposable representing the scheduled work.</returns>
    public static IDisposable ScheduleSafe(ISequencer? scheduler, TimeSpan dueTime, Action action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        return scheduler is null
            ? ScheduledWait.Run(dueTime, action)
            : scheduler.Schedule(dueTime, action);
    }
}
