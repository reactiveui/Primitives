// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Provides a set of extension methods for virtual time scheduling.</summary>
public static class VirtualTimeSequencerExtensions
{
    /// <summary>Provides virtual-time scheduling extensions for <see cref="VirtualClock"/>.</summary>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    extension(VirtualClock scheduler)
    {
        /// <summary>Schedules an action to be executed at <paramref name="dueTime"/>.</summary>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        /// <param name="action">Action to be executed.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
        public IDisposable ScheduleRelative(TimeSpan dueTime, Action action)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            ArgumentExceptionHelper.ThrowIfNull(action);

            return scheduler.ScheduleRelative(action, dueTime, static (_, a) => Invoke(a));
        }

        /// <summary>Schedules an action to be executed at <paramref name="dueTime"/>.</summary>
        /// <param name="dueTime">Absolute time at which to execute the action.</param>
        /// <param name="action">Action to be executed.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
        public IDisposable ScheduleAbsolute(DateTimeOffset dueTime, Action action)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            ArgumentExceptionHelper.ThrowIfNull(action);

            return scheduler.ScheduleAbsolute(action, dueTime, static (_, a) => Invoke(a));
        }
    }

    /// <summary>Provides virtual-time scheduling extensions for <see cref="VirtualTimeSequencer{TAbsolute, TRelative}"/>.</summary>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <typeparam name="TAbsolute">Absolute time representation type.</typeparam>
    /// <typeparam name="TRelative">Relative time representation type.</typeparam>
    extension<TAbsolute, TRelative>(VirtualTimeSequencer<TAbsolute, TRelative> scheduler)
        where TAbsolute : IComparable<TAbsolute>
    {
        /// <summary>Schedules an action to be executed at <paramref name="dueTime"/>.</summary>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        /// <param name="action">Action to be executed.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
        public IDisposable ScheduleRelative(TRelative dueTime, Action action)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            ArgumentExceptionHelper.ThrowIfNull(action);

            // As stated in Sequencer.Simple.cs,
            // an anonymous delegate will allow delegate caching.
            // Watch https://github.com/dotnet/roslyn/issues/5835 for compiler
            // support for caching delegates from method groups.
            return scheduler.ScheduleRelative(action, dueTime, static (_, a) => Invoke(a));
        }

        /// <summary>Schedules an action to be executed at <paramref name="dueTime"/>.</summary>
        /// <param name="dueTime">Absolute time at which to execute the action.</param>
        /// <param name="action">Action to be executed.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
        public IDisposable ScheduleAbsolute(TAbsolute dueTime, Action action)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            ArgumentExceptionHelper.ThrowIfNull(action);

            return scheduler.ScheduleAbsolute(action, dueTime, static (_, a) => Invoke(a));
        }
    }

    /// <summary>Invokes an action and returns an empty disposable.</summary>
    /// <param name="action">Action to invoke.</param>
    /// <returns>An empty disposable.</returns>
    private static EmptyDisposable Invoke(Action action)
    {
        action();
        return EmptyDisposable.Instance;
    }
}
