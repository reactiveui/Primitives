// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive;
#else
namespace ReactiveUI.Primitives.Async;
#endif

/// <summary>Provides extension methods for comparing <see cref="AsyncContext"/> instances with the current async context.</summary>
public static class AsyncContextExtensions
{
    /// <summary>Current-context comparison operators for an <see cref="AsyncContext"/> instance.</summary>
    /// <param name="context">The <see cref="AsyncContext"/> instance to compare with the current context.</param>
    extension(AsyncContext context)
    {
        /// <summary>Determines whether the specified <see cref="AsyncContext"/> represents the current asynchronous context.</summary>
        /// <returns><see langword="true"/> if the specified <see cref="AsyncContext"/> matches the current <see
        /// cref="SynchronizationContext"/> or <see cref="TaskScheduler"/>; otherwise, <see langword="false"/>.</returns>
        /// <remarks>This method compares the <see cref="SynchronizationContext"/>, <see cref="TaskScheduler"/>, or sequencer adapter
        /// of the provided <see cref="AsyncContext"/> with the current context to determine equivalence. Use this method to
        /// check if code is executing within the intended asynchronous environment.</remarks>
        public bool IsSameAsCurrentAsyncContext()
        {
            ArgumentExceptionHelper.ThrowIfNull(context);

            if (context.SynchronizationContext is not null)
            {
                return context.SynchronizationContext == SynchronizationContext.Current;
            }

            if (context.Sequencer is not null)
            {
                return TaskScheduler.Current is AsyncContext.SequencerTaskScheduler adapter
                       && ReferenceEquals(adapter.Sequencer, context.Sequencer);
            }

            return context.TaskScheduler is not null
                ? context.TaskScheduler == TaskScheduler.Current
                : TaskScheduler.Current == TaskScheduler.Default;
        }
    }
}
