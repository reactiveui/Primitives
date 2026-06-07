// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides extension methods for comparing <see cref="AsyncContext"/> instances with the current async context.
/// </summary>
public static class AsyncContextMixins
{
    /// <summary>
    /// Determines whether the specified <see cref="AsyncContext"/> represents the current asynchronous context.
    /// </summary>
    /// <remarks>This method compares the <see cref="SynchronizationContext"/>, <see cref="TaskScheduler"/>, or sequencer adapter
    /// of the provided <see cref="AsyncContext"/> with the current context to determine equivalence. Use this method to
    /// check if code is executing within the intended asynchronous environment.</remarks>
    /// <param name="this">The <see cref="AsyncContext"/> instance to compare with the current context.</param>
    /// <returns><see langword="true"/> if the specified <see cref="AsyncContext"/> matches the current <see
    /// cref="SynchronizationContext"/> or <see cref="TaskScheduler"/>; otherwise, <see langword="false"/>.</returns>
    public static bool IsSameAsCurrentAsyncContext(this AsyncContext @this)
    {
        ArgumentExceptionHelper.ThrowIfNull(@this);

        if (@this.SynchronizationContext is not null)
        {
            return @this.SynchronizationContext == SynchronizationContext.Current;
        }

        if (@this.Sequencer is not null)
        {
            return TaskScheduler.Current is AsyncContext.SequencerTaskScheduler adapter &&
                   ReferenceEquals(adapter.Sequencer, @this.Sequencer);
        }

        if (@this.TaskScheduler is not null)
        {
            return @this.TaskScheduler == TaskScheduler.Current;
        }

        return TaskScheduler.Current == TaskScheduler.Default;
    }
}
