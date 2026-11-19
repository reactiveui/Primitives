// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

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
        /// <remarks>A sequencer matches only when the current task scheduler wraps that same sequencer instance.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsSameAsCurrentAsyncContext() => AsyncContext.IsSameAsCurrent(context);
    }
}
