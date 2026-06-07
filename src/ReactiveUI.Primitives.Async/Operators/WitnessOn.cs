// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides extension methods for controlling the execution context of asynchronous observable sequences.
/// </summary>
/// <remarks>The methods in this class allow you to specify the context on which observer callbacks are invoked
/// for an asynchronous observable sequence. This is useful for ensuring that notifications are delivered on a
/// particular synchronization or task context, such as a UI thread or a custom scheduler.</remarks>
public static partial class SignalAsync
{
    /// <summary>
    /// Wraps the source observable so that observer callbacks are invoked on the specified async context.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="asyncContext">The async context on which observer callbacks should be invoked.</param>
    /// <param name="forceYielding">When true, forces an asynchronous yield before invoking each callback, even if already on the target
    /// context.</param>
    /// <returns>An observable sequence whose observer callbacks execute on the specified context.</returns>
    public static IObservableAsync<T> WitnessOn<T>(this IObservableAsync<T> @this, AsyncContext asyncContext, bool forceYielding) =>
        new ContextSwitchSignalAsync<T>(@this, asyncContext, forceYielding);

    /// <summary>
    /// Wraps the source observable so that observer callbacks are invoked on the specified async context.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="asyncContext">The async context on which observer callbacks should be invoked.</param>
    /// <returns>An observable sequence whose observer callbacks execute on the specified context.</returns>
    public static IObservableAsync<T> WitnessOn<T>(this IObservableAsync<T> @this, AsyncContext asyncContext) =>
        @this.WitnessOn(asyncContext, false);

    /// <summary>
    /// Wraps the source observable so that observer callbacks are invoked on the specified synchronization context.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="synchronizationContext">The synchronization context on which observer callbacks should be posted.</param>
    /// <param name="forceYielding">When true, forces an asynchronous yield before invoking each callback.</param>
    /// <returns>An observable sequence whose observer callbacks execute on the specified synchronization context.</returns>
    public static IObservableAsync<T> WitnessOn<T>(this IObservableAsync<T> @this, SynchronizationContext synchronizationContext, bool forceYielding)
    {
        var asyncContext = AsyncContext.From(synchronizationContext);
        return new ContextSwitchSignalAsync<T>(@this, asyncContext, forceYielding);
    }

    /// <summary>
    /// Wraps the source observable so that observer callbacks are invoked on the specified synchronization context.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="synchronizationContext">The synchronization context on which observer callbacks should be posted.</param>
    /// <returns>An observable sequence whose observer callbacks execute on the specified synchronization context.</returns>
    public static IObservableAsync<T> WitnessOn<T>(this IObservableAsync<T> @this, SynchronizationContext synchronizationContext) =>
        @this.WitnessOn(synchronizationContext, false);

    /// <summary>
    /// Wraps the source observable so that observer callbacks are invoked using the specified task scheduler.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="taskScheduler">The task scheduler on which observer callbacks should be scheduled.</param>
    /// <param name="forceYielding">When true, forces an asynchronous yield before invoking each callback.</param>
    /// <returns>An observable sequence whose observer callbacks execute on the specified task scheduler.</returns>
    public static IObservableAsync<T> WitnessOn<T>(this IObservableAsync<T> @this, TaskScheduler taskScheduler, bool forceYielding)
    {
        var asyncContext = AsyncContext.From(taskScheduler);
        return new ContextSwitchSignalAsync<T>(@this, asyncContext, forceYielding);
    }

    /// <summary>
    /// Wraps the source observable so that observer callbacks are invoked using the specified task scheduler.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="taskScheduler">The task scheduler on which observer callbacks should be scheduled.</param>
    /// <returns>An observable sequence whose observer callbacks execute on the specified task scheduler.</returns>
    public static IObservableAsync<T> WitnessOn<T>(this IObservableAsync<T> @this, TaskScheduler taskScheduler) =>
        @this.WitnessOn(taskScheduler, false);

    /// <summary>
    /// Configures the observable sequence to notify observers on the specified scheduler.
    /// </summary>
    /// <remarks>Use this method to control the context (such as a UI thread or a specific task
    /// scheduler) on which observers receive notifications. This is useful for ensuring thread safety or updating
    /// UI elements from observable sequences.</remarks>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="scheduler">The scheduler on which to observe and deliver notifications to observers. Cannot be null.</param>
    /// <param name="forceYielding">true to force yielding to the scheduler even if already on the target context; otherwise, false.</param>
    /// <returns>An observable sequence whose notifications are delivered on the specified scheduler.</returns>
    public static IObservableAsync<T> WitnessOn<T>(this IObservableAsync<T> @this, ISequencer scheduler, bool forceYielding)
    {
        var asyncContext = AsyncContext.From(scheduler);
        return new ContextSwitchSignalAsync<T>(@this, asyncContext, forceYielding);
    }

    /// <summary>
    /// Configures the observable sequence to notify observers on the specified scheduler.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="scheduler">The scheduler on which to observe and deliver notifications to observers. Cannot be null.</param>
    /// <returns>An observable sequence whose notifications are delivered on the specified scheduler.</returns>
    public static IObservableAsync<T> WitnessOn<T>(this IObservableAsync<T> @this, ISequencer scheduler) =>
        @this.WitnessOn(scheduler, false);
}
