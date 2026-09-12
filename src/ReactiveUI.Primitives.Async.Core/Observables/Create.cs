// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides factory methods for creating asynchronous observables and background jobs that emit values to observers asynchronously.</summary>
/// <remarks>These are the entry points for turning an arbitrary asynchronous producer into a sequence: supply the
/// subscribe logic yourself, or hand over a job to run per subscriber.</remarks>
public static partial class SignalAsync
{
    /// <summary>Creates a new asynchronous observable sequence using the specified subscription function.</summary>
    /// <typeparam name="T">The type of the elements produced by the observable sequence.</typeparam>
    /// <param name="subscribeAsync">Wires up one observer and yields the handle that tears that wiring down.</param>
    /// <returns>An observable sequence that runs <paramref name="subscribeAsync"/> for each observer.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="subscribeAsync"/> is <see langword="null"/>.</exception>
    /// <remarks>The disposable <paramref name="subscribeAsync"/> returns owns the subscription's resources and must
    /// stop the producer when disposed.</remarks>
    public static IObservableAsync<T> Create<T>(
        Func<IObserverAsync<T>, CancellationToken, ValueTask<IAsyncDisposable>> subscribeAsync) =>
        subscribeAsync is null
            ? throw new ArgumentNullException(nameof(subscribeAsync))
            : new CallbackSignalAsync<T>(subscribeAsync);

    /// <summary>Creates a new observable sequence that runs the specified asynchronous job as a background task.</summary>
    /// <typeparam name="T">The type of elements produced by the observable sequence.</typeparam>
    /// <param name="job">Pushes results into the observer it is handed, stopping when its token is cancelled.</param>
    /// <returns>An observable sequence that runs <paramref name="job"/> on a scheduled task per subscriber.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<T> CreateAsBackgroundJob<T>(
        Func<IObserverAsync<T>, CancellationToken, ValueTask> job) =>
        new BackgroundJobSignal<T>(job, false, null);

    /// <summary>Creates a new observable sequence that runs the specified asynchronous job per subscriber, optionally on the subscribing thread.</summary>
    /// <typeparam name="T">The type of elements produced by the observable sequence.</typeparam>
    /// <param name="job">Pushes results into the observer it is handed, stopping when its token is cancelled.</param>
    /// <param name="startSynchronously"><see langword="true"/> to run the job on the subscribing thread, so subscribe
    /// does not return until the job yields; <see langword="false"/> to schedule it.</param>
    /// <returns>An observable sequence that runs <paramref name="job"/> per subscriber.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<T> CreateAsBackgroundJob<T>(
        Func<IObserverAsync<T>, CancellationToken, ValueTask> job,
        bool startSynchronously) =>
        new BackgroundJobSignal<T>(job, startSynchronously, null);

    /// <summary>Creates a new observable sequence that runs the specified asynchronous job as a background task using the provided task scheduler.</summary>
    /// <typeparam name="T">The type of the elements produced by the observable sequence.</typeparam>
    /// <param name="job">Pushes results into the observer it is handed, stopping when its token is cancelled.</param>
    /// <param name="taskScheduler">The scheduler each subscriber's job is queued to.</param>
    /// <returns>An observable sequence that runs <paramref name="job"/> per subscriber on
    /// <paramref name="taskScheduler"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<T> CreateAsBackgroundJob<T>(
        Func<IObserverAsync<T>, CancellationToken, ValueTask> job,
        TaskScheduler taskScheduler) =>
        new BackgroundJobSignal<T>(job, false, taskScheduler);
}
