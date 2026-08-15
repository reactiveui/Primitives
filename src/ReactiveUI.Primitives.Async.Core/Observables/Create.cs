// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides factory methods for creating asynchronous observables and background jobs that emit values to observers
/// asynchronously.
/// </summary>
/// <remarks>The methods in this class allow developers to construct asynchronous observables by supplying custom
/// subscription logic or background jobs. Observables created with these methods support asynchronous notification and
/// cancellation, enabling integration with modern async workflows. Use these methods to bridge asynchronous producers
/// with consumers following the observer pattern.</remarks>
public static partial class SignalAsync
{
    /// <summary>Creates a new asynchronous observable sequence using the specified subscription function.</summary>
    /// <typeparam name="T">The type of the elements produced by the observable sequence.</typeparam>
    /// <param name="subscribeAsync">A function that is invoked when an observer subscribes to the sequence. The function receives an asynchronous
    /// observer and a cancellation token, and returns a task that yields a disposable resource representing the
    /// subscription.</param>
    /// <returns>An SignalAsync{T} that invokes the specified subscription function for each observer.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="subscribeAsync"/> is <see langword="null"/>.</exception>
    /// <remarks>The subscription function is responsible for handling observer notifications and managing the
    /// lifetime of the subscription. The returned disposable should release any resources or cancel ongoing operations
    /// when disposed.</remarks>
    public static IObservableAsync<T> Create<T>(
        Func<IObserverAsync<T>, CancellationToken, ValueTask<IAsyncDisposable>> subscribeAsync) =>
        subscribeAsync is null
            ? throw new ArgumentNullException(nameof(subscribeAsync))
            : new CallbackSignalAsync<T>(subscribeAsync);

    /// <summary>Creates a new observable sequence that runs the specified asynchronous job as a background task.</summary>
    /// <typeparam name="T">The type of elements produced by the observable sequence.</typeparam>
    /// <param name="job">A delegate that defines the asynchronous job to execute. The delegate receives an observer to report results and
    /// a cancellation token to observe cancellation requests.</param>
    /// <returns>An SignalAsync{T} that represents the observable sequence produced by the background job.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<T> CreateAsBackgroundJob<T>(
        Func<IObserverAsync<T>, CancellationToken, ValueTask> job) =>
        new BackgroundJobSignal<T>(job, false, null);

    /// <summary>Creates a new observable sequence that runs the specified asynchronous job as a background task.</summary>
    /// <typeparam name="T">The type of elements produced by the observable sequence.</typeparam>
    /// <param name="job">A delegate that defines the asynchronous job to execute. The delegate receives an observer to report results and
    /// a cancellation token to observe cancellation requests.</param>
    /// <param name="startSynchronously">true to start the job synchronously on the calling thread; otherwise, false to schedule it to run
    /// asynchronously.</param>
    /// <returns>An SignalAsync{T} that represents the observable sequence produced by the background job.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<T> CreateAsBackgroundJob<T>(
        Func<IObserverAsync<T>, CancellationToken, ValueTask> job,
        bool startSynchronously) =>
        new BackgroundJobSignal<T>(job, startSynchronously, null);

    /// <summary>
    /// Creates a new observable sequence that runs the specified asynchronous job as a background task using the
    /// provided task scheduler.
    /// </summary>
    /// <typeparam name="T">The type of the elements produced by the observable sequence.</typeparam>
    /// <param name="job">A delegate that defines the asynchronous job to execute. The delegate receives an observer to report results and
    /// a cancellation token to observe cancellation requests.</param>
    /// <param name="taskScheduler">The task scheduler that is used to schedule the background job.</param>
    /// <returns>An SignalAsync{T} that represents the asynchronous background job and emits the results produced by the job.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<T> CreateAsBackgroundJob<T>(
        Func<IObserverAsync<T>, CancellationToken, ValueTask> job,
        TaskScheduler taskScheduler) =>
        new BackgroundJobSignal<T>(job, false, taskScheduler);
}
