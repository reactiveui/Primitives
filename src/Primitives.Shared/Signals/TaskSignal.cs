// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>Provides factory methods for creating task-backed signals.</summary>
public static class TaskSignal
{
    /// <summary>Creates the specified source.</summary>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="observableFactory">The observable factory.</param>
    /// <returns>
    /// An AsyncObservable.
    /// </returns>
    /// <exception cref="ArgumentExceptionHelper">observableFactory.</exception>
    public static ITaskSignal<TResult> Create<TResult>(
        Func<ITaskSignal<TResult>, IObservable<TResult>> observableFactory) =>
        Instance(observableFactory, null, null);

    /// <summary>Creates the specified source.</summary>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="observableFactory">The observable factory.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>
    /// An AsyncObservable.
    /// </returns>
    /// <exception cref="ArgumentExceptionHelper">observableFactory.</exception>
    public static ITaskSignal<TResult> Create<TResult>(
        Func<ITaskSignal<TResult>, IObservable<TResult>> observableFactory,
        ISequencer? scheduler) =>
        Instance(observableFactory, scheduler, null);

    /// <summary>Creates the specified source.</summary>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="observableFactory">The observable factory.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <param name="cancellationTokenSource">The cancellation token source.</param>
    /// <returns>
    /// An AsyncObservable.
    /// </returns>
    /// <exception cref="ArgumentExceptionHelper">observableFactory.</exception>
    public static ITaskSignal<TResult> Create<TResult>(
        Func<ITaskSignal<TResult>, IObservable<TResult>> observableFactory,
        ISequencer? scheduler,
        CancellationTokenSource? cancellationTokenSource) =>
        Instance(observableFactory, scheduler, cancellationTokenSource);

    /// <summary>Executes the Instance operation.</summary>
    /// <typeparam name="TResult">The TResult type.</typeparam>
    /// <param name="observableFactory">The observableFactory value.</param>
    /// <param name="scheduler">The scheduler value.</param>
    /// <param name="cancellationTokenSource">The cancellationTokenSource value.</param>
    /// <returns>The result.</returns>
    private static TaskSignal<TResult> Instance<TResult>(
        Func<ITaskSignal<TResult>, IObservable<TResult>> observableFactory,
        ISequencer? scheduler,
        CancellationTokenSource? cancellationTokenSource)
    {
        ArgumentExceptionHelper.ThrowIfNull(observableFactory);

        return TaskSignal<TResult>.Create(observableFactory, scheduler, cancellationTokenSource);
    }
}
