// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>Provides factory methods for creating task-backed signals.</summary>
public static class TaskSignal
{
    /// <summary>Creates a task-backed signal whose source the factory builds from the signal itself.</summary>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="observableFactory">Builds the source, receiving the signal it will belong to.</param>
    /// <returns>A task-backed signal that notifies on the current thread and owns its own cancellation source.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="observableFactory"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ITaskSignal<TResult> Create<TResult>(
        Func<ITaskSignal<TResult>, IObservable<TResult>> observableFactory) =>
        Instance(observableFactory, null, null);

    /// <summary>Creates a task-backed signal that notifies on the supplied sequencer.</summary>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="observableFactory">Builds the source, receiving the signal it will belong to.</param>
    /// <param name="scheduler">The sequencer notifications are delivered on, or <see langword="null"/> for the current thread.</param>
    /// <returns>A task-backed signal that owns its own cancellation source.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="observableFactory"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ITaskSignal<TResult> Create<TResult>(
        Func<ITaskSignal<TResult>, IObservable<TResult>> observableFactory,
        ISequencer? scheduler) =>
        Instance(observableFactory, scheduler, null);

    /// <summary>Creates a task-backed signal that notifies on the supplied sequencer and cancels through the supplied source.</summary>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="observableFactory">Builds the source, receiving the signal it will belong to.</param>
    /// <param name="scheduler">The sequencer notifications are delivered on, or <see langword="null"/> for the current thread.</param>
    /// <param name="cancellationTokenSource">The cancellation source to observe, or <see langword="null"/> to own a new one.</param>
    /// <returns>A task-backed signal.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="observableFactory"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ITaskSignal<TResult> Create<TResult>(
        Func<ITaskSignal<TResult>, IObservable<TResult>> observableFactory,
        ISequencer? scheduler,
        CancellationTokenSource? cancellationTokenSource) =>
        Instance(observableFactory, scheduler, cancellationTokenSource);

    /// <summary>Validates the factory and builds the signal the public overloads return.</summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="observableFactory">Builds the source from the signal.</param>
    /// <param name="scheduler">The sequencer notifications are delivered on.</param>
    /// <param name="cancellationTokenSource">The cancellation source to observe.</param>
    /// <returns>The built signal.</returns>
    private static TaskSignal<TResult> Instance<TResult>(
        Func<ITaskSignal<TResult>, IObservable<TResult>> observableFactory,
        ISequencer? scheduler,
        CancellationTokenSource? cancellationTokenSource)
    {
        ArgumentExceptionHelper.ThrowIfNull(observableFactory);

        return TaskSignal<TResult>.Create(observableFactory, scheduler, cancellationTokenSource);
    }
}
