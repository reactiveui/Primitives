// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Async observable that emits items from the source until the specified task completes.</summary>
/// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
/// <param name="source">The source observable sequence.</param>
/// <param name="task">The task whose completion triggers the end of the sequence.</param>
/// <param name="options">Options controlling the take-until behavior.</param>
/// <param name="stopToken">A token whose cancellation also completes the sequence.</param>
[System.Diagnostics.DebuggerDisplay("TaskStopSignal: Source = {Source}")]
public sealed class TaskStopSignal<T>(
    IObservableAsync<T> source,
    Task task,
    TakeUntilOptions options,
    CancellationToken stopToken) : IObservableAsync<T>
{
    /// <summary>Initializes a new instance of the <see cref="TaskStopSignal{T}"/> class.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="task">The task whose completion triggers the end of the sequence.</param>
    /// <param name="options">Options controlling the take-until behavior.</param>
    public TaskStopSignal(
        IObservableAsync<T> source,
        Task task,
        TakeUntilOptions options)
        : this(source, task, options, default)
    {
    }

    /// <summary>Gets the source observable sequence.</summary>
    internal IObservableAsync<T> Source { get; } = source;

    /// <summary>Gets the task whose completion triggers the end of the sequence.</summary>
    internal Task Task { get; } = task;

    /// <summary>Gets options controlling the take-until behavior.</summary>
    internal TakeUntilOptions Options { get; } = options;

    /// <summary>Gets a token whose cancellation also completes the sequence.</summary>
    internal CancellationToken StopToken { get; } = stopToken;

    /// <summary>Creates a <see cref="TaskStopSignal{T}"/> with default options.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="task">The task whose completion triggers the end of the sequence.</param>
    /// <returns>A configured <see cref="TaskStopSignal{T}"/>.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public static TaskStopSignal<T> Create(IObservableAsync<T> source, Task task) =>
        Create(source, task, null, default);

    /// <summary>Creates a <see cref="TaskStopSignal{T}"/> with optional options.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="task">The task whose completion triggers the end of the sequence.</param>
    /// <param name="options">Optional options; <see langword="null"/> uses <see cref="TakeUntilOptions.Default"/>.</param>
    /// <returns>A configured <see cref="TaskStopSignal{T}"/>.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public static TaskStopSignal<T> Create(IObservableAsync<T> source, Task task, TakeUntilOptions? options) =>
        Create(source, task, options, default);

    /// <summary>Creates a <see cref="TaskStopSignal{T}"/> with default options and a stop token.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="task">The task whose completion triggers the end of the sequence.</param>
    /// <param name="stopToken">A token whose cancellation also completes the sequence.</param>
    /// <returns>A configured <see cref="TaskStopSignal{T}"/>.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public static TaskStopSignal<T> Create(IObservableAsync<T> source, Task task, CancellationToken stopToken) =>
        Create(source, task, null, stopToken);

    /// <summary>Creates a <see cref="TaskStopSignal{T}"/> with optional options and a stop token.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="task">The task whose completion triggers the end of the sequence.</param>
    /// <param name="options">Optional options; <see langword="null"/> uses <see cref="TakeUntilOptions.Default"/>.</param>
    /// <param name="stopToken">A token whose cancellation also completes the sequence.</param>
    /// <returns>A configured <see cref="TaskStopSignal{T}"/>.</returns>
    public static TaskStopSignal<T> Create(
        IObservableAsync<T> source,
        Task task,
        TakeUntilOptions? options,
        CancellationToken stopToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return new(source, task, options ?? TakeUntilOptions.Default, stopToken);
    }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        TaskStopCoordinator<T> subscription = new(this, observer);
        subscription.LinkExternalCancellation(cancellationToken);
        return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
            subscription,
            () => subscription.SubscribeSourcesAsync(cancellationToken));
    }
}
