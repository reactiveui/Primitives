// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Async.Advanced;
#endif

/// <summary>Switches source notifications onto an async context before forwarding them downstream.</summary>
/// <typeparam name="T">The source element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("WitnessOnSignal: Source = {Source}, ForceYielding = {ForceYielding}")]
public sealed class WitnessOnSignal<T> : IObservableAsync<T>
{
    /// <summary>Initializes a new instance of the <see cref="WitnessOnSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="asyncContext">The target async context.</param>
    /// <param name="forceYielding">true to force yielding before every forwarded notification; otherwise, false.</param>
    public WitnessOnSignal(IObservableAsync<T> source, AsyncContext asyncContext, bool forceYielding)
    {
        Source = source;
        AsyncContext = asyncContext;
        ForceYielding = forceYielding;
    }

    /// <summary>Initializes a new instance of the <see cref="WitnessOnSignal{T}"/> class without forced yielding.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="asyncContext">The target async context.</param>
    public WitnessOnSignal(IObservableAsync<T> source, AsyncContext asyncContext)
        : this(source, asyncContext, false)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="WitnessOnSignal{T}"/> class for a synchronization context.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="synchronizationContext">The target synchronization context.</param>
    /// <param name="forceYielding">true to force yielding before every forwarded notification; otherwise, false.</param>
    public WitnessOnSignal(IObservableAsync<T> source, SynchronizationContext synchronizationContext, bool forceYielding)
        : this(source, AsyncContext.From(synchronizationContext), forceYielding)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="WitnessOnSignal{T}"/> class for a synchronization context without forced yielding.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="synchronizationContext">The target synchronization context.</param>
    public WitnessOnSignal(IObservableAsync<T> source, SynchronizationContext synchronizationContext)
        : this(source, AsyncContext.From(synchronizationContext), false)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="WitnessOnSignal{T}"/> class for a task scheduler.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="taskScheduler">The target task scheduler.</param>
    /// <param name="forceYielding">true to force yielding before every forwarded notification; otherwise, false.</param>
    public WitnessOnSignal(IObservableAsync<T> source, TaskScheduler taskScheduler, bool forceYielding)
        : this(source, AsyncContext.From(taskScheduler), forceYielding)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="WitnessOnSignal{T}"/> class for a task scheduler without forced yielding.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="taskScheduler">The target task scheduler.</param>
    public WitnessOnSignal(IObservableAsync<T> source, TaskScheduler taskScheduler)
        : this(source, AsyncContext.From(taskScheduler), false)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="WitnessOnSignal{T}"/> class for a sequencer.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="scheduler">The target sequencer.</param>
    /// <param name="forceYielding">true to force yielding before every forwarded notification; otherwise, false.</param>
    public WitnessOnSignal(IObservableAsync<T> source, ISequencer scheduler, bool forceYielding)
        : this(source, AsyncContext.From(scheduler), forceYielding)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="WitnessOnSignal{T}"/> class for a sequencer without forced yielding.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="scheduler">The target sequencer.</param>
    public WitnessOnSignal(IObservableAsync<T> source, ISequencer scheduler)
        : this(source, AsyncContext.From(scheduler), false)
    {
    }

    /// <summary>Gets the source observable.</summary>
    private IObservableAsync<T> Source { get; }

    /// <summary>Gets the target async context.</summary>
    private AsyncContext AsyncContext { get; }

    /// <summary>Gets a value indicating whether every notification should yield.</summary>
    private bool ForceYielding { get; }

    /// <summary>Observes <paramref name="source"/> on <paramref name="asyncContext"/> when supplied, or returns <paramref name="source"/> unchanged.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="asyncContext">Optional target async context.</param>
    /// <returns>The context-switched or original observable sequence.</returns>
    public static IObservableAsync<T> CreateSafe(IObservableAsync<T> source, AsyncContext? asyncContext)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        return asyncContext is null ? source : new WitnessOnSignal<T>(source, asyncContext, false);
    }

    /// <summary>Observes <paramref name="source"/> on <paramref name="asyncContext"/> when supplied, or returns <paramref name="source"/> unchanged.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="asyncContext">Optional target async context.</param>
    /// <param name="forceYielding">Whether to force yielding when switching context.</param>
    /// <returns>The context-switched or original observable sequence.</returns>
    public static IObservableAsync<T> CreateSafe(
        IObservableAsync<T> source,
        AsyncContext? asyncContext,
        bool forceYielding)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        return asyncContext is null ? source : new WitnessOnSignal<T>(source, asyncContext, forceYielding);
    }

    /// <summary>Observes <paramref name="source"/> on <paramref name="taskScheduler"/> when supplied, or returns <paramref name="source"/> unchanged.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="taskScheduler">Optional target task scheduler.</param>
    /// <returns>The context-switched or original observable sequence.</returns>
    public static IObservableAsync<T> CreateSafe(IObservableAsync<T> source, TaskScheduler? taskScheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        return taskScheduler is null
            ? source
            : new WitnessOnSignal<T>(source, AsyncContext.From(taskScheduler), false);
    }

    /// <summary>Observes <paramref name="source"/> on <paramref name="taskScheduler"/> when supplied, or returns <paramref name="source"/> unchanged.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="taskScheduler">Optional target task scheduler.</param>
    /// <param name="forceYielding">Whether to force yielding when switching context.</param>
    /// <returns>The context-switched or original observable sequence.</returns>
    public static IObservableAsync<T> CreateSafe(
        IObservableAsync<T> source,
        TaskScheduler? taskScheduler,
        bool forceYielding)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        return taskScheduler is null
            ? source
            : new WitnessOnSignal<T>(source, AsyncContext.From(taskScheduler), forceYielding);
    }

    /// <summary>Observes <paramref name="source"/> on <paramref name="asyncContext"/> only when <paramref name="condition"/> is <see langword="true"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="condition">Whether to switch context.</param>
    /// <param name="asyncContext">The target async context.</param>
    /// <returns>The context-switched or original observable sequence.</returns>
    public static IObservableAsync<T> CreateIf(
        IObservableAsync<T> source,
        bool condition,
        AsyncContext asyncContext)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(asyncContext);
        return condition ? new WitnessOnSignal<T>(source, asyncContext, false) : source;
    }

    /// <summary>Observes <paramref name="source"/> on <paramref name="asyncContext"/> only when <paramref name="condition"/> is <see langword="true"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="condition">Whether to switch context.</param>
    /// <param name="asyncContext">The target async context.</param>
    /// <param name="forceYielding">Whether to force yielding when switching context.</param>
    /// <returns>The context-switched or original observable sequence.</returns>
    public static IObservableAsync<T> CreateIf(
        IObservableAsync<T> source,
        bool condition,
        AsyncContext asyncContext,
        bool forceYielding)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(asyncContext);
        return condition ? new WitnessOnSignal<T>(source, asyncContext, forceYielding) : source;
    }

    /// <summary>Observes <paramref name="source"/> on <paramref name="taskScheduler"/> only when <paramref name="condition"/> is <see langword="true"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="condition">Whether to switch context.</param>
    /// <param name="taskScheduler">The target task scheduler.</param>
    /// <returns>The context-switched or original observable sequence.</returns>
    public static IObservableAsync<T> CreateIf(
        IObservableAsync<T> source,
        bool condition,
        TaskScheduler taskScheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(taskScheduler);
        return condition ? new WitnessOnSignal<T>(source, AsyncContext.From(taskScheduler), false) : source;
    }

    /// <summary>Observes <paramref name="source"/> on <paramref name="taskScheduler"/> only when <paramref name="condition"/> is <see langword="true"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="condition">Whether to switch context.</param>
    /// <param name="taskScheduler">The target task scheduler.</param>
    /// <param name="forceYielding">Whether to force yielding when switching context.</param>
    /// <returns>The context-switched or original observable sequence.</returns>
    public static IObservableAsync<T> CreateIf(
        IObservableAsync<T> source,
        bool condition,
        TaskScheduler taskScheduler,
        bool forceYielding)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(taskScheduler);
        return condition ? new WitnessOnSignal<T>(source, AsyncContext.From(taskScheduler), forceYielding) : source;
    }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        WitnessOnWitness<T> contextSwitchObserver = new(observer, AsyncContext, ForceYielding);
        return WitnessSubscription.SubscribeAsync(Source, contextSwitchObserver, observer, cancellationToken);
    }
}
