// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Dedicated signal for <c>Calm</c> (quiet-period debounce).</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("CalmSignal: DueTime = {_dueTime}, Source = {_source}")]
public sealed class CalmSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The quiet period.</summary>
    private readonly TimeSpan _dueTime;

    /// <summary>The sequencer used to schedule quiet-period timers.</summary>
    private readonly ISequencer _scheduler;

    /// <summary>Initializes a new instance of the <see cref="CalmSignal{T}"/> class on the thread-pool sequencer.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The quiet period.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public CalmSignal(IObservable<T> source, TimeSpan dueTime)
        : this(source, dueTime, ThreadPoolSequencer.Instance)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CalmSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The quiet period.</param>
    /// <param name="scheduler">The sequencer used to schedule quiet-period timers.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="scheduler"/> is <see langword="null"/>.</exception>
    public CalmSignal(IObservable<T> source, TimeSpan dueTime, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        _source = source;
        _dueTime = dueTime;
        _scheduler = scheduler;
    }

    /// <summary>Creates a <see cref="CalmSignal{T}"/> on <paramref name="scheduler"/>, defaulting to the thread pool when <see langword="null"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The quiet period.</param>
    /// <param name="scheduler">The sequencer used to schedule quiet-period timers, or <see langword="null"/> for the thread pool.</param>
    /// <returns>The quiet-period signal.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static CalmSignal<T> Create(IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return new(source, dueTime, scheduler ?? ThreadPoolSequencer.Instance);
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() => _scheduler == Sequencer.CurrentThread;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        CalmCoordinator<T> coordinator = new(_source, _dueTime, _scheduler);
        return !IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired
            ? coordinator.Run(observer)
            : CurrentThreadSequencer.RunTrampoline(
                (coordinator, observer),
                static s => s.coordinator.Run(s.observer));
    }
}
