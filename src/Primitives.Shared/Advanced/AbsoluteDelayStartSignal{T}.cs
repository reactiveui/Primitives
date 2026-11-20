// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Dedicated signal for absolute <c>DelayStart</c>/<c>DelaySubscription</c> overloads.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("AbsoluteDelayStartSignal: Source = {_source}")]
public sealed class AbsoluteDelayStartSignal<T> : IObservable<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The absolute time at which to subscribe to the source.</summary>
    private readonly DateTimeOffset _dueTime;

    /// <summary>The sequencer used to schedule the delayed subscription.</summary>
    private readonly ISequencer _scheduler;

    /// <summary>Initializes a new instance of the <see cref="AbsoluteDelayStartSignal{T}"/> class on the thread-pool sequencer.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The absolute time at which to subscribe to the source.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public AbsoluteDelayStartSignal(IObservable<T> source, DateTimeOffset dueTime)
        : this(source, dueTime, ThreadPoolSequencer.Instance)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="AbsoluteDelayStartSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The absolute time at which to subscribe to the source.</param>
    /// <param name="scheduler">The sequencer used to schedule the delayed subscription.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="scheduler"/> is <see langword="null"/>.</exception>
    public AbsoluteDelayStartSignal(IObservable<T> source, DateTimeOffset dueTime, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        _source = source;
        _dueTime = dueTime;
        _scheduler = scheduler;
    }

    /// <summary>Gets the sequencer used to schedule the delayed subscription.</summary>
    internal ISequencer Scheduler => _scheduler;

    /// <summary>Creates an <see cref="AbsoluteDelayStartSignal{T}"/> on <paramref name="scheduler"/>, defaulting to the thread pool when <see langword="null"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The absolute time at which to subscribe to the source.</param>
    /// <param name="scheduler">The sequencer used to schedule the delayed subscription, or <see langword="null"/> for the thread pool.</param>
    /// <returns>The delayed-subscription signal.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static AbsoluteDelayStartSignal<T> Create(IObservable<T> source, DateTimeOffset dueTime, ISequencer? scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return new(source, dueTime, scheduler ?? ThreadPoolSequencer.Instance);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var dueTime = Sequencer.Normalize(_dueTime - _scheduler.Now);
        return _source is RangeSignal range && typeof(T) == typeof(int)
            ? new ShiftedRangeSignal<T>(range, dueTime, _scheduler).Subscribe(observer)
            : new DelayStartSignal<T>(_source, dueTime, _scheduler).Subscribe(observer);
    }
}
