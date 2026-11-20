// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Dedicated signal for <c>DelayStart</c> (delay the subscription itself).</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("DelayStartSignal: Source = {_source}")]
public sealed class DelayStartSignal<T> : IObservable<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The delay before subscribing to the source.</summary>
    private readonly TimeSpan _dueTime;

    /// <summary>The sequencer used to schedule the delayed subscription.</summary>
    private readonly ISequencer _scheduler;

    /// <summary>Initializes a new instance of the <see cref="DelayStartSignal{T}"/> class on the thread-pool sequencer.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The delay before subscribing to the source.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public DelayStartSignal(IObservable<T> source, TimeSpan dueTime)
        : this(source, dueTime, ThreadPoolSequencer.Instance)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="DelayStartSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The delay before subscribing to the source.</param>
    /// <param name="scheduler">The sequencer used to schedule the delayed subscription.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="scheduler"/> is <see langword="null"/>.</exception>
    public DelayStartSignal(IObservable<T> source, TimeSpan dueTime, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        _source = source;
        _dueTime = dueTime;
        _scheduler = scheduler;
    }

    /// <summary>Creates a delayed-subscription sequence on the thread-pool sequencer, using <see cref="ShiftedRangeSignal{T}"/> for integer ranges.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The delay before subscribing to the source.</param>
    /// <returns>The delayed-subscription sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<T> Create(IObservable<T> source, TimeSpan dueTime) =>
        Create(source, dueTime, ThreadPoolSequencer.Instance);

    /// <summary>Creates a delayed-subscription sequence on <paramref name="scheduler"/>, using <see cref="ShiftedRangeSignal{T}"/> for integer ranges.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The delay before subscribing to the source.</param>
    /// <param name="scheduler">The sequencer used to schedule the delayed subscription, or <see langword="null"/> for the thread pool.</param>
    /// <returns>The delayed-subscription sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        var sequencer = scheduler ?? ThreadPoolSequencer.Instance;
        return source is RangeSignal range && typeof(T) == typeof(int)
            ? new ShiftedRangeSignal<T>(range, Sequencer.Normalize(dueTime), sequencer)
            : new DelayStartSignal<T>(source, dueTime, sequencer);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return _scheduler.Schedule(
            (Source: _source, Observer: observer),
            Sequencer.Normalize(_dueTime),
            static (_, s) => s.Source.Subscribe(s.Observer));
    }
}
