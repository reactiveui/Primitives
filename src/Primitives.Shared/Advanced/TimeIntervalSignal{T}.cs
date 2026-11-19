// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Signal that measures elapsed time between source values.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("TimeIntervalSignal: Source = {Source}, Scheduler = {Scheduler}")]
public sealed class TimeIntervalSignal<T> : IObservable<TimeInterval<T>>
{
    /// <summary>Initializes a new instance of the <see cref="TimeIntervalSignal{T}"/> class using <see cref="Sequencer.Immediate"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public TimeIntervalSignal(IObservable<T> source)
        : this(source, Sequencer.Immediate)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="TimeIntervalSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="sequencer">The sequencer that supplies timestamps.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="sequencer"/> is <see langword="null"/>.</exception>
    public TimeIntervalSignal(IObservable<T> source, ISequencer sequencer)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(sequencer);

        Source = source;
        Scheduler = sequencer;
    }

    /// <summary>Gets the source observable.</summary>
    private IObservable<T> Source { get; }

    /// <summary>Gets the sequencer that supplies timestamps.</summary>
    private ISequencer Scheduler { get; }

    /// <summary>Creates a time-interval sequence using <see cref="Sequencer.Immediate"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <returns>A sequence containing each value with its elapsed interval.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<TimeInterval<T>> Create(IObservable<T> source) =>
        Create(source, Sequencer.Immediate);

    /// <summary>Creates a time-interval sequence using <paramref name="scheduler"/>, defaulting to <see cref="Sequencer.Immediate"/> when <see langword="null"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="scheduler">The sequencer that supplies timestamps, or <see langword="null"/> for immediate.</param>
    /// <returns>A sequence containing each value with its elapsed interval.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<TimeInterval<T>> Create(IObservable<T> source, ISequencer? scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        scheduler ??= Sequencer.Immediate;
        return source is RangeSignal range && typeof(T) == typeof(int)
            ? new TimeIntervalRangeSignal<T>(range, scheduler)
            : new TimeIntervalSignal<T>(source, scheduler);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TimeInterval<T>> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        TimeIntervalWitness<T> sink = new(observer, Scheduler);
        sink.SetSubscription(Source.Subscribe(sink));
        return sink;
    }
}
