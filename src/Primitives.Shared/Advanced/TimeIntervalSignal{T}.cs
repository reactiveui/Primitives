// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Signal that measures elapsed time between source values.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class TimeIntervalSignal<T> : IObservable<TimeInterval<T>>
{
    /// <summary>Initializes a new instance of the <see cref="TimeIntervalSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="sequencer">The sequencer that supplies timestamps.</param>
    public TimeIntervalSignal(IObservable<T> source, ISequencer sequencer)
    {
        Source = source;
        Sequencer = sequencer;
    }

    /// <summary>Gets the source observable.</summary>
    private IObservable<T> Source { get; }

    /// <summary>Gets the sequencer that supplies timestamps.</summary>
    private ISequencer Sequencer { get; }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TimeInterval<T>> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        TimeIntervalWitness<T> sink = new(observer, Sequencer);
        sink.SetSubscription(Source.Subscribe(sink));
        return sink;
    }
}
