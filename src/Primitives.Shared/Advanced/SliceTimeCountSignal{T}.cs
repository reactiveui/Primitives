// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Cold signal that splits a source into windows that end after a duration or a number of values, whichever comes first.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("SliceTimeCountSignal: TimeSpan = {_timeSpan}, Count = {_count}, Source = {_source}")]
public sealed class SliceTimeCountSignal<T> : IObservable<IObservable<T>>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The maximum duration of each window.</summary>
    private readonly TimeSpan _timeSpan;

    /// <summary>The maximum number of values in each window.</summary>
    private readonly int _count;

    /// <summary>The sequencer that schedules the timer.</summary>
    private readonly ISequencer _sequencer;

    /// <summary>Initializes a new instance of the <see cref="SliceTimeCountSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="timeSpan">The maximum duration of each window.</param>
    /// <param name="count">The maximum number of values in each window.</param>
    /// <param name="sequencer">The sequencer that schedules the timer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="sequencer"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeSpan"/> or <paramref name="count"/> is zero or negative.</exception>
    public SliceTimeCountSignal(IObservable<T> source, TimeSpan timeSpan, int count, ISequencer sequencer)
    {
        SliceTimeGuard.ThrowIfNotPositive(timeSpan);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(count);
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _timeSpan = timeSpan;
        _count = count;
        _sequencer = sequencer ?? throw new ArgumentNullException(nameof(sequencer));
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<IObservable<T>> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SliceTimeCountWitness<T> sink = new(observer, _timeSpan, _count, _sequencer);
        sink.Start();
        sink.SetSubscription(_source.Subscribe(sink));
        return sink.Subscription;
    }
}
