// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Cold signal that splits a source into windows of a fixed duration.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("SliceTimeSignal: TimeSpan = {_timeSpan}, TimeShift = {_timeShift}, Source = {_source}")]
public sealed class SliceTimeSignal<T> : IObservable<IObservable<T>>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The duration of each window.</summary>
    private readonly TimeSpan _timeSpan;

    /// <summary>The time between the starts of consecutive windows.</summary>
    private readonly TimeSpan _timeShift;

    /// <summary>The sequencer that schedules the timer.</summary>
    private readonly ISequencer _sequencer;

    /// <summary>Initializes a new instance of the <see cref="SliceTimeSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="timeSpan">The duration of each window.</param>
    /// <param name="timeShift">The time between the starts of consecutive windows.</param>
    /// <param name="sequencer">The sequencer that schedules the timer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="sequencer"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeSpan"/> or <paramref name="timeShift"/> is zero or negative.</exception>
    public SliceTimeSignal(IObservable<T> source, TimeSpan timeSpan, TimeSpan timeShift, ISequencer sequencer)
    {
        SliceTimeGuard.ThrowIfNotPositive(timeSpan);
        SliceTimeGuard.ThrowIfNotPositive(timeShift);
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _timeSpan = timeSpan;
        _timeShift = timeShift;
        _sequencer = sequencer ?? throw new ArgumentNullException(nameof(sequencer));
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<IObservable<T>> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SliceTimeWitness<T> sink = new(observer, _timeSpan, _timeShift, _sequencer);
        sink.Start();
        sink.SetSubscription(_source.Subscribe(sink));
        return sink.Subscription;
    }
}
