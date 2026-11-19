// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Projects a range's values into timestamped moments.</summary>
/// <typeparam name="T">The range value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("TimestampRangeSignal: Range = {_range}")]
public sealed class TimestampRangeSignal<T> : IInlineSignal<Moment<T>>
{
    /// <summary>The range source.</summary>
    private readonly RangeSignal _range;

    /// <summary>The sequencer used to read timestamps.</summary>
    private readonly ISequencer _sequencer;

    /// <summary>Initializes a new instance of the <see cref="TimestampRangeSignal{T}"/> class.</summary>
    /// <param name="range">The range source.</param>
    /// <param name="sequencer">The sequencer used to read timestamps.</param>
    /// <exception cref="ArgumentNullException"><paramref name="range"/> or <paramref name="sequencer"/> is <see langword="null"/>.</exception>
    public TimestampRangeSignal(RangeSignal range, ISequencer sequencer)
    {
        ArgumentExceptionHelper.ThrowIfNull(range);
        ArgumentExceptionHelper.ThrowIfNull(sequencer);

        _range = range;
        _sequencer = sequencer;
    }

    /// <summary>Creates a timestamped sequence using <see cref="Sequencer.Immediate"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <returns>A sequence containing each value paired with its timestamp.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<Moment<T>> Create(IObservable<T> source) => Create(source, Sequencer.Immediate);

    /// <summary>Creates a timestamped sequence using <paramref name="scheduler"/>, defaulting to <see cref="Sequencer.Immediate"/> when <see langword="null"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="scheduler">The sequencer that supplies timestamps, or <see langword="null"/> for immediate.</param>
    /// <returns>A sequence containing each value paired with its timestamp.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<Moment<T>> Create(IObservable<T> source, ISequencer? scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        scheduler ??= Sequencer.Immediate;
        return source is RangeSignal range && typeof(T) == typeof(int)
            ? new TimestampRangeSignal<T>(range, scheduler)
            : new MapWithSignal<T, ISequencer, Moment<T>>(source, scheduler, CreateMoment);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<Moment<T>> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        Emit(observer);
        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(Action<Moment<T>> onNext, Action<Exception> onError, Action onCompleted)
    {
        ArgumentExceptionHelper.ThrowIfNull(onNext);

        Emit(onNext);
        onCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Stamps <paramref name="value"/> with <paramref name="scheduler"/>'s current time.</summary>
    /// <param name="scheduler">The sequencer supplying timestamps.</param>
    /// <param name="value">The value to stamp.</param>
    /// <returns>The stamped moment.</returns>
    private static Moment<T> CreateMoment(ISequencer scheduler, T value) => new(value, scheduler.Now);

    /// <summary>Emits timestamped range values.</summary>
    /// <param name="onNext">The next callback.</param>
    private void Emit(Action<Moment<T>> onNext)
    {
        if (_sequencer.IsImmediate)
        {
            var timestamp = _sequencer.Now;
            for (var i = 0; i < _range.Count; i++)
            {
                var value = _range.Start + i;
                onNext(new(Unsafe.As<int, T>(ref value), timestamp));
            }

            return;
        }

        for (var i = 0; i < _range.Count; i++)
        {
            var value = _range.Start + i;
            onNext(new(Unsafe.As<int, T>(ref value), _sequencer.Now));
        }
    }

    /// <summary>Emits timestamped range values straight to an observer.</summary>
    /// <param name="observer">The downstream observer.</param>
    private void Emit(IObserver<Moment<T>> observer)
    {
        if (_sequencer.IsImmediate)
        {
            var timestamp = _sequencer.Now;
            for (var i = 0; i < _range.Count; i++)
            {
                var value = _range.Start + i;
                observer.OnNext(new(Unsafe.As<int, T>(ref value), timestamp));
            }

            return;
        }

        for (var i = 0; i < _range.Count; i++)
        {
            var value = _range.Start + i;
            observer.OnNext(new(Unsafe.As<int, T>(ref value), _sequencer.Now));
        }
    }
}
