// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Projects a range's values into interval-tagged values.</summary>
/// <typeparam name="T">The range value type.</typeparam>
/// <param name="range">The range source.</param>
/// <param name="sequencer">The sequencer used to read timestamps.</param>
[System.Diagnostics.DebuggerDisplay("TimeIntervalRangeSignal: Range = {_range}")]
public sealed class TimeIntervalRangeSignal<T>(RangeSignal range, ISequencer sequencer) : IInlineSignal<TimeInterval<T>>
{
    /// <summary>The range source.</summary>
    private readonly RangeSignal _range = range;

    /// <summary>The sequencer used to read timestamps.</summary>
    private readonly ISequencer _sequencer = sequencer;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TimeInterval<T>> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        Emit(observer);
        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(Action<TimeInterval<T>> onNext, Action<Exception> onError, Action onCompleted)
    {
        ArgumentExceptionHelper.ThrowIfNull(onNext);

        Emit(onNext);
        onCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Emits interval-tagged range values.</summary>
    /// <param name="onNext">The next callback.</param>
    private void Emit(Action<TimeInterval<T>> onNext)
    {
        if (_sequencer.IsImmediate)
        {
            for (var i = 0; i < _range.Count; i++)
            {
                var value = _range.Start + i;
                onNext(new(Unsafe.As<int, T>(ref value), TimeSpan.Zero));
            }

            return;
        }

        var last = _sequencer.Now;
        for (var i = 0; i < _range.Count; i++)
        {
            var now = _sequencer.Now;
            var interval = i == 0 ? TimeSpan.Zero : now - last;
            last = now;
            var value = _range.Start + i;
            onNext(new(Unsafe.As<int, T>(ref value), interval));
        }
    }

    /// <summary>Emits interval-tagged range values straight to an observer.</summary>
    /// <param name="observer">The downstream observer.</param>
    private void Emit(IObserver<TimeInterval<T>> observer)
    {
        if (_sequencer.IsImmediate)
        {
            for (var i = 0; i < _range.Count; i++)
            {
                var value = _range.Start + i;
                observer.OnNext(new(Unsafe.As<int, T>(ref value), TimeSpan.Zero));
            }

            return;
        }

        var last = _sequencer.Now;
        for (var i = 0; i < _range.Count; i++)
        {
            var now = _sequencer.Now;
            var interval = i == 0 ? TimeSpan.Zero : now - last;
            last = now;
            var value = _range.Start + i;
            observer.OnNext(new(Unsafe.As<int, T>(ref value), interval));
        }
    }
}
