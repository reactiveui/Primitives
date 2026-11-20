// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Emits a whole range as one batch scheduled after the due time.</summary>
/// <typeparam name="T">The range value type.</typeparam>
/// <param name="range">The range source.</param>
/// <param name="dueTime">The normalized due time.</param>
/// <param name="sequencer">The sequencer used to schedule the range batch.</param>
[System.Diagnostics.DebuggerDisplay("ShiftedRangeSignal: Range = {_range}")]
public sealed class ShiftedRangeSignal<T>(RangeSignal range, TimeSpan dueTime, ISequencer sequencer) : IRequireCurrentThread<T>, IInlineSignal<T>
{
    /// <summary>The range source.</summary>
    private readonly RangeSignal _range = range;

    /// <summary>The normalized due time.</summary>
    private readonly TimeSpan _dueTime = dueTime;

    /// <summary>The sequencer used to schedule the range batch.</summary>
    private readonly ISequencer _sequencer = sequencer;

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() => _sequencer == Sequencer.CurrentThread;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return _sequencer.Schedule(
            (Observer: observer, Range: _range),
            _dueTime,
            static (_, state) => EmitShiftedRange(state.Observer, state.Range));
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        ArgumentExceptionHelper.ThrowIfNull(onNext);

        ArgumentExceptionHelper.ThrowIfNull(onCompleted);

        return _sequencer.Schedule(
            (OnNext: onNext, OnCompleted: onCompleted, Range: _range),
            _dueTime,
            static (_, state) => EmitShiftedRange(state.OnNext, state.OnCompleted, state.Range));
    }

    /// <summary>Emits all range values and completion from a scheduled batch.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="range">The source range.</param>
    /// <returns>An empty disposable.</returns>
    private static EmptyDisposable EmitShiftedRange(IObserver<T> observer, RangeSignal range)
    {
        for (var i = 0; i < range.Count; i++)
        {
            observer.OnNext((T)(object)(range.Start + i));
        }

        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Emits all range values and completion from a scheduled batch.</summary>
    /// <param name="onNext">The next callback.</param>
    /// <param name="onCompleted">The completion callback.</param>
    /// <param name="range">The source range.</param>
    /// <returns>An empty disposable.</returns>
    private static EmptyDisposable EmitShiftedRange(Action<T> onNext, Action onCompleted, RangeSignal range)
    {
        for (var i = 0; i < range.Count; i++)
        {
            onNext((T)(object)(range.Start + i));
        }

        onCompleted();
        return EmptyDisposable.Instance;
    }
}
