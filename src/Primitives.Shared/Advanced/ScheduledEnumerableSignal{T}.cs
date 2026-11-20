// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Emits the values of an enumerable on the supplied sequencer, then completes.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ScheduledEnumerableSignal: Values = {Values}, Scheduler = {Scheduler}")]
public sealed class ScheduledEnumerableSignal<T> : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="ScheduledEnumerableSignal{T}"/> class.</summary>
    /// <param name="values">The values to emit.</param>
    /// <param name="scheduler">The sequencer that enumerates and emits the values.</param>
    public ScheduledEnumerableSignal(IEnumerable<T> values, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(values);

        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        Values = values;
        Scheduler = scheduler;
    }

    /// <summary>Gets the values to emit.</summary>
    private IEnumerable<T> Values { get; }

    /// <summary>Gets the sequencer that enumerates and emits the values.</summary>
    private ISequencer Scheduler { get; }

    /// <summary>
    /// Creates an enumerable signal scheduled on <paramref name="scheduler"/>, using <see cref="FromEnumerableSignal{T}"/>
    /// when <paramref name="scheduler"/> is immediate or current-thread.
    /// </summary>
    /// <param name="values">The values to emit.</param>
    /// <param name="scheduler">The sequencer that enumerates and emits the values.</param>
    /// <returns>The observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="values"/> or <paramref name="scheduler"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IEnumerable<T> values, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(values);
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        return scheduler.IsImmediate || scheduler == Sequencer.CurrentThread
            ? new FromEnumerableSignal<T>(values)
            : new ScheduledEnumerableSignal<T>(values, scheduler);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        CancellationDisposable cancel = new();
        var scheduled = Scheduler.Schedule(
            (Self: this, observer, cancel),
            static (_, s) =>
            {
                s.Self.Emit(s.observer, s.cancel);
                return EmptyDisposable.Instance;
            });
        return new MultipleDisposable(cancel, scheduled);
    }

    /// <summary>Emits enumerable values while honoring subscription cancellation.</summary>
    /// <param name="observer">The destination observer.</param>
    /// <param name="cancel">The subscription cancellation.</param>
    private void Emit(IObserver<T> observer, CancellationDisposable cancel)
    {
        foreach (var value in Values)
        {
            if (cancel.IsDisposed)
            {
                return;
            }

            observer.OnNext(value);
        }

        if (cancel.IsDisposed)
        {
            return;
        }

        observer.OnCompleted();
    }
}
