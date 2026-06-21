// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Scheduled enumerable-backed signal used by observable conversion overloads.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class ScheduledEnumerableSignal<T> : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="ScheduledEnumerableSignal{T}"/> class.</summary>
    /// <param name="values">The values to emit.</param>
    /// <param name="scheduler">The scheduler used to enumerate and emit the values.</param>
    public ScheduledEnumerableSignal(IEnumerable<T> values, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(values);

        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        Values = values;
        Scheduler = scheduler;
    }

    /// <summary>Gets the values to emit.</summary>
    private IEnumerable<T> Values { get; }

    /// <summary>Gets the scheduler used to enumerate and emit the values.</summary>
    private ISequencer Scheduler { get; }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        CancellationDisposable cancel = new();
        var scheduled = Scheduler.Schedule(() => Emit(observer, cancel));
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
