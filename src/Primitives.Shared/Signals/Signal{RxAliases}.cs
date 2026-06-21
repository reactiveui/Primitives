// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Advanced;

namespace ReactiveUI.Primitives.Reactive.Signals;
#else
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>System.Reactive factory aliases for the Primitives signal factory vocabulary.</summary>
public static partial class Signal
{
    /// <summary>Creates a signal whose source is produced separately for each subscription.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="observableFactory">The factory that creates the source signal for a subscription.</param>
    /// <returns>A signal that subscribes to the factory-produced source for each observer.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="observableFactory"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Defer<T>(Func<IObservable<T>> observableFactory)
    {
        ArgumentExceptionHelper.ThrowIfNull(observableFactory);

        return new DeferSignal<T>(observableFactory);
    }

    /// <summary>Returns an observable sequence that contains a single value.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="value">The value to emit.</param>
    /// <returns>An observable sequence that emits <paramref name="value"/> and completes.</returns>
    public static IObservable<T> Return<T>(T value) =>
        new ImmediateReturnSignal<T>(value);

    /// <summary>Returns an observable sequence that contains a single scheduled value.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="value">The value to emit.</param>
    /// <param name="scheduler">The scheduler used to emit the value.</param>
    /// <returns>An observable sequence that emits <paramref name="value"/> and completes.</returns>
    public static IObservable<T> Return<T>(T value, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        return scheduler == Sequencer.Immediate ? new ImmediateReturnSignal<T>(value) : new ReturnSignal<T>(value, scheduler);
    }

    /// <summary>Returns an empty observable sequence.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <returns>An observable sequence that completes without values.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
    public static IObservable<T> Empty<T>() => ImmutableEmptySignal<T>.Instance;

    /// <summary>Returns an empty observable sequence on the supplied scheduler.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="scheduler">The scheduler used to complete the sequence.</param>
    /// <returns>An observable sequence that completes without values.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
    public static IObservable<T> Empty<T>(ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        return scheduler == Sequencer.Immediate ? ImmutableEmptySignal<T>.Instance : new EmptySignal<T>(scheduler);
    }

    /// <summary>Returns a non-terminating observable sequence.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <returns>An observable sequence that never emits and never terminates.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
    public static IObservable<T> Never<T>() => ImmutableNeverSignal<T>.Instance;

    /// <summary>Returns an observable sequence that terminates with an error.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="error">The error used to terminate the sequence.</param>
    /// <returns>An observable sequence that terminates with <paramref name="error"/>.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
    public static IObservable<T> Throw<T>(Exception error) => new ImmediateThrowSignal<T>(error);

    /// <summary>Returns an observable sequence that terminates with a scheduled error.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="error">The error used to terminate the sequence.</param>
    /// <param name="scheduler">The scheduler used to emit the error.</param>
    /// <returns>An observable sequence that terminates with <paramref name="error"/>.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
    public static IObservable<T> Throw<T>(Exception error, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        return scheduler == Sequencer.Immediate ? new ImmediateThrowSignal<T>(error) : new ThrowSignal<T>(error, scheduler);
    }

    /// <summary>Returns an observable sequence that emits a range of integral values.</summary>
    /// <param name="start">The first value to emit.</param>
    /// <param name="count">The number of values to emit.</param>
    /// <returns>An observable sequence that emits the requested range and completes.</returns>
    public static IObservable<int> Range(int start, int count)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(count);

        return count == 0 ? ImmutableEmptySignal<int>.Instance : new RangeSignal(start, count);
    }

    /// <summary>Returns an observable sequence that emits a scheduled range of integral values.</summary>
    /// <param name="start">The first value to emit.</param>
    /// <param name="count">The number of values to emit.</param>
    /// <param name="scheduler">The scheduler used to emit the values.</param>
    /// <returns>An observable sequence that emits the requested range and completes.</returns>
    public static IObservable<int> Range(int start, int count, ISequencer scheduler)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(count);

        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        if (count == 0)
        {
            return ImmutableEmptySignal<int>.Instance;
        }

        return scheduler == Sequencer.Immediate || scheduler == Sequencer.CurrentThread
            ? new RangeSignal(start, count)
            : new SequenceSignal(start, count, scheduler);
    }

    /// <summary>Returns an observable sequence that emits a single tick after the due time.</summary>
    /// <param name="dueTime">The relative time after which to emit the tick.</param>
    /// <returns>An observable sequence that emits one tick and completes.</returns>
    public static IObservable<long> Timer(TimeSpan dueTime) =>
        new AfterSignal(dueTime, ThreadPoolSequencer.Instance);

    /// <summary>Returns an observable sequence that emits a single tick after the due time on a scheduler.</summary>
    /// <param name="dueTime">The relative time after which to emit the tick.</param>
    /// <param name="scheduler">The scheduler used to emit the tick.</param>
    /// <returns>An observable sequence that emits one tick and completes.</returns>
    public static IObservable<long> Timer(TimeSpan dueTime, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        return new AfterSignal(dueTime, scheduler);
    }

    /// <summary>Returns an observable sequence that emits a single tick at an absolute due time.</summary>
    /// <param name="dueTime">The absolute time at which to emit the tick.</param>
    /// <returns>An observable sequence that emits one tick and completes.</returns>
    public static IObservable<long> Timer(DateTimeOffset dueTime) =>
        new AfterSignal(Sequencer.Normalize(dueTime - ThreadPoolSequencer.Instance.Now), ThreadPoolSequencer.Instance);

    /// <summary>Returns an observable sequence that emits a single tick at an absolute due time on a scheduler.</summary>
    /// <param name="dueTime">The absolute time at which to emit the tick.</param>
    /// <param name="scheduler">The scheduler used to emit the tick.</param>
    /// <returns>An observable sequence that emits one tick and completes.</returns>
    public static IObservable<long> Timer(DateTimeOffset dueTime, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        return new AfterSignal(Sequencer.Normalize(dueTime - scheduler.Now), scheduler);
    }

    /// <summary>Returns an observable sequence that emits ticks periodically after an initial due time.</summary>
    /// <param name="dueTime">The relative time before the first tick.</param>
    /// <param name="period">The period between subsequent ticks.</param>
    /// <returns>An observable sequence that emits periodic ticks.</returns>
    public static IObservable<long> Timer(TimeSpan dueTime, TimeSpan period) =>
        new AfterSignal(dueTime, period, ThreadPoolSequencer.Instance);

    /// <summary>Returns an observable sequence that emits scheduled ticks periodically after an initial due time.</summary>
    /// <param name="dueTime">The relative time before the first tick.</param>
    /// <param name="period">The period between subsequent ticks.</param>
    /// <param name="scheduler">The scheduler used to emit ticks.</param>
    /// <returns>An observable sequence that emits periodic ticks.</returns>
    public static IObservable<long> Timer(TimeSpan dueTime, TimeSpan period, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        return new AfterSignal(dueTime, period, scheduler);
    }

    /// <summary>Returns an observable sequence that emits monotonically increasing ticks at the specified interval.</summary>
    /// <param name="period">The period between ticks.</param>
    /// <returns>An observable sequence that emits periodic ticks.</returns>
    public static IObservable<long> Interval(TimeSpan period)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(period, TimeSpan.Zero);

        return new EverySignal(period, ThreadPoolSequencer.Instance);
    }

    /// <summary>Returns an observable sequence that emits scheduled, monotonically increasing ticks at the specified interval.</summary>
    /// <param name="period">The period between ticks.</param>
    /// <param name="scheduler">The scheduler used to emit ticks.</param>
    /// <returns>An observable sequence that emits periodic ticks.</returns>
    public static IObservable<long> Interval(TimeSpan period, ISequencer scheduler)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(period, TimeSpan.Zero);

        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        return new EverySignal(period, scheduler);
    }

    /// <summary>Fails the sequence if it does not terminate before the timeout.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The timeout duration.</param>
    /// <returns>A sequence that errors with <see cref="TimeoutException"/> when the timeout elapses first.</returns>
    public static IObservable<T> Timeout<T>(IObservable<T> source, TimeSpan dueTime)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return new ExpireSignal<T>(source, dueTime, ThreadPoolSequencer.Instance);
    }

    /// <summary>Fails the sequence if it does not terminate before the sequencer timeout.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The timeout duration.</param>
    /// <param name="scheduler">The sequencer used to schedule the timeout.</param>
    /// <returns>A sequence that errors with <see cref="TimeoutException"/> when the timeout elapses first.</returns>
    public static IObservable<T> Timeout<T>(IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        scheduler ??= ThreadPoolSequencer.Instance;
        return new ExpireSignal<T>(source, dueTime, scheduler);
    }

    /// <summary>Concatenates the supplied observable sources.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">The sources to concatenate.</param>
    /// <returns>An observable sequence that subscribes to each source after the previous one completes.</returns>
    public static IObservable<T> Concat<T>(params IObservable<T>[] sources)
    {
        var validated = ValidateSources(sources);
        var rangeConcat = TryCreateRangeConcat(validated);
        return rangeConcat is null ? new ChainSignal<T>(validated) : (IObservable<T>)(object)rangeConcat;
    }

    /// <summary>Concatenates the supplied observable sources.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">The sources to concatenate.</param>
    /// <returns>An observable sequence that subscribes to each source after the previous one completes.</returns>
    public static IObservable<T> Concat<T>(IEnumerable<IObservable<T>> sources)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        return new ChainSignal<T>(sources);
    }

    /// <summary>Merges the supplied observable sources.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">The sources to merge.</param>
    /// <returns>An observable sequence that forwards values from every source.</returns>
    public static IObservable<T> Merge<T>(params IObservable<T>[] sources)
    {
        var validated = ValidateSources(sources);
        var rangeConcat = TryCreateRangeConcat(validated);
        return rangeConcat is null ? new EnumerableBlendSignal<T>(validated) : (IObservable<T>)(object)rangeConcat;
    }

    /// <summary>Merges the supplied observable sources.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">The sources to merge.</param>
    /// <returns>An observable sequence that forwards values from every source.</returns>
    public static IObservable<T> Merge<T>(IEnumerable<IObservable<T>> sources)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        return new EnumerableBlendSignal<T>(sources);
    }

    /// <summary>Switches to the most recent inner observable sequence.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">The outer sequence of inner sources.</param>
    /// <returns>An observable sequence that mirrors the latest inner source.</returns>
    public static IObservable<T> Switch<T>(IObservable<IObservable<T>> sources)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        return TryCreateSynchronousSwitchRangeSignal(sources, out var rangeSignal) ? rangeSignal : new SwitchSignal<T>(sources);
    }

    /// <summary>Combines latest values from two signals using latest-fusion semantics.</summary>
    /// <typeparam name="TLeft">The type of the left signal values.</typeparam>
    /// <typeparam name="TRight">The type of the right signal values.</typeparam>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="left">The left signal.</param>
    /// <param name="right">The right signal.</param>
    /// <param name="selector">The function that combines the latest values.</param>
    /// <returns>An observable sequence that combines the latest paired values.</returns>
    public static IObservable<TResult> PairLatest<TLeft, TRight, TResult>(
        IObservable<TLeft> left,
        IObservable<TRight> right,
        Func<TLeft, TRight, TResult> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(left);

        ArgumentExceptionHelper.ThrowIfNull(right);

        ArgumentExceptionHelper.ThrowIfNull(selector);

        return typeof(TLeft) == typeof(int) && typeof(TRight) == typeof(int) && left is RangeSignal leftRange && right is RangeSignal rightRange
            ? new RangeSyncLatestSignal<TResult>(leftRange, rightRange, (Func<int, int, TResult>)(object)selector)
            : new SyncLatestSignal<TLeft, TRight, TResult>(left, right, selector);
    }

    /// <summary>Creates a range-concat signal for synchronous Switch over known range inners.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">The outer source.</param>
    /// <param name="signal">The optimized signal when available.</param>
    /// <returns><see langword="true"/> when the fast path applies.</returns>
    private static bool TryCreateSynchronousSwitchRangeSignal<T>(IObservable<IObservable<T>> sources, out IObservable<T> signal)
    {
        signal = null!;
        if (typeof(T) != typeof(int) || sources is not FromEnumerableSignal<IObservable<T>> enumerable)
        {
            return false;
        }

        if (!enumerable.TryGetReadOnlyValues(out var innerSources) || innerSources.Count == 0)
        {
            return false;
        }

        var ranges = new RangeSignal[innerSources.Count];
        for (var i = 0; i < innerSources.Count; i++)
        {
            if (innerSources[i] is not RangeSignal range)
            {
                return false;
            }

            ranges[i] = range;
        }

        signal = (IObservable<T>)(object)new RangeConcatSignal(ranges);
        return true;
    }
}
