// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Advanced;
#else
using ReactiveUI.Primitives.Advanced;
#endif

#if PRIMITIVES_CORE
namespace ReactiveUI.Primitives.Core.Internal;
#elif REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Internal;
#else
namespace ReactiveUI.Primitives.Internal;
#endif

/// <summary>Pushes <see cref="RangeSignal"/> and indexable <see cref="FromEnumerableSignal{T}"/> sources directly into observers or callbacks without allocating stage witnesses.</summary>
internal static class DirectSequenceEmission
{
    /// <summary>Checks whether <paramref name="source"/> requires subscription on the current thread.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <returns><see langword="true"/> when <paramref name="source"/> implements <see cref="IRequireCurrentThread{T}"/> and requires current-thread subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool RequiresCurrentThread<TSource>(IObservable<TSource> source) =>
        source is IRequireCurrentThread<TSource> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <summary>Creates a validated <see cref="DirectObserverCallbacks{T}"/> instance.</summary>
    /// <typeparam name="T">The emitted element type.</typeparam>
    /// <param name="onNext">The value callback.</param>
    /// <param name="onError">The error callback.</param>
    /// <param name="onCompleted">The completion callback.</param>
    /// <returns>The validated callback record.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="onNext"/>, <paramref name="onError"/>, or <paramref name="onCompleted"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static DirectObserverCallbacks<T> ValidateCallbacks<T>(
        Action<T> onNext,
        Action<Exception> onError,
        Action onCompleted)
    {
        ArgumentExceptionHelper.ThrowIfNull(onNext);
        ArgumentExceptionHelper.ThrowIfNull(onError);
        ArgumentExceptionHelper.ThrowIfNull(onCompleted);
        return new(onNext, onError, onCompleted);
    }

    /// <summary>Emits filtered values directly when <paramref name="source"/> is a synchronous range or indexable enumerable.</summary>
    /// <typeparam name="TSource">The element type.</typeparam>
    /// <typeparam name="TContext">The caller context type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="context">The caller context.</param>
    /// <param name="acceptInt">Evaluates an integer range value.</param>
    /// <param name="acceptItem">Evaluates a list element.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <returns><see langword="true"/> when <paramref name="source"/> was emitted directly; otherwise, <see langword="false"/>.</returns>
    internal static bool TrySubscribeFiltered<TSource, TContext>(
        IObservable<TSource> source,
        TContext context,
        Func<int, TContext, bool> acceptInt,
        Func<TSource, TContext, bool> acceptItem,
        IObserver<TSource> observer)
    {
        if (typeof(TSource) == typeof(int) && source is RangeSignal range)
        {
            EmitRangeFiltered(range, context, acceptInt, observer);
            return true;
        }

        if (source is FromEnumerableSignal<TSource> enumerable && enumerable.TryGetReadOnlyValues(out var values))
        {
            EmitListFiltered(values, context, acceptItem, observer);
            return true;
        }

        return false;
    }

    /// <summary>Emits projected values directly when <paramref name="source"/> is a synchronous range or indexable enumerable.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TContext">The caller context type.</typeparam>
    /// <typeparam name="TResult">The projected element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="context">The caller context.</param>
    /// <param name="projectInt">Projects an integer range value.</param>
    /// <param name="projectItem">Projects a list element.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <returns><see langword="true"/> when <paramref name="source"/> was emitted directly; otherwise, <see langword="false"/>.</returns>
    internal static bool TrySubscribeProjected<TSource, TContext, TResult>(
        IObservable<TSource> source,
        TContext context,
        Func<int, TContext, TResult> projectInt,
        Func<TSource, TContext, TResult> projectItem,
        IObserver<TResult> observer)
    {
        if (typeof(TSource) == typeof(int) && source is RangeSignal range)
        {
            EmitRangeProjected(range, context, projectInt, observer);
            return true;
        }

        if (source is FromEnumerableSignal<TSource> enumerable && enumerable.TryGetReadOnlyValues(out var values))
        {
            EmitListProjected(values, context, projectItem, observer);
            return true;
        }

        return false;
    }

    /// <summary>Invokes a stateful side-effect callback and forwards values directly when <paramref name="source"/> is a synchronous range or indexable enumerable.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TState">The state type passed to the side-effect callback.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="tap">The state and side-effect action pair.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <returns><see langword="true"/> when <paramref name="source"/> was emitted directly; otherwise, <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool TrySubscribeTapped<TSource, TState>(
        IObservable<TSource> source,
        StateCallbackPair<TState, Action<TState, TSource>> tap,
        IObserver<TSource> observer) =>
        TrySubscribeTapped(source, tap, null, observer);

    /// <summary>Invokes side-effect callbacks and forwards values directly when <paramref name="source"/> is a synchronous range or indexable enumerable.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TState">The state type passed to the side-effect callback.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="tap">The state and side-effect action pair.</param>
    /// <param name="onCompleted">Optional completion side-effect callback.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <returns><see langword="true"/> when <paramref name="source"/> was emitted directly; otherwise, <see langword="false"/>.</returns>
    internal static bool TrySubscribeTapped<TSource, TState>(
        IObservable<TSource> source,
        StateCallbackPair<TState, Action<TState, TSource>> tap,
        Action? onCompleted,
        IObserver<TSource> observer)
    {
        if (typeof(TSource) == typeof(int) && source is RangeSignal range)
        {
            EmitRangeTapped(range, tap, onCompleted, observer);
            return true;
        }

        if (source is FromEnumerableSignal<TSource> enumerable && enumerable.TryGetReadOnlyValues(out var values))
        {
            EmitListTapped(values, tap, onCompleted, observer);
            return true;
        }

        return false;
    }

    /// <summary>Emits fused projected-and-filtered values directly when <paramref name="source"/> is a synchronous range or indexable enumerable.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TContext">The caller context type.</typeparam>
    /// <typeparam name="TResult">The projected element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="context">The caller context.</param>
    /// <param name="evaluateInt">Evaluates an integer range value.</param>
    /// <param name="evaluateItem">Evaluates a list element.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <returns><see langword="true"/> when <paramref name="source"/> was emitted directly; otherwise, <see langword="false"/>.</returns>
    internal static bool TrySubscribeFilteredProjection<TSource, TContext, TResult>(
        IObservable<TSource> source,
        TContext context,
        Func<int, TContext, ProjectedOutcome<TResult>> evaluateInt,
        Func<TSource, TContext, ProjectedOutcome<TResult>> evaluateItem,
        IObserver<TResult> observer)
    {
        if (typeof(TSource) == typeof(int) && source is RangeSignal range)
        {
            EmitRangeFilteredProjection(range, context, evaluateInt, observer);
            return true;
        }

        if (source is FromEnumerableSignal<TSource> enumerable && enumerable.TryGetReadOnlyValues(out var values))
        {
            EmitListFilteredProjection(values, context, evaluateItem, observer);
            return true;
        }

        return false;
    }

    /// <summary>Emits filtered integer range values directly into <paramref name="callbacks"/> when <paramref name="source"/> is a <see cref="RangeSignal"/>.</summary>
    /// <typeparam name="TSource">The element type.</typeparam>
    /// <typeparam name="TContext">The caller context type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="context">The caller context.</param>
    /// <param name="acceptInt">Evaluates an integer range value.</param>
    /// <param name="callbacks">The downstream callbacks.</param>
    /// <returns><see langword="true"/> when <paramref name="source"/> was emitted directly; otherwise, <see langword="false"/>.</returns>
    internal static bool TrySubscribeRangeFilteredCallbacks<TSource, TContext>(
        IObservable<TSource> source,
        TContext context,
        Func<int, TContext, bool> acceptInt,
        DirectObserverCallbacks<TSource> callbacks)
    {
        if (typeof(TSource) != typeof(int) || source is not RangeSignal range)
        {
            return false;
        }

        var intOnNext = Unsafe.As<Action<int>>(callbacks.OnNext);
        var start = range.Start;
        var count = range.Count;
        for (var i = 0; i < count; i++)
        {
            var value = start + i;
            bool keep;
            try
            {
                keep = acceptInt(value, context);
            }
            catch (Exception error) when (!UnaryWitnessDelivery.IsFatal(error))
            {
                callbacks.OnError(error);
                return true;
            }

            if (keep)
            {
                intOnNext(value);
            }
        }

        callbacks.OnCompleted();
        return true;
    }

    /// <summary>
    /// Emits projected values directly into <paramref name="callbacks"/> when <paramref name="source"/> is a synchronous range
    /// or (when <paramref name="projectItem"/> is supplied) an indexable enumerable.
    /// </summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TContext">The caller context type.</typeparam>
    /// <typeparam name="TResult">The projected element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="context">The caller context.</param>
    /// <param name="projectInt">Projects an integer range value.</param>
    /// <param name="projectItem">Optional list-element projection; when <see langword="null"/>, only <see cref="RangeSignal"/> is handled directly.</param>
    /// <param name="callbacks">The downstream callbacks.</param>
    /// <returns><see langword="true"/> when <paramref name="source"/> was emitted directly; otherwise, <see langword="false"/>.</returns>
    internal static bool TrySubscribeProjectedCallbacks<TSource, TContext, TResult>(
        IObservable<TSource> source,
        TContext context,
        Func<int, TContext, TResult> projectInt,
        Func<TSource, TContext, TResult>? projectItem,
        DirectObserverCallbacks<TResult> callbacks)
    {
        if (typeof(TSource) == typeof(int) && source is RangeSignal range)
        {
            EmitRangeProjectedCallbacks(range, context, projectInt, callbacks);
            return true;
        }

        if (projectItem is not null
            && source is FromEnumerableSignal<TSource> enumerable
            && enumerable.TryGetReadOnlyValues(out var values))
        {
            EmitListProjectedCallbacks(values, context, projectItem, callbacks);
            return true;
        }

        return false;
    }

    /// <summary>Emits fused projected-and-filtered integer range values directly into <paramref name="callbacks"/> when <paramref name="source"/> is a <see cref="RangeSignal"/>.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TContext">The caller context type.</typeparam>
    /// <typeparam name="TResult">The projected element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="context">The caller context.</param>
    /// <param name="evaluateInt">Evaluates an integer range value.</param>
    /// <param name="callbacks">The downstream callbacks.</param>
    /// <returns><see langword="true"/> when <paramref name="source"/> was emitted directly; otherwise, <see langword="false"/>.</returns>
    internal static bool TrySubscribeRangeFilteredProjectionCallbacks<TSource, TContext, TResult>(
        IObservable<TSource> source,
        TContext context,
        Func<int, TContext, ProjectedOutcome<TResult>> evaluateInt,
        DirectObserverCallbacks<TResult> callbacks)
    {
        if (typeof(TSource) != typeof(int) || source is not RangeSignal range)
        {
            return false;
        }

        var start = range.Start;
        var count = range.Count;
        for (var i = 0; i < count; i++)
        {
            ProjectedOutcome<TResult> outcome;
            try
            {
                outcome = evaluateInt(start + i, context);
            }
            catch (Exception error) when (!UnaryWitnessDelivery.IsFatal(error))
            {
                callbacks.OnError(error);
                return true;
            }

            if (outcome.HasValue)
            {
                callbacks.OnNext(outcome.Value);
            }
        }

        callbacks.OnCompleted();
        return true;
    }

    /// <summary>Accumulates <paramref name="source"/> directly when it is a synchronous range or indexable enumerable.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TAccumulate">The accumulator type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="seed">The initial accumulator value.</param>
    /// <param name="accumulator">The accumulation function.</param>
    /// <param name="emitEachStep"><see langword="true"/> to emit after each step (<c>Fold</c>); <see langword="false"/> to emit only the final value (<c>Reduce</c>).</param>
    /// <param name="observer">The downstream observer.</param>
    /// <returns><see langword="true"/> when <paramref name="source"/> was accumulated directly; otherwise, <see langword="false"/>.</returns>
    internal static bool TrySubscribeAccumulated<TSource, TAccumulate>(
        IObservable<TSource> source,
        TAccumulate seed,
        Func<TAccumulate, TSource, TAccumulate> accumulator,
        bool emitEachStep,
        IObserver<TAccumulate> observer)
    {
        if (typeof(TSource) == typeof(int) && source is RangeSignal range)
        {
            EmitRangeAccumulated(range, seed, accumulator, emitEachStep, observer);
            return true;
        }

        if (source is FromEnumerableSignal<TSource> enumerable && enumerable.TryGetReadOnlyValues(out var values))
        {
            EmitListAccumulated(values, seed, accumulator, emitEachStep, observer);
            return true;
        }

        return false;
    }

    /// <summary>Emits filtered integer range elements to <paramref name="observer"/>.</summary>
    /// <typeparam name="TSource">The source element type (verified to be <see cref="int"/>).</typeparam>
    /// <typeparam name="TContext">The caller context type.</typeparam>
    /// <param name="range">The integer range source.</param>
    /// <param name="context">The caller context.</param>
    /// <param name="acceptInt">Evaluates each integer element.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitRangeFiltered<TSource, TContext>(
        RangeSignal range,
        TContext context,
        Func<int, TContext, bool> acceptInt,
        IObserver<TSource> observer)
    {
        var intObserver = Unsafe.As<IObserver<int>>(observer);
        var start = range.Start;
        var count = range.Count;
        for (var i = 0; i < count; i++)
        {
            var value = start + i;
            bool keep;
            try
            {
                keep = acceptInt(value, context);
            }
            catch (Exception error) when (!UnaryWitnessDelivery.IsFatal(error))
            {
                observer.OnError(error);
                return;
            }

            if (keep)
            {
                intObserver.OnNext(value);
            }
        }

        observer.OnCompleted();
    }

    /// <summary>Emits filtered list elements to <paramref name="observer"/>.</summary>
    /// <typeparam name="TSource">The element type.</typeparam>
    /// <typeparam name="TContext">The caller context type.</typeparam>
    /// <param name="values">The indexable values.</param>
    /// <param name="context">The caller context.</param>
    /// <param name="acceptItem">Evaluates each list element.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitListFiltered<TSource, TContext>(
        IReadOnlyList<TSource> values,
        TContext context,
        Func<TSource, TContext, bool> acceptItem,
        IObserver<TSource> observer)
    {
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            bool keep;
            try
            {
                keep = acceptItem(value, context);
            }
            catch (Exception error) when (!UnaryWitnessDelivery.IsFatal(error))
            {
                observer.OnError(error);
                return;
            }

            if (keep)
            {
                observer.OnNext(value);
            }
        }

        observer.OnCompleted();
    }

    /// <summary>Emits projected integer range elements to <paramref name="observer"/>.</summary>
    /// <typeparam name="TContext">The caller context type.</typeparam>
    /// <typeparam name="TResult">The projected element type.</typeparam>
    /// <param name="range">The integer range source.</param>
    /// <param name="context">The caller context.</param>
    /// <param name="projectInt">Projects each integer element.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitRangeProjected<TContext, TResult>(
        RangeSignal range,
        TContext context,
        Func<int, TContext, TResult> projectInt,
        IObserver<TResult> observer)
    {
        var start = range.Start;
        var count = range.Count;
        for (var i = 0; i < count; i++)
        {
            TResult projected;
            try
            {
                projected = projectInt(start + i, context);
            }
            catch (Exception error) when (!UnaryWitnessDelivery.IsFatal(error))
            {
                observer.OnError(error);
                return;
            }

            observer.OnNext(projected);
        }

        observer.OnCompleted();
    }

    /// <summary>Emits projected list elements to <paramref name="observer"/>.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TContext">The caller context type.</typeparam>
    /// <typeparam name="TResult">The projected element type.</typeparam>
    /// <param name="values">The indexable values.</param>
    /// <param name="context">The caller context.</param>
    /// <param name="projectItem">Projects each list element.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitListProjected<TSource, TContext, TResult>(
        IReadOnlyList<TSource> values,
        TContext context,
        Func<TSource, TContext, TResult> projectItem,
        IObserver<TResult> observer)
    {
        for (var i = 0; i < values.Count; i++)
        {
            TResult projected;
            var element = values[i];
            try
            {
                projected = projectItem(element, context);
            }
            catch (Exception error) when (!UnaryWitnessDelivery.IsFatal(error))
            {
                observer.OnError(error);
                return;
            }

            observer.OnNext(projected);
        }

        observer.OnCompleted();
    }

    /// <summary>Invokes side effects on each range value, forwards it to <paramref name="observer"/>, and completes.</summary>
    /// <typeparam name="TSource">The source element type (verified to be <see cref="int"/>).</typeparam>
    /// <typeparam name="TState">The state type.</typeparam>
    /// <param name="range">The range source.</param>
    /// <param name="tap">The state and side-effect callback pair.</param>
    /// <param name="onCompleted">Optional completion side-effect callback.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitRangeTapped<TSource, TState>(
        RangeSignal range,
        StateCallbackPair<TState, Action<TState, TSource>> tap,
        Action? onCompleted,
        IObserver<TSource> observer)
    {
        var intCallback = Unsafe.As<Action<TState, int>>(tap.Callback);
        var intObserver = Unsafe.As<IObserver<int>>(observer);
        var state = tap.State;
        var start = range.Start;
        var count = range.Count;
        for (var i = 0; i < count; i++)
        {
            var value = start + i;
            try
            {
                intCallback(state, value);
            }
            catch (Exception error) when (!UnaryWitnessDelivery.IsFatal(error))
            {
                observer.OnError(error);
                return;
            }

            intObserver.OnNext(value);
        }

        CompleteTapped(onCompleted, observer);
    }

    /// <summary>Invokes side effects on each list value, forwards it to <paramref name="observer"/>, and completes.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TState">The state type.</typeparam>
    /// <param name="values">The indexable values.</param>
    /// <param name="tap">The state and side-effect callback pair.</param>
    /// <param name="onCompleted">Optional completion side-effect callback.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitListTapped<TSource, TState>(
        IReadOnlyList<TSource> values,
        StateCallbackPair<TState, Action<TState, TSource>> tap,
        Action? onCompleted,
        IObserver<TSource> observer)
    {
        var (state, callback) = tap;
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            try
            {
                callback(state, value);
            }
            catch (Exception error) when (!UnaryWitnessDelivery.IsFatal(error))
            {
                observer.OnError(error);
                return;
            }

            observer.OnNext(value);
        }

        CompleteTapped(onCompleted, observer);
    }

    /// <summary>Runs the optional completion side effect before completing or faulting <paramref name="observer"/>.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <param name="onCompleted">Optional completion side-effect callback.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void CompleteTapped<TSource>(Action? onCompleted, IObserver<TSource> observer)
    {
        if (onCompleted is not null)
        {
            try
            {
                onCompleted();
            }
            catch (Exception error) when (!UnaryWitnessDelivery.IsFatal(error))
            {
                observer.OnError(error);
                return;
            }
        }

        observer.OnCompleted();
    }

    /// <summary>Emits fused projected-and-filtered integer range elements to <paramref name="observer"/>.</summary>
    /// <typeparam name="TContext">The caller context type.</typeparam>
    /// <typeparam name="TResult">The projected element type.</typeparam>
    /// <param name="range">The integer range source.</param>
    /// <param name="context">The caller context.</param>
    /// <param name="evaluateInt">Evaluates each integer element.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitRangeFilteredProjection<TContext, TResult>(
        RangeSignal range,
        TContext context,
        Func<int, TContext, ProjectedOutcome<TResult>> evaluateInt,
        IObserver<TResult> observer)
    {
        var start = range.Start;
        var count = range.Count;
        for (var i = 0; i < count; i++)
        {
            ProjectedOutcome<TResult> outcome;
            try
            {
                outcome = evaluateInt(start + i, context);
            }
            catch (Exception error) when (!UnaryWitnessDelivery.IsFatal(error))
            {
                observer.OnError(error);
                return;
            }

            if (outcome.HasValue)
            {
                observer.OnNext(outcome.Value);
            }
        }

        observer.OnCompleted();
    }

    /// <summary>Emits fused projected-and-filtered list elements to <paramref name="observer"/>.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TContext">The caller context type.</typeparam>
    /// <typeparam name="TResult">The projected element type.</typeparam>
    /// <param name="values">The indexable values.</param>
    /// <param name="context">The caller context.</param>
    /// <param name="evaluateItem">Evaluates each list element.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitListFilteredProjection<TSource, TContext, TResult>(
        IReadOnlyList<TSource> values,
        TContext context,
        Func<TSource, TContext, ProjectedOutcome<TResult>> evaluateItem,
        IObserver<TResult> observer)
    {
        for (var i = 0; i < values.Count; i++)
        {
            ProjectedOutcome<TResult> outcome;
            var element = values[i];
            try
            {
                outcome = evaluateItem(element, context);
            }
            catch (Exception error) when (!UnaryWitnessDelivery.IsFatal(error))
            {
                observer.OnError(error);
                return;
            }

            if (outcome.HasValue)
            {
                observer.OnNext(outcome.Value);
            }
        }

        observer.OnCompleted();
    }

    /// <summary>Emits projected integer range elements to <paramref name="callbacks"/>.</summary>
    /// <typeparam name="TContext">The caller context type.</typeparam>
    /// <typeparam name="TResult">The projected element type.</typeparam>
    /// <param name="range">The integer range source.</param>
    /// <param name="context">The caller context.</param>
    /// <param name="projectInt">Projects each integer element.</param>
    /// <param name="callbacks">The downstream callbacks.</param>
    private static void EmitRangeProjectedCallbacks<TContext, TResult>(
        RangeSignal range,
        TContext context,
        Func<int, TContext, TResult> projectInt,
        DirectObserverCallbacks<TResult> callbacks)
    {
        var start = range.Start;
        var count = range.Count;
        for (var i = 0; i < count; i++)
        {
            TResult projected;
            try
            {
                projected = projectInt(start + i, context);
            }
            catch (Exception error) when (!UnaryWitnessDelivery.IsFatal(error))
            {
                callbacks.OnError(error);
                return;
            }

            callbacks.OnNext(projected);
        }

        callbacks.OnCompleted();
    }

    /// <summary>Emits projected list elements to <paramref name="callbacks"/>.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TContext">The caller context type.</typeparam>
    /// <typeparam name="TResult">The projected element type.</typeparam>
    /// <param name="values">The indexable values.</param>
    /// <param name="context">The caller context.</param>
    /// <param name="projectItem">Projects each list element.</param>
    /// <param name="callbacks">The downstream callbacks.</param>
    private static void EmitListProjectedCallbacks<TSource, TContext, TResult>(
        IReadOnlyList<TSource> values,
        TContext context,
        Func<TSource, TContext, TResult> projectItem,
        DirectObserverCallbacks<TResult> callbacks)
    {
        for (var i = 0; i < values.Count; i++)
        {
            TResult projected;
            var element = values[i];
            try
            {
                projected = projectItem(element, context);
            }
            catch (Exception error) when (!UnaryWitnessDelivery.IsFatal(error))
            {
                callbacks.OnError(error);
                return;
            }

            callbacks.OnNext(projected);
        }

        callbacks.OnCompleted();
    }

    /// <summary>Accumulates integer range elements and emits running or final state to <paramref name="observer"/>.</summary>
    /// <typeparam name="TSource">The source element type (verified to be <see cref="int"/>).</typeparam>
    /// <typeparam name="TAccumulate">The accumulator type.</typeparam>
    /// <param name="range">The integer range source.</param>
    /// <param name="seed">The initial accumulator value.</param>
    /// <param name="accumulator">The accumulation function.</param>
    /// <param name="emitEachStep">Whether to emit after each step.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitRangeAccumulated<TSource, TAccumulate>(
        RangeSignal range,
        TAccumulate seed,
        Func<TAccumulate, TSource, TAccumulate> accumulator,
        bool emitEachStep,
        IObserver<TAccumulate> observer)
    {
        var intAccumulator = Unsafe.As<Func<TAccumulate, int, TAccumulate>>(accumulator);
        var current = seed;
        var start = range.Start;
        var count = range.Count;
        for (var i = 0; i < count; i++)
        {
            if (!TryStepAccumulator(intAccumulator, ref current, start + i, observer))
            {
                return;
            }

            if (emitEachStep)
            {
                observer.OnNext(current);
            }
        }

        if (!emitEachStep)
        {
            observer.OnNext(current);
        }

        observer.OnCompleted();
    }

    /// <summary>Accumulates list elements and emits running or final state to <paramref name="observer"/>.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TAccumulate">The accumulator type.</typeparam>
    /// <param name="values">The indexable values.</param>
    /// <param name="seed">The initial accumulator value.</param>
    /// <param name="accumulator">The accumulation function.</param>
    /// <param name="emitEachStep">Whether to emit after each step.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitListAccumulated<TSource, TAccumulate>(
        IReadOnlyList<TSource> values,
        TAccumulate seed,
        Func<TAccumulate, TSource, TAccumulate> accumulator,
        bool emitEachStep,
        IObserver<TAccumulate> observer)
    {
        var current = seed;
        for (var i = 0; i < values.Count; i++)
        {
            if (!TryStepAccumulator(accumulator, ref current, values[i], observer))
            {
                return;
            }

            if (emitEachStep)
            {
                observer.OnNext(current);
            }
        }

        if (!emitEachStep)
        {
            observer.OnNext(current);
        }

        observer.OnCompleted();
    }

    /// <summary>Advances <paramref name="current"/> using <paramref name="accumulator"/>, routing non-fatal callback failures to <paramref name="observer"/>.</summary>
    /// <typeparam name="TItem">The source item type.</typeparam>
    /// <typeparam name="TAccumulate">The accumulator type.</typeparam>
    /// <param name="accumulator">The accumulation function.</param>
    /// <param name="current">The running accumulator value.</param>
    /// <param name="item">The incoming element.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <returns><see langword="true"/> when accumulation succeeded; <see langword="false"/> when faulted.</returns>
    private static bool TryStepAccumulator<TItem, TAccumulate>(
        Func<TAccumulate, TItem, TAccumulate> accumulator,
        ref TAccumulate current,
        TItem item,
        IObserver<TAccumulate> observer)
    {
        try
        {
            current = accumulator(current, item);
            return true;
        }
        catch (Exception error) when (!UnaryWitnessDelivery.IsFatal(error))
        {
            observer.OnError(error);
            return false;
        }
    }
}
