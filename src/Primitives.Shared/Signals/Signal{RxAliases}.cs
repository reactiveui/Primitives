// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>System.Reactive factory aliases for the Primitives signal factory vocabulary.</summary>
public static partial class Signal
{
    /// <summary>Returns an observable sequence that contains a single value.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="value">The value to emit.</param>
    /// <returns>An observable sequence that emits <paramref name="value"/> and completes.</returns>
    public static IObservable<T> Return<T>(T value) => Emit(value);

    /// <summary>Returns an observable sequence that contains a single scheduled value.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="value">The value to emit.</param>
    /// <param name="scheduler">The scheduler used to emit the value.</param>
    /// <returns>An observable sequence that emits <paramref name="value"/> and completes.</returns>
    public static IObservable<T> Return<T>(T value, ISequencer scheduler) => Emit(value, scheduler);

    /// <summary>Returns an empty observable sequence.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <returns>An observable sequence that completes without values.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
    public static IObservable<T> Empty<T>() => None<T>();

    /// <summary>Returns an empty observable sequence on the supplied scheduler.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="scheduler">The scheduler used to complete the sequence.</param>
    /// <returns>An observable sequence that completes without values.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
    public static IObservable<T> Empty<T>(ISequencer scheduler) => None<T>(scheduler);

    /// <summary>Returns a non-terminating observable sequence.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <returns>An observable sequence that never emits and never terminates.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
    public static IObservable<T> Never<T>() => Silent<T>();

    /// <summary>Returns an observable sequence that terminates with an error.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="error">The error used to terminate the sequence.</param>
    /// <returns>An observable sequence that terminates with <paramref name="error"/>.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
    public static IObservable<T> Throw<T>(Exception error) => Fail<T>(error);

    /// <summary>Returns an observable sequence that terminates with a scheduled error.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="error">The error used to terminate the sequence.</param>
    /// <param name="scheduler">The scheduler used to emit the error.</param>
    /// <returns>An observable sequence that terminates with <paramref name="error"/>.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
    public static IObservable<T> Throw<T>(Exception error, ISequencer scheduler) => Fail<T>(error, scheduler);

    /// <summary>Returns an observable sequence that emits a single tick after the due time.</summary>
    /// <param name="dueTime">The relative time after which to emit the tick.</param>
    /// <returns>An observable sequence that emits one tick and completes.</returns>
    public static IObservable<long> Timer(TimeSpan dueTime) => After(dueTime);

    /// <summary>Returns an observable sequence that emits a single tick after the due time on a scheduler.</summary>
    /// <param name="dueTime">The relative time after which to emit the tick.</param>
    /// <param name="scheduler">The scheduler used to emit the tick.</param>
    /// <returns>An observable sequence that emits one tick and completes.</returns>
    public static IObservable<long> Timer(TimeSpan dueTime, ISequencer scheduler) => After(dueTime, scheduler);

    /// <summary>Returns an observable sequence that emits a single tick at an absolute due time.</summary>
    /// <param name="dueTime">The absolute time at which to emit the tick.</param>
    /// <returns>An observable sequence that emits one tick and completes.</returns>
    public static IObservable<long> Timer(DateTimeOffset dueTime) => After(dueTime);

    /// <summary>Returns an observable sequence that emits a single tick at an absolute due time on a scheduler.</summary>
    /// <param name="dueTime">The absolute time at which to emit the tick.</param>
    /// <param name="scheduler">The scheduler used to emit the tick.</param>
    /// <returns>An observable sequence that emits one tick and completes.</returns>
    public static IObservable<long> Timer(DateTimeOffset dueTime, ISequencer scheduler) => After(dueTime, scheduler);

    /// <summary>Returns an observable sequence that emits ticks periodically after an initial due time.</summary>
    /// <param name="dueTime">The relative time before the first tick.</param>
    /// <param name="period">The period between subsequent ticks.</param>
    /// <returns>An observable sequence that emits periodic ticks.</returns>
    public static IObservable<long> Timer(TimeSpan dueTime, TimeSpan period) => After(dueTime, period);

    /// <summary>Returns an observable sequence that emits scheduled ticks periodically after an initial due time.</summary>
    /// <param name="dueTime">The relative time before the first tick.</param>
    /// <param name="period">The period between subsequent ticks.</param>
    /// <param name="scheduler">The scheduler used to emit ticks.</param>
    /// <returns>An observable sequence that emits periodic ticks.</returns>
    public static IObservable<long> Timer(TimeSpan dueTime, TimeSpan period, ISequencer scheduler) => After(dueTime, period, scheduler);

    /// <summary>Merges the supplied observable sources.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">The sources to merge.</param>
    /// <returns>An observable sequence that forwards values from every source.</returns>
    public static IObservable<T> Merge<T>(params IObservable<T>[] sources) => Blend(sources);

    /// <summary>Merges the supplied observable sources.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">The sources to merge.</param>
    /// <returns>An observable sequence that forwards values from every source.</returns>
    public static IObservable<T> Merge<T>(IEnumerable<IObservable<T>> sources) => sources.Blend();

    /// <summary>Switches to the most recent inner observable sequence.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">The outer sequence of inner sources.</param>
    /// <returns>An observable sequence that mirrors the latest inner source.</returns>
    public static IObservable<T> Switch<T>(IObservable<IObservable<T>> sources) => sources.SwitchTo();
}
