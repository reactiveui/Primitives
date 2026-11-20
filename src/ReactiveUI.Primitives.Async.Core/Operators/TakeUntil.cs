// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides a set of extension methods for creating observable sequences that emit items from a source sequence until a
/// specified condition is met or an external signal is received.
/// </summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Take-until operators that emit items from an observable source until a stop condition is met.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Returns an observable sequence that emits items from the source sequence until the specified other observable emits an item or completes.</summary>
        /// <typeparam name="TOther">The type of the elements in the other observable sequence that triggers termination of the source sequence.</typeparam>
        /// <param name="other">The observable sequence whose first emission or completion will cause the returned sequence to stop emitting
        /// items from the source.</param>
        /// <returns>An observable sequence that emits items from the source sequence until the other observable emits an item or
        /// completes.</returns>
        /// <exception cref="ArgumentNullException">Thrown if either the source sequence or the other observable is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> TakeUntil<TOther>(IObservableAsync<TOther> other) =>
            TakeUntilAsyncSignal<T, TOther>.Create(source, other);

        /// <summary>Returns an observable sequence that emits items from the source sequence until the specified other observable emits an item or completes.</summary>
        /// <typeparam name="TOther">The type of the elements in the other observable sequence that triggers termination of the source sequence.</typeparam>
        /// <param name="other">The observable sequence whose first emission or completion will cause the returned sequence to stop emitting
        /// items from the source.</param>
        /// <param name="options">An optional set of options that control the behavior of the take-until operation. If null, default options
        /// are used.</param>
        /// <returns>An observable sequence that emits items from the source sequence until the other observable emits an item or
        /// completes.</returns>
        /// <exception cref="ArgumentNullException">Thrown if either the source sequence or the other observable is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> TakeUntil<TOther>(IObservableAsync<TOther> other, TakeUntilOptions? options) =>
            TakeUntilAsyncSignal<T, TOther>.Create(source, other, options);

        /// <summary>Emits source items until <paramref name="other"/> or <paramref name="cancellationToken"/> fires.</summary>
        /// <typeparam name="TOther">Element type of the other sequence.</typeparam>
        /// <param name="other">The observable sequence whose first emission or completion terminates the result.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        /// <exception cref="ArgumentNullException">Thrown if either the source sequence or the other observable is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> TakeUntil<TOther>(
            IObservableAsync<TOther> other,
            CancellationToken cancellationToken) =>
            TakeUntilAsyncSignal<T, TOther>.Create(source, other, cancellationToken);

        /// <summary>Emits source items until <paramref name="other"/> or <paramref name="cancellationToken"/> fires.</summary>
        /// <typeparam name="TOther">Element type of the other sequence.</typeparam>
        /// <param name="other">The observable sequence whose first emission or completion terminates the result.</param>
        /// <param name="options">Options controlling the take-until behavior, or null for defaults.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        /// <exception cref="ArgumentNullException">Thrown if either the source sequence or the other observable is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> TakeUntil<TOther>(
            IObservableAsync<TOther> other,
            TakeUntilOptions? options,
            CancellationToken cancellationToken) =>
            TakeUntilAsyncSignal<T, TOther>.Create(source, other, options, cancellationToken);

        /// <summary>Returns an observable sequence that emits items from the source until the specified task completes.</summary>
        /// <param name="task">The task whose completion will signal the termination of the observable sequence. The sequence will stop
        /// emitting items when this task completes, regardless of its result.</param>
        /// <returns>An observable sequence that emits items from the source until the specified task completes.</returns>
        /// <exception cref="ArgumentNullException">Thrown if the source observable is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> TakeUntil(Task task) =>
            TaskStopSignal<T>.Create(source, task);

        /// <summary>Returns an observable sequence that emits items from the source until the specified task completes.</summary>
        /// <param name="task">The task whose completion will signal the termination of the observable sequence. The sequence will stop
        /// emitting items when this task completes, regardless of its result.</param>
        /// <param name="options">An optional set of options that control the behavior of the take-until operation. If null, default options
        /// are used.</param>
        /// <returns>An observable sequence that emits items from the source until the specified task completes.</returns>
        /// <exception cref="ArgumentNullException">Thrown if the source observable is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> TakeUntil(Task task, TakeUntilOptions? options) =>
            TaskStopSignal<T>.Create(source, task, options);

        /// <summary>
        /// Returns an observable sequence that emits items from the source until <paramref name="task"/> completes or
        /// <paramref name="cancellationToken"/> is cancelled - whichever comes first.
        /// </summary>
        /// <param name="task">The task whose completion terminates the result.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        /// <exception cref="ArgumentNullException">Thrown if the source observable is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> TakeUntil(Task task, CancellationToken cancellationToken) =>
            TaskStopSignal<T>.Create(source, task, cancellationToken);

        /// <summary>
        /// Returns an observable sequence that emits items from the source until <paramref name="task"/> completes or
        /// <paramref name="cancellationToken"/> is cancelled - whichever comes first.
        /// </summary>
        /// <param name="task">The task whose completion terminates the result.</param>
        /// <param name="options">Options controlling the take-until behavior, or null for defaults.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        /// <exception cref="ArgumentNullException">Thrown if the source observable is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> TakeUntil(
            Task task,
            TakeUntilOptions? options,
            CancellationToken cancellationToken) =>
            TaskStopSignal<T>.Create(source, task, options, cancellationToken);

        /// <summary>Returns an observable sequence that emits items from the source sequence until the specified cancellation token is canceled.</summary>
        /// <param name="cancellationToken">A cancellation token that, when canceled, will terminate the resulting observable sequence.</param>
        /// <returns>An observable sequence that completes when the provided cancellation token is canceled or when the source
        /// sequence completes.</returns>
        /// <remarks>A token that is canceled at the time of the call completes the sequence immediately.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> TakeUntil(CancellationToken cancellationToken) =>
            new CancellationStopSignal<T>(source, cancellationToken);

        /// <summary>Returns a sequence that emits elements from the source until the specified predicate returns true for an element.</summary>
        /// <param name="predicate">A function to test each element for a condition. The sequence stops after the first
        /// element for which this function returns true.</param>
        /// <returns>An observable sequence that contains the elements from the source sequence up to and including the
        /// first element for which the predicate returns true.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="predicate"/> is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> TakeUntil(Func<T, bool> predicate) =>
            new PredicateStopSignal<T>(source, predicate);

        /// <summary>
        /// Returns an observable sequence that emits items from the source until <paramref name="predicate"/> returns
        /// <see langword="true"/> for an element or <paramref name="cancellationToken"/> is cancelled - whichever
        /// comes first.
        /// </summary>
        /// <param name="predicate">A predicate evaluated for each element; first true terminates the sequence.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="predicate"/> is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> TakeUntil(Func<T, bool> predicate, CancellationToken cancellationToken) =>
            new PredicateStopSignal<T>(source, predicate, cancellationToken);

        /// <summary>Returns an observable sequence that emits elements from the source sequence until the specified asynchronous predicate returns true for an element.</summary>
        /// <param name="asyncPredicate">A function that evaluates each element and its associated cancellation token asynchronously. The sequence
        /// stops after the first element for which this function returns true.</param>
        /// <returns>An observable sequence that contains the elements from the source sequence up to and including the
        /// first element for which the asynchronous predicate returns true.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="asyncPredicate"/> is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> TakeUntil(Func<T, CancellationToken, ValueTask<bool>> asyncPredicate) =>
            new AsyncPredicateStopSignal<T>(source, asyncPredicate);

        /// <summary>
        /// Returns an observable sequence that emits items from the source until <paramref name="asyncPredicate"/>
        /// returns <see langword="true"/> for an element or <paramref name="cancellationToken"/> is cancelled -
        /// whichever comes first.
        /// </summary>
        /// <param name="asyncPredicate">An async predicate evaluated for each element; first true terminates the sequence.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="asyncPredicate"/> is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> TakeUntil(
            Func<T, CancellationToken, ValueTask<bool>> asyncPredicate,
            CancellationToken cancellationToken) =>
            new AsyncPredicateStopSignal<T>(source, asyncPredicate, cancellationToken);

        /// <summary>Returns an observable sequence that emits items from the source sequence until the specified stop signal completes.</summary>
        /// <param name="stopSignal">A delegate that provides a completion signal. The returned observable will stop emitting items when this
        /// signal completes.</param>
        /// <returns>An observable sequence that emits items from the source until the stop signal completes.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="stopSignal"/> is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> TakeUntil(CompletionSignalDelegate stopSignal) =>
            DelegateStopSignal<T>.Create(source, stopSignal);

        /// <summary>Returns an observable sequence that emits items from the source sequence until the specified stop signal completes.</summary>
        /// <param name="stopSignal">A delegate that provides a completion signal. The returned observable will stop emitting items when this
        /// signal completes.</param>
        /// <param name="options">An optional set of options that configure the behavior of the take-until operation. If null, default options
        /// are used.</param>
        /// <returns>An observable sequence that emits items from the source until the stop signal completes.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="stopSignal"/> is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> TakeUntil(
            CompletionSignalDelegate stopSignal,
            TakeUntilOptions? options) =>
            DelegateStopSignal<T>.Create(source, stopSignal, options);

        /// <summary>Emits source items until <paramref name="stopSignal"/> or <paramref name="cancellationToken"/> fires.</summary>
        /// <param name="stopSignal">A delegate that provides the completion stop signal.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="stopSignal"/> is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> TakeUntil(
            CompletionSignalDelegate stopSignal,
            CancellationToken cancellationToken) =>
            DelegateStopSignal<T>.Create(source, stopSignal, cancellationToken);

        /// <summary>Emits source items until <paramref name="stopSignal"/> or <paramref name="cancellationToken"/> fires.</summary>
        /// <param name="stopSignal">A delegate that provides the completion stop signal.</param>
        /// <param name="options">Options controlling the take-until behavior, or null for defaults.</param>
        /// <param name="cancellationToken">A cancellation token that also terminates the result when cancelled.</param>
        /// <returns>An observable sequence that completes on the first of the two signals.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="stopSignal"/> is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> TakeUntil(
            CompletionSignalDelegate stopSignal,
            TakeUntilOptions? options,
            CancellationToken cancellationToken) =>
            DelegateStopSignal<T>.Create(source, stopSignal, options, cancellationToken);
    }
}
