// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides extension methods for converting tasks, asynchronous enumerables, and enumerable sequences into
/// asynchronous observable sequences.
/// </summary>
/// <remarks>The methods in this class enable integration of task-based and enumerable workflows with asynchronous
/// observables. Each method returns an observable sequence that emits values or completion notifications based on the
/// source sequence or task. Cancellation and error propagation are supported according to the source's behavior. These
/// extensions are useful for bridging between different asynchronous programming models.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Observable-conversion operators for an asynchronous enumerable source.</summary>
    /// <param name="this">The asynchronous enumerable sequence to convert. Cannot be null.</param>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    extension<T>(IAsyncEnumerable<T> @this)
    {
        /// <summary>Converts an asynchronous enumerable sequence to an asynchronous observable sequence.</summary>
        /// <remarks>The returned observable emits each element from the source sequence as it is produced and
        /// signals completion when the source sequence ends. Cancellation is supported via the observer's cancellation
        /// token.</remarks>
        /// <returns>An asynchronous observable sequence that emits the elements of the source sequence.</returns>
        [SuppressMessage(
            "Roslynator",
            "RCS1047:Non-asynchronous method name should not end with \'Async\'",
            Justification = "This is an existing method")]
        public IObservableAsync<T> ToAsyncSignal() => new AsyncEnumerableSignal<T>(@this);
    }

    /// <summary>Observable-conversion operators for an enumerable source.</summary>
    /// <param name="this">The enumerable sequence to convert to an asynchronous observable. Cannot be null.</param>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    extension<T>(IEnumerable<T> @this)
    {
        /// <summary>
        /// Converts the specified enumerable sequence to an asynchronous observable sequence, emitting each element in the
        /// background.
        /// </summary>
        /// <remarks>The returned observable emits items on a background thread. Cancellation is supported via the
        /// observer's cancellation token. If the source sequence is empty, the observable completes immediately.</remarks>
        /// <returns>An asynchronous observable sequence that emits each element from the source enumerable and completes when all
        /// elements have been emitted.</returns>
        [SuppressMessage(
            "Roslynator",
            "RCS1047:Non-asynchronous method name should not end with \'Async\'",
            Justification = "This is an existing method")]
        public IObservableAsync<T> ToAsyncSignal() => new EnumerableSignal<T>(@this);
    }

    /// <summary>Observable-conversion operators for an asynchronous observable source.</summary>
    /// <param name="this">The source sequence.</param>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>Returns an async observable as an async signal.</summary>
        /// <returns>An observable sequence validated.</returns>
        [SuppressMessage(
            "Roslynator",
            "RCS1047:Non-asynchronous method name should not end with \'Async\'",
            Justification = "This is an existing method")]
        public IObservableAsync<T> ToAsyncSignal() =>
            @this ?? throw new ArgumentNullException(nameof(@this));
    }

    /// <summary>Observable-conversion operators for a task that produces a single value.</summary>
    /// <param name="this">The task to convert to an asynchronous observable sequence. Cannot be null.</param>
    /// <typeparam name="T">The type of the value produced by the task and emitted by the observable sequence.</typeparam>
    extension<T>(Task<T> @this)
    {
        /// <summary>
        /// Converts a task representing a single asynchronous value into an observable sequence that emits the result when
        /// the task completes.
        /// </summary>
        /// <remarks>The returned observable will emit the task's result and then complete. If the task is
        /// canceled or fails, the observable will propagate the corresponding error. The task is awaited in the background,
        /// and cancellation is supported via the observable's subscription.</remarks>
        /// <returns>An asynchronous observable sequence that emits the result of the task when it completes, followed by a
        /// completion notification.</returns>
        [SuppressMessage(
            "Roslynator",
            "RCS1047:Non-asynchronous method name should not end with \'Async\'",
            Justification = "This is an existing method")]
        public IObservableAsync<T> ToAsyncSignal() => new TaskResultSignal<T>(@this);
    }
}
