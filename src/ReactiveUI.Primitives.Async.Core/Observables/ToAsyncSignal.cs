// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for converting tasks, asynchronous enumerables, and enumerable sequences into asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Observable-conversion operators for an asynchronous enumerable source.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The asynchronous enumerable sequence to convert. Cannot be null.</param>
    extension<T>(IAsyncEnumerable<T> source)
    {
        /// <summary>Converts an asynchronous enumerable sequence to an asynchronous observable sequence.</summary>
        /// <returns>An asynchronous observable sequence that emits the elements of the source sequence.</returns>
        /// <remarks>The source is enumerated once per subscriber.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SuppressMessage(
            "Roslynator",
            "RCS1047:Non-asynchronous method name should not end with \'Async\'",
            Justification = "The suffix names the asynchronous signal the method returns, not asynchronous work.")]
        public IObservableAsync<T> ToAsyncSignal() => new AsyncEnumerableSignal<T>(source);
    }

    /// <summary>Observable-conversion operators for an enumerable source.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The enumerable sequence to convert to an asynchronous observable. Cannot be null.</param>
    extension<T>(IEnumerable<T> source)
    {
        /// <summary>Emits the enumerable's elements, awaiting each observer notification.</summary>
        /// <returns>An asynchronous observable sequence that emits each element from the source enumerable and completes when all
        /// elements have been emitted.</returns>
        /// <remarks>Enumeration starts on the subscribing thread, once per subscriber.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SuppressMessage(
            "Roslynator",
            "RCS1047:Non-asynchronous method name should not end with \'Async\'",
            Justification = "The suffix names the asynchronous signal the method returns, not asynchronous work.")]
        public IObservableAsync<T> ToAsyncSignal() => new EnumerableSignal<T>(source);
    }

    /// <summary>Observable-conversion operators for an asynchronous observable source.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Null-checks and returns the source unchanged, so generic code can convert without testing the source's type.</summary>
        /// <returns>The same sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [SuppressMessage(
            "Roslynator",
            "RCS1047:Non-asynchronous method name should not end with \'Async\'",
            Justification = "The suffix names the asynchronous signal the method returns, not asynchronous work.")]
        public IObservableAsync<T> ToAsyncSignal() =>
            source ?? throw new ArgumentNullException(nameof(source));
    }

    /// <summary>Observable-conversion operators for a task that produces a single value.</summary>
    /// <typeparam name="T">The type of the value produced by the task and emitted by the observable sequence.</typeparam>
    /// <param name="task">The task to convert to an asynchronous observable sequence. Cannot be null.</param>
    extension<T>(Task<T> task)
    {
        /// <summary>Converts a task representing a single asynchronous value into an observable sequence that emits the result when the task completes.</summary>
        /// <returns>An asynchronous observable sequence that emits the result of the task when it completes, followed by a
        /// completion notification.</returns>
        /// <remarks>A faulted or cancelled task terminates the sequence with that error. Since the task is a single
        /// shared instance, every subscriber observes the same outcome.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SuppressMessage(
            "Roslynator",
            "RCS1047:Non-asynchronous method name should not end with \'Async\'",
            Justification = "The suffix names the asynchronous signal the method returns, not asynchronous work.")]
        public IObservableAsync<T> ToAsyncSignal() => new TaskResultSignal<T>(task);
    }
}
