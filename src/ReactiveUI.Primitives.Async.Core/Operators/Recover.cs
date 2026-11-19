// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for composing and handling asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Error-handling operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Creates a new observable sequence that continues with a handler-provided sequence when an exception occurs in the source sequence.</summary>
        /// <param name="handler">A function that receives the exception thrown by the source sequence and returns an alternative observable
        /// sequence to continue with.</param>
        /// <returns>An observable sequence that emits items from the source sequence, or from the handler-provided sequence if
        /// an exception is encountered.</returns>
        /// <exception cref="ArgumentNullException">Thrown if the source sequence or <paramref name="handler"/> is null.</exception>
        /// <remarks>If the handler throws, the resulting sequence completes with that exception.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Recover(Func<Exception, IObservableAsync<T>> handler) =>
            new CatchSignal<T>(source, handler);

        /// <summary>Resumes with a fallback sequence after a terminal failure.</summary>
        /// <param name="fallback">The fallback sequence used after a failure.</param>
        /// <returns>An observable sequence that resumes with the fallback on failure.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Resume(IObservableAsync<T> fallback) =>
            CatchSignal<T>.CreateResume(source, fallback);

        /// <summary>Continues the observable sequence with an alternative sequence provided by the specified handler when an error occurs, and ignores the error after invoking the handler.</summary>
        /// <param name="handler">A function that receives the exception and returns an alternative observable sequence to resume with after
        /// an error occurs.</param>
        /// <returns>An observable sequence that resumes with the sequence returned by the handler when an error is encountered,
        /// and ignores the error after handling.</returns>
        /// <remarks>An error-resume notification is reported to the global unhandled-exception handler rather than
        /// forwarded downstream, so subscribers never observe it.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> CatchAndIgnoreErrorResume(Func<Exception, IObservableAsync<T>> handler) =>
            CatchSignal<T>.CreateIgnoreErrorResume(source, handler);
    }
}
