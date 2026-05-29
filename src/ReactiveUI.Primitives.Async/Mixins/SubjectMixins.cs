// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Async.Subjects;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides extension methods for working with subjects in a reactive programming context.
/// </summary>
/// <remarks>The methods in this class enable interoperability between subjects and asynchronous observer
/// patterns. These extensions are intended to simplify the integration of subjects with APIs that expect asynchronous
/// observers.</remarks>
public static class SubjectMixins
{
    /// <summary>
    /// Creates an asynchronous observer wrapper for the specified subject.
    /// </summary>
    /// <typeparam name="T">The type of the elements processed by the subject and observer.</typeparam>
    /// <param name="subject">The subject to wrap as an asynchronous observer. Cannot be null.</param>
    /// <returns>An asynchronous observer that forwards notifications to the specified subject.</returns>
    [SuppressMessage(
        "Roslynator",
        "RCS1047:Non-asynchronous method name should not end with \'Async\'",
        Justification = "This is an existing method")]
    public static IObserverAsync<T> AsObserverAsync<T>(this ISubjectAsync<T> subject)
    {
        ArgumentExceptionHelper.ThrowIfNull(subject);

        return new SubjectAsyncObserver<T>(subject);
    }

    /// <summary>
    /// Creates a new subject that applies a transformation to the values of the source subject using the specified
    /// mapping function.
    /// </summary>
    /// <remarks>The returned subject reflects the mapped values of the original subject. Subscribers to the
    /// returned subject will observe the transformed sequence as defined by the mapper function. The mapping is applied
    /// to all values published by the source subject.</remarks>
    /// <typeparam name="T">The type of the elements processed by the subject.</typeparam>
    /// <param name="this">The source subject whose values are to be mapped.</param>
    /// <param name="mapper">A function that takes an asynchronous observable of type T and returns a transformed asynchronous observable of
    /// type T. This function defines how the values are mapped.</param>
    /// <returns>A subject that emits values transformed by the specified mapping function.</returns>
    public static ISubjectAsync<T> MapValues<T>(
        this ISubjectAsync<T> @this,
        Func<IObservableAsync<T>, IObservableAsync<T>> mapper)
    {
        ArgumentExceptionHelper.ThrowIfNull(@this);
        ArgumentExceptionHelper.ThrowIfNull(mapper);

        return new MappedSubject<T>(@this, mapper);
    }

    /// <summary>
    /// A subject that applies a transformation to the observable values of the source subject.
    /// </summary>
    /// <typeparam name="T">The type of elements processed by the subject.</typeparam>
    internal sealed class MappedSubject<T>(
        ISubjectAsync<T> original,
        Func<IObservableAsync<T>, IObservableAsync<T>> mapper) : ISubjectAsync<T>
    {
        /// <inheritdoc/>
        public IObservableAsync<T> Values { get; } = mapper(original.Values);

        /// <inheritdoc/>
        public ValueTask<IAsyncDisposable> SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken) =>
            Values.SubscribeAsync(observer, cancellationToken);

        /// <inheritdoc/>
        public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
            original.OnNextAsync(value, cancellationToken);

        /// <inheritdoc/>
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
            original.OnErrorResumeAsync(error, cancellationToken);

        /// <inheritdoc/>
        public ValueTask OnCompletedAsync(Result result) => original.OnCompletedAsync(result);

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => original.DisposeAsync();
    }

    /// <summary>
    /// An asynchronous observer that forwards all notifications to the wrapped subject.
    /// </summary>
    /// <typeparam name="T">The type of elements processed by the observer.</typeparam>
    /// <param name="subject">The subject to forward notifications to.</param>
    internal sealed class SubjectAsyncObserver<T>(ISubjectAsync<T> subject) : ObserverAsync<T>
    {
        /// <summary>
        /// Forwards the value to the wrapped subject. The cancellation token is intentionally
        /// replaced with <see cref="CancellationToken.None"/> rather than passing our own dispose
        /// token through: subscribers downstream of the subject are <see cref="ObserverAsync{T}"/>
        /// wraps whose <c>TryEnter</c> short-circuits on <see cref="CancellationToken.None"/> via
        /// its fast path, avoiding a per-emission linked-CTS allocation on every observer. The
        /// upstream-disposal cascade is unaffected — by the time this observer is disposed (on
        /// source completion / error) no further <c>OnNext</c> calls reach this method.
        /// </summary>
        /// <param name="value">The value to be processed by the observer.</param>
        /// <param name="cancellationToken">The token captured by the base observer's TryEnter scope. Ignored on the forward.</param>
        /// <returns>A ValueTask that represents the asynchronous operation.</returns>
        protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            return subject.OnNextAsync(value, CancellationToken.None);
        }

        /// <summary>
        /// Forwards the error to the wrapped subject, replacing the cancellation token with
        /// <see cref="CancellationToken.None"/> for the same fast-path reason as
        /// <see cref="OnNextAsyncCore"/>.
        /// </summary>
        /// <param name="error">The exception that caused the error condition. Cannot be null.</param>
        /// <param name="cancellationToken">The token captured by the base observer's TryEnter scope. Ignored on the forward.</param>
        /// <returns>A ValueTask that represents the asynchronous error handling operation.</returns>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            return subject.OnErrorResumeAsync(error, CancellationToken.None);
        }

        /// <summary>
        /// Performs asynchronous completion logic when the operation has finished, using the specified result.
        /// </summary>
        /// <param name="result">The result of the completed operation, containing any relevant outcome information.</param>
        /// <returns>A ValueTask that represents the asynchronous completion operation.</returns>
        protected override ValueTask OnCompletedAsyncCore(Result result) => subject.OnCompletedAsync(result);
    }
}
