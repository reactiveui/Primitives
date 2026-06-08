// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>An observable that switches the notification context of a source observable to a specified async context.</summary>
/// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
/// <param name="source">The source observable whose notifications will be context-switched.</param>
/// <param name="asyncContext">The async context to switch notifications onto.</param>
/// <param name="forceYielding">Whether to force yielding even if already on the target context.</param>
internal sealed class ContextSwitchSignalAsync<T>(
    IObservableAsync<T> source,
    AsyncContext asyncContext,
    bool forceYielding) : SignalAsync<T>
{
    /// <inheritdoc/>
    protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        var contextSwitchObserver = new ContextSwitchWitness(observer, asyncContext, forceYielding);
        return source.SubscribeAsync(contextSwitchObserver, cancellationToken);
    }

    /// <summary>An observer that switches each notification onto the specified async context before forwarding.</summary>
    /// <param name="observer">The downstream observer to forward notifications to.</param>
    /// <param name="asyncContext">The async context to switch onto.</param>
    /// <param name="forceYielding">Whether to force yielding even if already on the target context.</param>
    internal sealed class ContextSwitchWitness(IObserverAsync<T> observer, AsyncContext asyncContext, bool forceYielding)
        : ObserverAsync<T>
    {
        /// <summary>Slow path: switch to the target context then forward the value.
        /// Exposed as <see langword="internal"/> so tests can invoke the slow-path body
        /// directly without needing to race the current-context check.</summary>
        /// <param name="value">The value to forward.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task that completes after the context switch and downstream forward.</returns>
        internal async ValueTask ForwardAfterContextSwitchAsync(T value, CancellationToken cancellationToken)
        {
            await asyncContext.SwitchContextAsync(forceYielding, cancellationToken);
            await observer.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Slow path: switch to the target context then forward the error. Exposed as <see langword="internal"/> for direct unit testing.</summary>
        /// <param name="error">The error to forward.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task that completes after the context switch and downstream forward.</returns>
        internal async ValueTask ForwardErrorAfterContextSwitchAsync(Exception error, CancellationToken cancellationToken)
        {
            await asyncContext.SwitchContextAsync(forceYielding, cancellationToken);
            await observer.OnErrorResumeAsync(error, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Slow path: switch to the target context then forward completion. Exposed as <see langword="internal"/> for direct unit testing.</summary>
        /// <param name="result">The completion result.</param>
        /// <returns>A task that completes after the context switch and downstream forward.</returns>
        internal async ValueTask ForwardCompletionAfterContextSwitchAsync(Result result)
        {
            await asyncContext.SwitchContextAsync(forceYielding, CancellationToken.None);
            await observer.OnCompletedAsync(result).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            // Fast path: already on the target context and no forced yield — skip the awaitable
            // dance entirely and forward synchronously.
            if (!forceYielding && asyncContext.IsSameAsCurrentAsyncContext())
            {
                return observer.OnNextAsync(value, cancellationToken);
            }

            return ForwardAfterContextSwitchAsync(value, cancellationToken);
        }

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
        {
            if (!forceYielding && asyncContext.IsSameAsCurrentAsyncContext())
            {
                return observer.OnErrorResumeAsync(error, cancellationToken);
            }

            return ForwardErrorAfterContextSwitchAsync(error, cancellationToken);
        }

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result)
        {
            if (!forceYielding && asyncContext.IsSameAsCurrentAsyncContext())
            {
                return observer.OnCompletedAsync(result);
            }

            return ForwardCompletionAfterContextSwitchAsync(result);
        }
    }
}
