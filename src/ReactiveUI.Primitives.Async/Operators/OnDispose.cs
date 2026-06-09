// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internals;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for composing and managing asynchronous observable sequences.</summary>
/// <remarks>The SignalAsync class offers utility methods for working with asynchronous observables, enabling
/// additional behaviors such as resource cleanup or side-effect handling when subscriptions are disposed. These methods
/// are intended to simplify the creation and management of custom observable pipelines in asynchronous programming
/// scenarios.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Disposal-callback operators that run an action when the observable source subscription is disposed.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>Registers a callback to be invoked asynchronously when the observable sequence is disposed.</summary>
        /// <remarks>Use this method to perform custom asynchronous cleanup or resource release logic when
        /// the observable sequence is disposed. The callback is invoked when the subscription is disposed, either
        /// explicitly or when the observer completes or errors.</remarks>
        /// <param name="disposeAction">A function that returns a ValueTask representing the asynchronous operation to execute upon disposal of the
        /// observable sequence. Cannot be null.</param>
        /// <returns>An SignalAsync{T} that invokes the specified asynchronous callback when disposed.</returns>
        public IObservableAsync<T> OnDispose(Func<ValueTask> disposeAction)
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);
            ArgumentExceptionHelper.ThrowIfNull(disposeAction);

            return new OnDisposeSignal<T>(@this, disposeAction);
        }

        /// <summary>Registers an action to be invoked when the observable sequence is disposed.</summary>
        /// <remarks>Use this method to perform cleanup or resource release logic when a subscription to
        /// the observable is disposed. The specified action is called synchronously during disposal. If multiple
        /// actions are registered through chained calls, each will be invoked in the order registered.</remarks>
        /// <param name="disposeAction">The action to execute when the subscription is disposed. Cannot be null.</param>
        /// <returns>An observable sequence that invokes the specified action upon disposal of the subscription.</returns>
        public IObservableAsync<T> OnDispose(Action disposeAction)
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);
            ArgumentExceptionHelper.ThrowIfNull(disposeAction);

            return new OnDisposeSyncSignal<T>(@this, disposeAction);
        }
    }

    /// <summary>Wraps a source observable with an async-action <c>OnDispose</c> observer without the prior <c>Create&lt;T&gt;</c> wrapper layer.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="disposeAction">The async dispose action.</param>
    internal sealed class OnDisposeSignal<T>(IObservableAsync<T> source, Func<ValueTask> disposeAction) : SignalAsync<T>
    {
        /// <inheritdoc/>
        protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(IObserverAsync<T> observer, CancellationToken cancellationToken)
        {
            var sink = new OnDisposeWitness<T>(observer, disposeAction);
            return source.SubscribeAsync(sink, cancellationToken);
        }
    }

    /// <summary>Wraps a source observable with a sync-action <c>OnDispose</c> observer without the prior <c>Create&lt;T&gt;</c> wrapper layer.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="disposeAction">The sync dispose action.</param>
    internal sealed class OnDisposeSyncSignal<T>(IObservableAsync<T> source, Action disposeAction) : SignalAsync<T>
    {
        /// <inheritdoc/>
        protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(IObserverAsync<T> observer, CancellationToken cancellationToken)
        {
            var sink = new OnDisposeWitnessSync<T>(observer, disposeAction);
            return source.SubscribeAsync(sink, cancellationToken);
        }
    }

    /// <summary>A witness that invokes a synchronous action when disposed.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="observer">The downstream observer to forward notifications to.</param>
    /// <param name="finallySync">The synchronous action to invoke when disposed.</param>
    internal sealed class OnDisposeWitnessSync<T>(IObserverAsync<T> observer, Action finallySync)
        : ForwardingWitnessAsync<T>(observer)
    {
        /// <inheritdoc/>
        protected override async ValueTask DisposeAsyncCore()
        {
            try
            {
                finallySync();
            }
            finally
            {
                await base.DisposeAsyncCore().ConfigureAwait(false);
            }
        }
    }

    /// <summary>A witness that invokes an asynchronous callback when disposed.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="observer">The downstream observer to forward notifications to.</param>
    /// <param name="finallyAsync">The asynchronous callback to invoke when disposed.</param>
    internal sealed class OnDisposeWitness<T>(IObserverAsync<T> observer, Func<ValueTask> finallyAsync)
        : ForwardingWitnessAsync<T>(observer)
    {
        /// <inheritdoc/>
        protected override async ValueTask DisposeAsyncCore()
        {
            try
            {
                await finallyAsync().ConfigureAwait(false);
            }
            finally
            {
                await base.DisposeAsyncCore().ConfigureAwait(false);
            }
        }
    }
}
