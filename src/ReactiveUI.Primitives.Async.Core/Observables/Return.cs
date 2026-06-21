// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides factory methods for creating asynchronous observable sequences.</summary>
/// <remarks>The SignalAsync class contains static methods for constructing and manipulating asynchronous
/// observables. Use these methods to create observables that emit values asynchronously, supporting scenarios such as
/// background processing or integration with asynchronous workflows.</remarks>
public static partial class SignalAsync
{
    /// <summary>Creates an observable sequence that emits a single value and then completes.</summary>
    /// <remarks>The returned observable sequence emits the value asynchronously and completes immediately
    /// after. Subscribe returns before the value is observed — emit + complete are scheduled onto the
    /// threadpool, preserving the deferred semantic the surrounding pipeline expects.</remarks>
    /// <typeparam name="T">The type of the value to be emitted by the observable sequence.</typeparam>
    /// <param name="value">The value to be emitted by the observable sequence.</param>
    /// <returns>An observable sequence that emits the specified value and then signals completion.</returns>
    public static IObservableAsync<T> Emit<T>(T value) => new ReturnSignalAsync<T>(value);

    /// <summary>Creates an observable sequence that emits a single value and then completes.</summary>
    /// <typeparam name="T">The type of the value to be emitted by the observable sequence.</typeparam>
    /// <param name="value">The value to be emitted by the observable sequence.</param>
    /// <returns>An observable sequence that emits the specified value and then signals completion.</returns>
    public static IObservableAsync<T> Return<T>(T value) => Emit(value);

    /// <summary>
    /// Single-value observable that captures the emitted value as a field and routes through a typed
    /// <see cref="TaskSignalSubscription{T}"/>. Same deferred-emit semantic as the previous
    /// <c>CreateAsBackgroundJob</c> path, but without the per-call <c>Func</c> closure allocation.
    /// </summary>
    /// <typeparam name="T">The element type emitted.</typeparam>
    /// <param name="value">The captured value emitted on each subscribe.</param>
    internal sealed class ReturnSignalAsync<T>(T value) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            ReturnSubscription subscription = new(observer, value);
            subscription.Start();
            return new(subscription);
        }

        /// <summary>Per-subscription task body that emits the captured value and signals completion.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="value">The captured value.</param>
        private sealed class ReturnSubscription(IObserverAsync<T> observer, T value) : TaskSignalSubscription<T>(observer)
        {
            /// <inheritdoc/>
            protected override async ValueTask ExecuteAsyncCore(IObserverAsync<T> observer, CancellationToken cancellationToken)
            {
                await observer.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
                await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
            }
        }
    }
}
