// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides factory methods for creating asynchronous observable sequences.</summary>
public static partial class SignalAsync
{
    /// <summary>Creates an observable sequence that emits a single value and then completes.</summary>
    /// <typeparam name="T">The type of the value to be emitted by the observable sequence.</typeparam>
    /// <param name="value">The value to be emitted by the observable sequence.</param>
    /// <returns>An observable sequence that emits the specified value and then signals completion.</returns>
    /// <remarks>Notification starts during subscription and may finish synchronously.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<T> Emit<T>(T value) => new ReturnSignalAsync<T>(value);

    /// <summary>Creates an observable sequence that emits a single value and then completes.</summary>
    /// <typeparam name="T">The type of the value to be emitted by the observable sequence.</typeparam>
    /// <param name="value">The value to be emitted by the observable sequence.</param>
    /// <returns>An observable sequence that emits the specified value and then signals completion.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification = "Return is the Rx-compatible alias for Emit and builds the same signal with no forwarding hop.")]
    public static IObservableAsync<T> Return<T>(T value) => new ReturnSignalAsync<T>(value);

    /// <summary>Defers one value per subscriber without allocating a closure.</summary>
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
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="value">The captured value.</param>
        private sealed class ReturnSubscription(IObserverAsync<T> downstream, T value) : IAsyncDisposable, ITaskSignalJob<T>
        {
            /// <summary>Runs the job and joins it on disposal.</summary>
            private readonly TaskSignalState _task = new();

            /// <summary>Starts the job and returns without waiting for it to finish.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Start() => _task.Start(this, downstream);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask DisposeAsync() => _task.DisposeAsync();

            /// <inheritdoc/>
            async ValueTask ITaskSignalJob<T>.ExecuteAsync(
                IObserverAsync<T> observer,
                CancellationToken cancellationToken)
            {
                await observer.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
                await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
            }
        }
    }
}
