// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Provides helpers for observing occasionally connected operations.</summary>
public static class OccasionallyConnectedExtensions
{
    /// <summary>Provides synchronization waiting helpers.</summary>
    /// <param name="engine">The synchronization engine.</param>
    extension(ISyncEngine engine)
    {
        /// <summary>Waits for an operation to synchronize using the system clock.</summary>
        /// <param name="operationId">The operation to observe.</param>
        /// <param name="timeout">The maximum wait, or <see cref="Timeout.InfiniteTimeSpan"/>.</param>
        /// <returns>The synchronization wait.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask AwaitSynchronizedAsync(OperationId operationId, TimeSpan timeout) =>
            engine.AwaitSynchronizedAsync(operationId, timeout, TimeProvider.System, CancellationToken.None);

        /// <summary>Waits for an operation to synchronize using the system clock.</summary>
        /// <param name="operationId">The operation to observe.</param>
        /// <param name="timeout">The maximum wait, or <see cref="Timeout.InfiniteTimeSpan"/>.</param>
        /// <param name="cancellationToken">The token canceling only the wait.</param>
        /// <returns>The synchronization wait.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask AwaitSynchronizedAsync(OperationId operationId, TimeSpan timeout, CancellationToken cancellationToken) =>
            engine.AwaitSynchronizedAsync(operationId, timeout, TimeProvider.System, cancellationToken);

        /// <summary>Waits for an operation to synchronize using the supplied clock.</summary>
        /// <param name="operationId">The operation to observe.</param>
        /// <param name="timeout">The maximum wait, or <see cref="Timeout.InfiniteTimeSpan"/>.</param>
        /// <param name="timeProvider">The clock used to measure the wait.</param>
        /// <returns>The synchronization wait.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask AwaitSynchronizedAsync(OperationId operationId, TimeSpan timeout, TimeProvider timeProvider) =>
            engine.AwaitSynchronizedAsync(operationId, timeout, timeProvider, CancellationToken.None);

        /// <summary>Waits for a persisted or newly reported synchronized result.</summary>
        /// <param name="operationId">The operation to observe.</param>
        /// <param name="timeout">The maximum wait, or <see cref="Timeout.InfiniteTimeSpan"/>.</param>
        /// <param name="timeProvider">The clock used to measure the wait.</param>
        /// <param name="cancellationToken">The token canceling only the wait.</param>
        /// <returns>The synchronization wait.</returns>
        /// <exception cref="ArgumentNullException">The engine or clock is missing.</exception>
        /// <exception cref="ArgumentException">The operation identifier is empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException">The timeout is invalid.</exception>
        /// <exception cref="OperationCanceledException">The caller cancels the wait.</exception>
        /// <exception cref="TimeoutException">The wait times out.</exception>
        /// <exception cref="InvalidOperationException">The operation fails or its status source closes without a terminal result.</exception>
        /// <remarks>
        /// Subscribes before querying persisted status so results arriving during lookup are not lost. Timeout and
        /// cancellation release the waiting subscription; they do not stop the engine or cancel the durable operation.
        /// Rejected, dead-lettered, ambiguous and guarantee-expired results fail the wait. Conflict remains pending
        /// until resolution produces a terminal result. The status lookup observes the supplied cancellation token;
        /// a lookup still in progress after timeout may finish independently, and its failure is observed.
        /// </remarks>
        public ValueTask AwaitSynchronizedAsync(
            OperationId operationId,
            TimeSpan timeout,
            TimeProvider timeProvider,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(engine);
            return new(OperationSynchronizationWaiter.WaitAsync(
                engine.OperationStates,
                token => engine.GetOperationStatusAsync(operationId, token),
                operationId,
                timeout,
                timeProvider,
                cancellationToken));
        }
    }
}
