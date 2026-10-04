// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Convenience overloads for <see cref="ISyncEngine"/> that use <see cref="CancellationToken.None"/>.</summary>
public static class ISyncEngineExtensions
{
    /// <summary>Convenience overloads for a synchronization engine.</summary>
    /// <param name="engine">The synchronization engine.</param>
    extension(ISyncEngine engine)
    {
        /// <summary>Persists and admits a synchronization operation.</summary>
        /// <param name="operation">The operation to enqueue.</param>
        /// <returns>The durable publish receipt.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PublishReceipt> EnqueueOperationAsync(SyncOperation operation) =>
            engine.EnqueueOperationAsync(operation, CancellationToken.None);

        /// <summary>Gets the latest durable status recorded for an operation.</summary>
        /// <param name="operationId">The operation identifier.</param>
        /// <returns>The operation status, or <see langword="null"/> when no status is known.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId) =>
            engine.GetOperationStatusAsync(operationId, CancellationToken.None);

        /// <summary>Starts synchronization work.</summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask StartAsync() => engine.StartAsync(CancellationToken.None);

        /// <summary>Stops synchronization work after in-flight local commits complete.</summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask StopAsync() => engine.StopAsync(CancellationToken.None);

        /// <summary>Requests an immediate synchronization attempt.</summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask TriggerSyncAsync() => engine.TriggerSyncAsync(CancellationToken.None);
    }
}
