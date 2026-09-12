// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Provides a local-first stream with typed local state and input values.</summary>
/// <typeparam name="TState">The local state type.</typeparam>
/// <typeparam name="TInput">The input value type.</typeparam>
public interface IOccasionallyConnectedStream<TState, TInput> : IAsyncDisposable
{
    /// <summary>Gets the stable identifier for this stream.</summary>
    StreamId StreamId { get; }

    /// <summary>Gets the durable subscription identifier for this stream.</summary>
    SubscriptionId SubscriptionId { get; }

    /// <summary>Gets locally committed state changes, replaying the latest committed state to new subscribers.</summary>
    IObservable<TState> Local { get; }

    /// <summary>Gets decoded, deduplicated remote messages after durable inbox application.</summary>
    /// <remarks>This observable does not replay remote messages by default.</remarks>
    IObservable<RemoteMessage<TInput>> Remote { get; }

    /// <summary>Gets synchronization lifecycle state changes.</summary>
    IObservable<SyncState> SyncStates { get; }

    /// <summary>Gets per-operation state changes.</summary>
    IObservable<SyncOperationStatus> OperationStates { get; }

    /// <summary>Gets operational faults.</summary>
    IObservable<OccasionallyConnectedFault> Faults { get; }

    /// <summary>Gets a bounded synchronous input bridge for callers that do not require a publish receipt.</summary>
    /// <remarks>
    /// <para>Asynchronous admission and persistence failures are reported through <see cref="Faults"/>.</para>
    /// <para>Completion and error notifications affect only the producer represented by this observer.</para>
    /// </remarks>
    IObserver<TInput> Input { get; }

    /// <summary>Publishes an input value and returns its local admission receipt.</summary>
    /// <param name="value">The value to publish.</param>
    /// <param name="options">The optional publish options.</param>
    /// <param name="cancellationToken">The token used to cancel admission and persistence.</param>
    /// <returns>The local publish receipt.</returns>
    ValueTask<PublishReceipt> PublishAsync(
        TInput value,
        RemotePublishOptions? options,
        CancellationToken cancellationToken);

    /// <summary>Starts stream synchronization work.</summary>
    /// <param name="cancellationToken">The token used to cancel startup.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask StartAsync(CancellationToken cancellationToken);

    /// <summary>Stops stream synchronization work.</summary>
    /// <param name="cancellationToken">The token used to cancel shutdown waiting.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask StopAsync(CancellationToken cancellationToken);
}
