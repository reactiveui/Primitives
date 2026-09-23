// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <content>Convenience publication and paired queue observation for a typed stream.</content>
internal sealed partial class OccasionallyConnectedStream<TState, TInput>
{
    /// <summary>Dispatches state and queue snapshots captured at one mutation boundary.</summary>
    private readonly ObserverNotificationDispatcher<OccasionallyConnectedCommittedStateQueueSnapshot<TState>> _committedStateQueueSnapshots;

    /// <summary>Stores the latest committed payload and queue summary for new subscribers.</summary>
    private LatestPaired? _latestPaired;

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<PublishReceipt> PublishSerializedInputAsync(
        PayloadEnvelope payload,
        RemotePublishOptions? options,
        CancellationToken cancellationToken) =>
        PublishSerializedAsync(payload, options, cancellationToken);

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void PublishInputFault(string code, string message, OperationId? operationId, Exception exception) =>
        PublishFault(code, message, operationId, exception);

    /// <summary>Publishes the state payload with its committed durable queue aggregate.</summary>
    /// <param name="payload">The owned state payload.</param>
    /// <param name="queue">The queue aggregate from the same mutation.</param>
    private void PublishCommittedStateQueueSnapshot(PayloadEnvelope payload, QueueDiagnosticSnapshot queue)
    {
        var pending = new PendingSyncSummary(checked((int)queue.PendingOperations), queue.PendingBytes, null);
        var schedules = new List<Action>();
        var factory = CreatePairedSnapshotFactory(payload, pending);
        var sizeBytes = GetPendingSnapshotNotificationSize(payload);
        lock (_gate)
        {
            _latestPaired = new(payload, pending);
            _ = _committedStateQueueSnapshots.PublishEventDeferred(factory, sizeBytes, schedules);
        }

        RunNotificationSchedules(schedules);
    }

    /// <summary>Creates a fresh state instance for each paired observer callback.</summary>
    /// <param name="payload">The committed state payload.</param>
    /// <param name="pending">The queue summary captured with the state.</param>
    /// <returns>The notification factory.</returns>
    private Func<CancellationToken, ValueTask<OccasionallyConnectedCommittedStateQueueSnapshot<TState>>> CreatePairedSnapshotFactory(
        PayloadEnvelope payload,
        PendingSyncSummary pending) =>
        async cancellationToken => new(
            await DeserializeLocalSnapshotAsync(payload, cancellationToken).ConfigureAwait(false),
            pending);

    /// <summary>Subscribes to paired snapshots with atomic latest replay.</summary>
    /// <param name="observer">The observer.</param>
    /// <returns>The subscription handle.</returns>
    private IDisposable SubscribePaired(IObserver<OccasionallyConnectedCommittedStateQueueSnapshot<TState>> observer)
    {
        var schedules = new List<Action>(1);
        IDisposable subscription;
        lock (_gate)
        {
            ThrowIfDisposed();
            subscription = _latestPaired is { } latest
                ? _committedStateQueueSnapshots.SubscribeDeferred(
                    observer,
                    _options.NotificationOptions,
                    true,
                    CreatePairedSnapshotFactory(latest.Payload, latest.Pending),
                    GetPendingSnapshotNotificationSize(latest.Payload),
                    schedules)
                : _committedStateQueueSnapshots.Subscribe(observer, _options.NotificationOptions);
        }

        RunNotificationSchedules(schedules);
        return subscription;
    }

    /// <summary>Observable wrapper for isolated paired state and queue snapshots.</summary>
    /// <param name="owner">The owning stream.</param>
    private sealed class PairedObservable(OccasionallyConnectedStream<TState, TInput> owner) :
        IObservable<OccasionallyConnectedCommittedStateQueueSnapshot<TState>>
    {
        /// <inheritdoc />
        public IDisposable Subscribe(IObserver<OccasionallyConnectedCommittedStateQueueSnapshot<TState>> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);
            return owner.SubscribePaired(observer);
        }
    }

    /// <summary>Stores the committed state payload and matching queue summary.</summary>
    /// <param name="Payload">The owned state payload.</param>
    /// <param name="Pending">The queue summary captured with it.</param>
    private sealed record LatestPaired(PayloadEnvelope Payload, PendingSyncSummary Pending);
}
