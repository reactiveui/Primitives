// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

/// <summary>Owns one open collaboration client context, stream, and HTTP client.</summary>
internal sealed class CollaborationClientSession : IAsyncDisposable
{
    /// <summary>The owned occasionally connected context.</summary>
    private readonly OccasionallyConnectedContext _context;

    /// <summary>The caller-owned HTTP client used by the transport adapter.</summary>
    private readonly HttpClient _httpClient;

    /// <summary>Initializes a new instance of the <see cref="CollaborationClientSession"/> class.</summary>
    /// <param name="context">The occasionally connected context.</param>
    /// <param name="activity">The activity stream.</param>
    /// <param name="httpClient">The HTTP client.</param>
    internal CollaborationClientSession(
        OccasionallyConnectedContext context,
        IOccasionallyConnectedStream<ActivityView, ActivityUpdate> activity,
        HttpClient httpClient)
    {
        _context = context;
        Activity = activity;
        _httpClient = httpClient;
    }

    /// <summary>Gets the activity stream.</summary>
    public IOccasionallyConnectedStream<ActivityView, ActivityUpdate> Activity { get; }

    /// <summary>Gets the context-level synchronization lifecycle states.</summary>
    internal IObservable<SyncState> ContextSyncStates => _context.SyncStates;

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        try
        {
            await _context.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _httpClient.Dispose();
        }
    }

    /// <summary>Starts the context and registered streams.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The start operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask StartAsync(CancellationToken cancellationToken) => _context.StartAsync(cancellationToken);

    /// <summary>Stops remote work without completing stream observers.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The stop operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask StopAsync(CancellationToken cancellationToken) => _context.StopAsync(cancellationToken);

    /// <summary>Publishes an activity update through the typed stream.</summary>
    /// <param name="update">The update.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The local publish receipt.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask<PublishReceipt> PublishAsync(ActivityUpdate update, CancellationToken cancellationToken) =>
        Activity.PublishAsync(update, CreatePublishOptions(), cancellationToken);

    /// <summary>Creates activity publish options.</summary>
    /// <returns>The publish options.</returns>
    private static RemotePublishOptions CreatePublishOptions() =>
        new()
        {
            StreamId = ActivityContracts.StreamId,
            Durable = true,
            DeliveryGuarantee = DeliveryGuarantee.AtLeastOnce,
            ConflictPolicy = ConflictPolicy.Merge,
            AdmissionStrategy = BufferStrategy.Block,
        };
}
