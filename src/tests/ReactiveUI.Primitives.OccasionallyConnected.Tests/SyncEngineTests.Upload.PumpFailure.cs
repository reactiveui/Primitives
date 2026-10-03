// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Upload control pump failure tests for <see cref="SyncEngine"/>.</content>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies a failed capability read releases the acquired session lease and reports the pump fault.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadPumpReportsCapabilityReadFailureBeforeLeasingOutboxWork()
    {
        var operation = CreateOperation();
        var store = CreateUploadStore([operation]);
        var session = new FailingUploadCapabilitySession(new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes));
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(store, new() { SessionOverride = session });
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        session.FailCapabilityReads = true;
        engine.NotifyLocalCommitReady(Stream, operation);

        await faults.WaitForCountAsync(ExpectedSingleOperation, GuardTimeout);
        await Assert.That(faults.Values[0].Code).IsEqualTo("OC.Engine.UploadPump");
        await Assert.That(store.LeaseRequests).IsEmpty();
        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
    }

    /// <summary>Delegates prepared upload while allowing the negotiated capability read to fail after connection.</summary>
    /// <param name="inner">The prepared session.</param>
    private sealed class FailingUploadCapabilitySession(PreparedSession inner) : IRemoteTransportSession, IRemoteTransportBatchPreparer
    {
        /// <summary>Tracks whether connected capability reads should fail.</summary>
        private volatile bool _failCapabilityReads;

        /// <summary>Gets or sets whether later capability reads fail.</summary>
        public bool FailCapabilityReads
        {
            get => _failCapabilityReads;
            set => _failCapabilityReads = value;
        }

        /// <summary>Gets the number of prepared upload calls.</summary>
        public int PrepareCalls => inner.PrepareCalls;

        /// <inheritdoc/>
        public NegotiatedCapabilities NegotiatedCapabilities =>
            FailCapabilityReads ? throw new InvalidOperationException("The connected session lost its capability snapshot.") : inner.NegotiatedCapabilities;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IPreparedRemotePush> PreparePushAsync(SyncBatch batch, CancellationToken cancellationToken) =>
            inner.PreparePushAsync(batch, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken) =>
            inner.PushAsync(batch, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(RemoteSubscribeRequest request, CancellationToken cancellationToken) =>
            inner.SubscribeAsync(request, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, CancellationToken cancellationToken) =>
            inner.AcknowledgeAsync(acknowledgement, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }
}
