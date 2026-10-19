// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Recovery-only transport double for <see cref="SyncEngineTests"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Records a retained-history gap from a session without the snapshot recovery facet.</summary>
    private sealed class NonSnapshotRecoveryReceiveSession : IRemoteTransportSession
    {
        /// <summary>Gets subscribe requests received by the session.</summary>
        public List<RemoteSubscribeRequest> SubscribeRequests { get; } = [];

        /// <summary>Gets acknowledgements received by the session.</summary>
        public List<ReceiveAcknowledgement> Acknowledgements { get; } = [];

        /// <summary>Gets or sets the optional retained-history gap raised by subscription.</summary>
        public RemoteSubscriptionRetentionGapException? SubscriptionGap { get; init; }

        /// <summary>Gets or sets the optional gate that releases retained-history gap emission.</summary>
        public TaskCompletionSource? ReleaseSubscriptionGap { get; init; }

        /// <summary>Gets the signal set when a retained-history gap is ready to be emitted.</summary>
        public TaskCompletionSource SubscriptionGapReady { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the signal set when subscription begins.</summary>
        public TaskCompletionSource SubscribeEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the number of disposal calls.</summary>
        public int DisposeCalls { get; private set; }

        /// <inheritdoc/>
        public NegotiatedCapabilities NegotiatedCapabilities { get; init; } = new(
            new(1, 0),
            RemoteTransportCapabilities.ServerIdempotency
            | RemoteTransportCapabilities.AtomicApplyAndAcknowledge
            | RemoteTransportCapabilities.ReceiveAcknowledgements,
            MaximumBatchOperations: 32,
            MaximumBatchBytes: 4096,
            ServerIdempotencyRetention: TimeSpan.FromMinutes(DefaultServerIdempotencyRetentionMinutes),
            ClientInboxRetentionRequired: null);

        /// <inheritdoc/>
        public ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public async IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(
            RemoteSubscribeRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            SubscribeRequests.Add(request);
            _ = SubscribeEntered.TrySetResult();
            if (SubscriptionGap is not null)
            {
                _ = SubscriptionGapReady.TrySetResult();
                if (ReleaseSubscriptionGap is not null)
                {
                    await ReleaseSubscriptionGap.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                }

                throw SubscriptionGap;
            }

            yield break;
        }

        /// <inheritdoc/>
        public ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Acknowledgements.Add(acknowledgement);
            return default;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync()
        {
            DisposeCalls++;
            return default;
        }
    }
}
