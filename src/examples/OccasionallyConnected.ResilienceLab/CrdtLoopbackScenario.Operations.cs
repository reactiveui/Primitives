// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Runs the bounded in-memory CRDT loopback demonstration.</summary>
internal static partial class CrdtLoopbackScenario
{
    /// <summary>Pushes G-counter operations and an exact duplicate replay.</summary>
    /// <param name="sessionA">The first client session.</param>
    /// <param name="sessionB">The second client session.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The asynchronous operation.</returns>
    private static async ValueTask PushCounterOperationsAsync(
        IRemoteTransportSession sessionA,
        IRemoteTransportSession sessionB,
        CrdtBounds bounds,
        CancellationToken cancellationToken)
    {
        var first = CreateOperationBatch(
            GCounterClientABatchSeed,
            GCounterClientAOperationSeed,
            GCounterStream,
            FirstSequence,
            FutureClientTimestamp,
            CrdtMutation.GCounterSet(ClientAId, GCounterClientA),
            bounds);
        await CrdtLoopbackPushVerifier.PushAcceptedAsync(sessionA, first, cancellationToken).ConfigureAwait(false);
        await CrdtLoopbackPushVerifier.PushAcceptedAsync(sessionA, first, cancellationToken).ConfigureAwait(false);
        await CrdtLoopbackPushVerifier.PushAcceptedAsync(
            sessionB,
            CreateOperationBatch(
                GCounterClientBBatchSeed,
                GCounterClientBOperationSeed,
                GCounterStream,
                FirstSequence,
                OlderClientTimestamp,
                CrdtMutation.GCounterSet(ClientBId, GCounterClientB),
                bounds),
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Pushes PN-counter operations in a different client order.</summary>
    /// <param name="sessionA">The first client session.</param>
    /// <param name="sessionB">The second client session.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The asynchronous operation.</returns>
    private static async ValueTask PushPNCounterOperationsAsync(
        IRemoteTransportSession sessionA,
        IRemoteTransportSession sessionB,
        CrdtBounds bounds,
        CancellationToken cancellationToken)
    {
        await CrdtLoopbackPushVerifier.PushAcceptedAsync(
            sessionB,
            CreateOperationBatch(
                PNCounterClientBBatchSeed,
                PNCounterClientBOperationSeed,
                PNCounterStream,
                FirstSequence,
                OlderClientTimestamp,
                CrdtMutation.PNCounterSet(ClientBId, PNCounterClientBPositive, PNCounterClientBNegative),
                bounds),
            cancellationToken).ConfigureAwait(false);
        await CrdtLoopbackPushVerifier.PushAcceptedAsync(
            sessionA,
            CreateOperationBatch(
                PNCounterClientABatchSeed,
                PNCounterClientAOperationSeed,
                PNCounterStream,
                FirstSequence,
                FutureClientTimestamp,
                CrdtMutation.PNCounterSet(ClientAId, PNCounterClientAPositive, PNCounterClientANegative),
                bounds),
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Pushes OR-set operations and returns the observed duplicate event delta.</summary>
    /// <param name="sessionA">The first client session.</param>
    /// <param name="sessionB">The second client session.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The event count delta after replaying the duplicate add.</returns>
    private static async ValueTask<int> PushORSetOperationsAsync(
        IRemoteTransportSession sessionA,
        IRemoteTransportSession sessionB,
        CrdtBounds bounds,
        CancellationToken cancellationToken)
    {
        var redAdd = CreateOperationBatch(
            ORSetRedAddBatchSeed,
            ORSetRedAddOperationSeed,
            ORSetStream,
            FirstSequence,
            OlderClientTimestamp,
            CrdtMutation.ORSetAdd(ToBytes(Red)),
            bounds);
        await CrdtLoopbackPushVerifier.PushAcceptedAsync(
            sessionA,
            CreateOperationBatch(
                ORSetBlueAddBatchSeed,
                ORSetBlueAddOperationSeed,
                ORSetStream,
                FirstSequence,
                FutureClientTimestamp,
                CrdtMutation.ORSetAdd(ToBytes(Blue)),
                bounds),
            cancellationToken).ConfigureAwait(false);
        await CrdtLoopbackPushVerifier.PushAcceptedAsync(sessionB, redAdd, cancellationToken).ConfigureAwait(false);

        var observed = await CrdtLoopbackReceiver.ReceiveStreamAsync(
            sessionA,
            ORSetStream,
            new(CreateGuid(ORSetObservedSubscriptionSeed)),
            bounds,
            cancellationToken).ConfigureAwait(false);
        var redDot = CrdtLoopbackORSetProjection.FindObservedDot(observed.States[^1], Red);
        await CrdtLoopbackPushVerifier.PushAcceptedAsync(
            sessionA,
            CreateOperationBatch(
                ORSetRedRemoveBatchSeed,
                ORSetRedRemoveOperationSeed,
                ORSetStream,
                SecondSequence,
                FutureClientTimestamp,
                CrdtMutation.ORSetRemove(ToBytes(Red), [redDot]),
                bounds),
            cancellationToken).ConfigureAwait(false);

        return await PushDuplicateORSetAddAsync(
            sessionA,
            sessionB,
            redAdd,
            bounds,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Pushes a duplicate OR-set add and gets its effect delta.</summary>
    /// <param name="sessionA">The first client session.</param>
    /// <param name="sessionB">The second client session.</param>
    /// <param name="redAdd">The original red add operation batch.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The duplicate effect delta.</returns>
    private static async ValueTask<int> PushDuplicateORSetAddAsync(
        IRemoteTransportSession sessionA,
        IRemoteTransportSession sessionB,
        SyncBatch redAdd,
        CrdtBounds bounds,
        CancellationToken cancellationToken)
    {
        var beforeDuplicate = await CrdtLoopbackReceiver.ReceiveStreamAsync(
            sessionB,
            ORSetStream,
            new(CreateGuid(ORSetBeforeDuplicateSubscriptionSeed)),
            bounds,
            cancellationToken).ConfigureAwait(false);
        await CrdtLoopbackPushVerifier.PushAcceptedAsync(sessionB, redAdd, cancellationToken).ConfigureAwait(false);
        var afterDuplicate = await CrdtLoopbackReceiver.ReceiveStreamAsync(
            sessionA,
            ORSetStream,
            new(CreateGuid(ORSetAfterDuplicateSubscriptionSeed)),
            bounds,
            cancellationToken).ConfigureAwait(false);
        return CrdtLoopbackDuplicateEffectVerifier.GetEffectDelta(
            beforeDuplicate,
            afterDuplicate,
            ExpectedDuplicateEffectDelta);
    }

    /// <summary>Pushes LWW operations and an exact older retry after the later server stamp wins.</summary>
    /// <param name="sessionA">The first client session.</param>
    /// <param name="sessionB">The second client session.</param>
    /// <param name="clock">The deterministic server clock.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The asynchronous operation.</returns>
    private static async ValueTask PushLwwOperationsAsync(
        IRemoteTransportSession sessionA,
        IRemoteTransportSession sessionB,
        DeterministicTimeProvider clock,
        CrdtBounds bounds,
        CancellationToken cancellationToken)
    {
        var first = CreateOperationBatch(
            LwwFirstBatchSeed,
            LwwFirstOperationSeed,
            LwwStream,
            FirstSequence,
            FutureClientTimestamp,
            CrdtMutation.LwwRegisterSet(ToBytes(First)),
            bounds);
        await CrdtLoopbackPushVerifier.PushAcceptedAsync(sessionA, first, cancellationToken).ConfigureAwait(false);
        clock.Advance();
        await CrdtLoopbackPushVerifier.PushAcceptedAsync(
            sessionB,
            CreateOperationBatch(
                LwwSecondBatchSeed,
                LwwSecondOperationSeed,
                LwwStream,
                FirstSequence,
                OlderClientTimestamp,
                CrdtMutation.LwwRegisterSet(ToBytes(Second)),
                bounds),
            cancellationToken).ConfigureAwait(false);
        clock.Advance();
        await CrdtLoopbackPushVerifier.PushAcceptedAsync(sessionA, first, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Creates the client A ACK probe descriptor.</summary>
    /// <returns>The ACK probe descriptor.</returns>
    private static AckProbeDescriptor CreateClientAAckProbe() =>
        new()
        {
            StreamId = AckProbeClientAStream,
            ClientId = ClientAId,
            SubscriptionSeed = AckProbeClientASubscriptionSeed,
            FirstBatchSeed = AckProbeClientAFirstBatchSeed,
            SecondBatchSeed = AckProbeClientASecondBatchSeed,
            FirstOperationSeed = AckProbeClientAFirstOperationSeed,
            SecondOperationSeed = AckProbeClientASecondOperationSeed,
        };

    /// <summary>Creates the client B ACK probe descriptor.</summary>
    /// <returns>The ACK probe descriptor.</returns>
    private static AckProbeDescriptor CreateClientBAckProbe() =>
        new()
        {
            StreamId = AckProbeClientBStream,
            ClientId = ClientBId,
            SubscriptionSeed = AckProbeClientBSubscriptionSeed,
            FirstBatchSeed = AckProbeClientBFirstBatchSeed,
            SecondBatchSeed = AckProbeClientBSecondBatchSeed,
            FirstOperationSeed = AckProbeClientBFirstOperationSeed,
            SecondOperationSeed = AckProbeClientBSecondOperationSeed,
        };

    /// <summary>Proves a same-subscription ACK resumes at the acknowledged cursor.</summary>
    /// <param name="session">The client session.</param>
    /// <param name="probe">The ACK probe descriptor.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>Whether the ACK resume proof passed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ValueTask<bool> ProveResumeAfterAckAsync(
        IRemoteTransportSession session,
        AckProbeDescriptor probe,
        CrdtBounds bounds,
        CancellationToken cancellationToken) =>
        CrdtLoopbackAckProbeWorkflow.ProveResumeAfterAckAsync(
            session,
            CreateAckProbePlan(probe, bounds),
            bounds,
            cancellationToken);

    /// <summary>Creates the deterministic ACK probe workflow plan.</summary>
    /// <param name="probe">The ACK probe descriptor.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The ACK probe workflow plan.</returns>
    private static CrdtLoopbackAckProbePlan CreateAckProbePlan(AckProbeDescriptor probe, CrdtBounds bounds) =>
        new()
        {
            StreamId = probe.StreamId,
            SubscriptionId = new(CreateGuid(probe.SubscriptionSeed)),
            FirstBatch = CreateOperationBatch(
                probe.FirstBatchSeed,
                probe.FirstOperationSeed,
                probe.StreamId,
                FirstSequence,
                OlderClientTimestamp,
                CrdtMutation.GCounterSet(probe.ClientId, AckProbeInitialCounter),
                bounds),
            SecondBatch = CreateOperationBatch(
                probe.SecondBatchSeed,
                probe.SecondOperationSeed,
                probe.StreamId,
                SecondSequence,
                OlderClientTimestamp,
                CrdtMutation.GCounterSet(probe.ClientId, AckProbeNextCounter),
                bounds),
            FirstValue = AckProbeInitialCounter,
            SecondValue = AckProbeNextCounter,
            ExpectedEventCount = ExpectedAckResumeEventCount,
            RewindDiagnosticFragment = RewindDiagnosticFragment,
        };
}
