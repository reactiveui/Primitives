// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Runs the bounded in-memory CRDT loopback demonstration.</summary>
internal static partial class MergeableLoopbackScenario
{
    /// <summary>Pushes increment-only counter operations and an exact duplicate replay.</summary>
    /// <param name="sessionA">The first client session.</param>
    /// <param name="sessionB">The second client session.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The asynchronous operation.</returns>
    private static async ValueTask PushIncrementOnlyCounterOperationsAsync(
        IRemoteTransportSession sessionA,
        IRemoteTransportSession sessionB,
        MergeableBounds bounds,
        CancellationToken cancellationToken)
    {
        var first = CreateOperationBatch(
            IncrementOnlyCounterClientABatchSeed,
            IncrementOnlyCounterClientAOperationSeed,
            IncrementOnlyCounterStream,
            FirstSequence,
            FutureClientTimestamp,
            MergeableChange.IncrementOnlyCounterSet(ClientAId, IncrementOnlyCounterClientA),
            bounds);
        await MergeableLoopbackPushVerifier.PushAcceptedAsync(sessionA, first, cancellationToken).ConfigureAwait(false);
        await MergeableLoopbackPushVerifier.PushAcceptedAsync(sessionA, first, cancellationToken).ConfigureAwait(false);
        await MergeableLoopbackPushVerifier.PushAcceptedAsync(
            sessionB,
            CreateOperationBatch(
                IncrementOnlyCounterClientBBatchSeed,
                IncrementOnlyCounterClientBOperationSeed,
                IncrementOnlyCounterStream,
                FirstSequence,
                OlderClientTimestamp,
                MergeableChange.IncrementOnlyCounterSet(ClientBId, IncrementOnlyCounterClientB),
                bounds),
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Pushes PN-counter operations in a different client order.</summary>
    /// <param name="sessionA">The first client session.</param>
    /// <param name="sessionB">The second client session.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The asynchronous operation.</returns>
    private static async ValueTask PushCounterOperationsAsync(
        IRemoteTransportSession sessionA,
        IRemoteTransportSession sessionB,
        MergeableBounds bounds,
        CancellationToken cancellationToken)
    {
        await MergeableLoopbackPushVerifier.PushAcceptedAsync(
            sessionB,
            CreateOperationBatch(
                CounterClientBBatchSeed,
                CounterClientBOperationSeed,
                CounterStream,
                FirstSequence,
                OlderClientTimestamp,
                MergeableChange.CounterSet(ClientBId, CounterClientBPositive, CounterClientBNegative),
                bounds),
            cancellationToken).ConfigureAwait(false);
        await MergeableLoopbackPushVerifier.PushAcceptedAsync(
            sessionA,
            CreateOperationBatch(
                CounterClientABatchSeed,
                CounterClientAOperationSeed,
                CounterStream,
                FirstSequence,
                FutureClientTimestamp,
                MergeableChange.CounterSet(ClientAId, CounterClientAPositive, CounterClientANegative),
                bounds),
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Pushes OR-set operations and returns the observed duplicate event delta.</summary>
    /// <param name="sessionA">The first client session.</param>
    /// <param name="sessionB">The second client session.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The event count delta after replaying the duplicate add.</returns>
    private static async ValueTask<int> PushAddWinsSetOperationsAsync(
        IRemoteTransportSession sessionA,
        IRemoteTransportSession sessionB,
        MergeableBounds bounds,
        CancellationToken cancellationToken)
    {
        var redAdd = CreateOperationBatch(
            AddWinsSetRedAddBatchSeed,
            AddWinsSetRedAddOperationSeed,
            AddWinsSetStream,
            FirstSequence,
            OlderClientTimestamp,
            MergeableChange.AddWinsSetAdd(ToBytes(Red)),
            bounds);
        await MergeableLoopbackPushVerifier.PushAcceptedAsync(
            sessionA,
            CreateOperationBatch(
                AddWinsSetBlueAddBatchSeed,
                AddWinsSetBlueAddOperationSeed,
                AddWinsSetStream,
                FirstSequence,
                FutureClientTimestamp,
                MergeableChange.AddWinsSetAdd(ToBytes(Blue)),
                bounds),
            cancellationToken).ConfigureAwait(false);
        await MergeableLoopbackPushVerifier.PushAcceptedAsync(sessionB, redAdd, cancellationToken).ConfigureAwait(false);

        var observed = await MergeableLoopbackReceiver.ReceiveStreamAsync(
            sessionA,
            AddWinsSetStream,
            new(CreateGuid(AddWinsSetObservedSubscriptionSeed)),
            bounds,
            cancellationToken).ConfigureAwait(false);
        var redDot = MergeableLoopbackAddWinsSetProjection.FindObservedDot(observed.States[^1], Red);
        await MergeableLoopbackPushVerifier.PushAcceptedAsync(
            sessionA,
            CreateOperationBatch(
                AddWinsSetRedRemoveBatchSeed,
                AddWinsSetRedRemoveOperationSeed,
                AddWinsSetStream,
                SecondSequence,
                FutureClientTimestamp,
                MergeableChange.AddWinsSetRemove(ToBytes(Red), [redDot]),
                bounds),
            cancellationToken).ConfigureAwait(false);

        return await PushDuplicateAddWinsSetAddAsync(
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
    private static async ValueTask<int> PushDuplicateAddWinsSetAddAsync(
        IRemoteTransportSession sessionA,
        IRemoteTransportSession sessionB,
        SyncBatch redAdd,
        MergeableBounds bounds,
        CancellationToken cancellationToken)
    {
        var beforeDuplicate = await MergeableLoopbackReceiver.ReceiveStreamAsync(
            sessionB,
            AddWinsSetStream,
            new(CreateGuid(AddWinsSetBeforeDuplicateSubscriptionSeed)),
            bounds,
            cancellationToken).ConfigureAwait(false);
        await MergeableLoopbackPushVerifier.PushAcceptedAsync(sessionB, redAdd, cancellationToken).ConfigureAwait(false);
        var afterDuplicate = await MergeableLoopbackReceiver.ReceiveStreamAsync(
            sessionA,
            AddWinsSetStream,
            new(CreateGuid(AddWinsSetAfterDuplicateSubscriptionSeed)),
            bounds,
            cancellationToken).ConfigureAwait(false);
        return MergeableLoopbackDuplicateEffectVerifier.GetEffectDelta(
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
        MergeableBounds bounds,
        CancellationToken cancellationToken)
    {
        var first = CreateOperationBatch(
            LwwFirstBatchSeed,
            LwwFirstOperationSeed,
            LwwStream,
            FirstSequence,
            FutureClientTimestamp,
            MergeableChange.LastWriterWinsValueSet(ToBytes(First)),
            bounds);
        await MergeableLoopbackPushVerifier.PushAcceptedAsync(sessionA, first, cancellationToken).ConfigureAwait(false);
        clock.Advance();
        await MergeableLoopbackPushVerifier.PushAcceptedAsync(
            sessionB,
            CreateOperationBatch(
                LwwSecondBatchSeed,
                LwwSecondOperationSeed,
                LwwStream,
                FirstSequence,
                OlderClientTimestamp,
                MergeableChange.LastWriterWinsValueSet(ToBytes(Second)),
                bounds),
            cancellationToken).ConfigureAwait(false);
        clock.Advance();
        await MergeableLoopbackPushVerifier.PushAcceptedAsync(sessionA, first, cancellationToken).ConfigureAwait(false);
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
        MergeableBounds bounds,
        CancellationToken cancellationToken) =>
        MergeableLoopbackAckProbeWorkflow.ProveResumeAfterAckAsync(
            session,
            CreateAckProbePlan(probe, bounds),
            bounds,
            cancellationToken);

    /// <summary>Creates the deterministic ACK probe workflow plan.</summary>
    /// <param name="probe">The ACK probe descriptor.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The ACK probe workflow plan.</returns>
    private static MergeableLoopbackAckProbePlan CreateAckProbePlan(AckProbeDescriptor probe, MergeableBounds bounds) =>
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
                MergeableChange.IncrementOnlyCounterSet(probe.ClientId, AckProbeInitialCounter),
                bounds),
            SecondBatch = CreateOperationBatch(
                probe.SecondBatchSeed,
                probe.SecondOperationSeed,
                probe.StreamId,
                SecondSequence,
                OlderClientTimestamp,
                MergeableChange.IncrementOnlyCounterSet(probe.ClientId, AckProbeNextCounter),
                bounds),
            FirstValue = AckProbeInitialCounter,
            SecondValue = AckProbeNextCounter,
            ExpectedEventCount = ExpectedAckResumeEventCount,
            RewindDiagnosticFragment = RewindDiagnosticFragment,
        };
}
