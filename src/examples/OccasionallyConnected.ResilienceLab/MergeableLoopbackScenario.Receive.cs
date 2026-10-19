// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Runs the bounded in-memory CRDT loopback demonstration.</summary>
internal static partial class MergeableLoopbackScenario
{
    /// <summary>Receives all authoritative stream states for one client and ACKs each cursor.</summary>
    /// <param name="session">The client session.</param>
    /// <param name="seedBase">The deterministic subscription id seed base.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The received final states.</returns>
    private static async ValueTask<MergeableLoopbackReceivedStates> ReceiveAllStreamsAsync(
        IRemoteTransportSession session,
        int seedBase,
        MergeableBounds bounds,
        CancellationToken cancellationToken)
    {
        var gcounter = await MergeableLoopbackReceiver.ReceiveStreamAsync(
            session,
            IncrementOnlyCounterStream,
            new(CreateGuid(seedBase)),
            bounds,
            cancellationToken).ConfigureAwait(false);
        var pncounter = await MergeableLoopbackReceiver.ReceiveStreamAsync(
            session,
            CounterStream,
            new(CreateGuid(seedBase + CounterReceiveSeedOffset)),
            bounds,
            cancellationToken).ConfigureAwait(false);
        var orset = await MergeableLoopbackReceiver.ReceiveStreamAsync(
            session,
            AddWinsSetStream,
            new(CreateGuid(seedBase + AddWinsSetReceiveSeedOffset)),
            bounds,
            cancellationToken).ConfigureAwait(false);
        var lww = await MergeableLoopbackReceiver.ReceiveStreamAsync(
            session,
            LwwStream,
            new(CreateGuid(seedBase + LwwReceiveSeedOffset)),
            bounds,
            cancellationToken).ConfigureAwait(false);
        return new(gcounter, pncounter, orset, lww);
    }
}
