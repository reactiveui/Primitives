// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Runs the public transport workflow that proves same-subscription ACK resume behavior.</summary>
internal static class CrdtLoopbackAckProbeWorkflow
{
    /// <summary>Proves a same-subscription ACK resumes at the acknowledged cursor.</summary>
    /// <param name="session">The client transport session.</param>
    /// <param name="plan">The deterministic ACK probe plan.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>Whether the ACK resume proof passed.</returns>
    internal static async ValueTask<bool> ProveResumeAfterAckAsync(
        IRemoteTransportSession session,
        CrdtLoopbackAckProbePlan plan,
        CrdtBounds bounds,
        CancellationToken cancellationToken)
    {
        await CrdtLoopbackPushVerifier.PushAcceptedAsync(session, plan.FirstBatch, cancellationToken).ConfigureAwait(false);
        var first = await CrdtLoopbackReceiver.ReceiveStreamAsync(
            session,
            plan.StreamId,
            plan.SubscriptionId,
            bounds,
            cancellationToken).ConfigureAwait(false);
        if (!CrdtLoopbackAckProbeVerifier.IsExpectedAckProbePage(first, plan.FirstValue, plan.ExpectedEventCount))
        {
            return false;
        }

        if (!await CrdtLoopbackAckProbeVerifier.IsStaleInitialReadRejectedAsync(
                () => CrdtLoopbackReceiver.ReceiveStreamAsync(
                    session,
                    plan.StreamId,
                    plan.SubscriptionId,
                    bounds,
                    cancellationToken),
                plan.RewindDiagnosticFragment).ConfigureAwait(false))
        {
            return false;
        }

        await CrdtLoopbackPushVerifier.PushAcceptedAsync(session, plan.SecondBatch, cancellationToken).ConfigureAwait(false);
        var second = await CrdtLoopbackReceiver.ReceiveStreamFromCursorAsync(
            session,
            plan.StreamId,
            plan.SubscriptionId,
            first.Cursor,
            bounds,
            cancellationToken).ConfigureAwait(false);
        return CrdtLoopbackAckProbeVerifier.IsExpectedResumePage(
            first,
            second,
            plan.SecondValue,
            plan.ExpectedEventCount);
    }
}
