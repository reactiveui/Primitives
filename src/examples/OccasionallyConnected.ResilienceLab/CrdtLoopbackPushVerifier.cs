// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Pushes CRDT loopback batches and verifies terminal accepted results.</summary>
internal static class CrdtLoopbackPushVerifier
{
    /// <summary>Pushes a batch and verifies that every operation reached a terminal accepted result.</summary>
    /// <param name="session">The transport session.</param>
    /// <param name="batch">The operation batch.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The remote sync result.</returns>
    /// <exception cref="InvalidOperationException">The server did not accept the batch.</exception>
    internal static async ValueTask<RemoteSyncResult> PushAcceptedAsync(
        IRemoteTransportSession session,
        SyncBatch batch,
        CancellationToken cancellationToken)
    {
        var result = await session.PushAsync(batch, cancellationToken).ConfigureAwait(false);
        VerifyAccepted(batch, result);
        return result;
    }

    /// <summary>Verifies that a remote sync result accepted every operation in the pushed batch.</summary>
    /// <param name="batch">The pushed batch.</param>
    /// <param name="result">The remote result.</param>
    /// <exception cref="InvalidOperationException">The server did not accept the batch.</exception>
    internal static void VerifyAccepted(SyncBatch batch, RemoteSyncResult result)
    {
        if (result.Operations.Count != batch.Operations.Count)
        {
            throw new InvalidOperationException(
                "The CRDT loopback push returned an unexpected operation result count.");
        }

        ValidateResultMembership(batch, result);
        for (var index = 0; index < result.Operations.Count; index++)
        {
            var operation = result.Operations[index];
            if (operation.Kind is OperationResultKind.Accepted or OperationResultKind.Conflict)
            {
                continue;
            }

            throw new InvalidOperationException($"The CRDT loopback push was not accepted: {operation.ReasonCode}");
        }
    }

    /// <summary>Verifies that the remote result exactly belongs to the pushed batch.</summary>
    /// <param name="batch">The pushed batch.</param>
    /// <param name="result">The remote result.</param>
    /// <exception cref="InvalidOperationException">The result did not match the pushed batch.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateResultMembership(SyncBatch batch, RemoteSyncResult result)
    {
        try
        {
            SyncBatchValidator.Validate(batch, result);
        }
        catch (SyncBatchValidationException exception)
        {
            throw new InvalidOperationException(
                "The CRDT loopback push returned operation results that do not match the pushed batch.",
                exception);
        }
    }
}
