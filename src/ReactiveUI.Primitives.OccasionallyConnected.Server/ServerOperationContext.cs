// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Provides immutable inputs for side-effect-free operation preparation.</summary>
internal sealed class ServerOperationContext
{
    /// <summary>Initializes a new instance of the <see cref="ServerOperationContext"/> class.</summary>
    /// <param name="client">The authenticated client identity.</param>
    /// <param name="operation">The authorized operation.</param>
    /// <param name="scope">The trusted server scope.</param>
    /// <param name="streamKey">The authenticated stream key.</param>
    /// <param name="operationKey">The authenticated operation key.</param>
    /// <param name="snapshot">The stream snapshot used for this preparation attempt.</param>
    /// <param name="candidateWrite">The server-owned logical write stamp captured for this attempt.</param>
    internal ServerOperationContext(
        ClientIdentity client,
        SyncOperation operation,
        ServerOperationScope scope,
        ServerStreamKey streamKey,
        ServerOperationKey operationKey,
        ServerCommitSnapshot snapshot,
        in ServerWriteStamp candidateWrite)
    {
        ArgumentExceptionHelper.ThrowIfNull(client);
        ArgumentExceptionHelper.ThrowIfNull(operation);
        ArgumentExceptionHelper.ThrowIfNull(snapshot);
        Client = client;
        Operation = operation;
        Scope = scope;
        StreamKey = streamKey;
        OperationKey = operationKey;
        Snapshot = snapshot;
        CandidateWrite = candidateWrite;
    }

    /// <summary>Gets the authenticated client identity.</summary>
    internal ClientIdentity Client { get; }

    /// <summary>Gets the authorized operation.</summary>
    internal SyncOperation Operation { get; }

    /// <summary>Gets the trusted server scope.</summary>
    internal ServerOperationScope Scope { get; }

    /// <summary>Gets the authenticated stream key.</summary>
    internal ServerStreamKey StreamKey { get; }

    /// <summary>Gets the authenticated operation key.</summary>
    internal ServerOperationKey OperationKey { get; }

    /// <summary>Gets the snapshot used for this preparation attempt.</summary>
    internal ServerCommitSnapshot Snapshot { get; }

    /// <summary>Gets the trusted write stamp shared by preparation and committed effects.</summary>
    internal ServerWriteStamp CandidateWrite { get; }
}
