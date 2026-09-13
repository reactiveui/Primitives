// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Authorizes a client against a target stream and operation before journal lookup.</summary>
internal interface IServerOperationAuthorizer
{
    /// <summary>Authorizes the operation and returns trusted server identities.</summary>
    /// <param name="client">The authenticated client identity supplied by the host.</param>
    /// <param name="operation">The candidate operation.</param>
    /// <returns>The trusted tenant and client scope for the operation.</returns>
    ServerOperationScope Authorize(ClientIdentity client, SyncOperation operation);
}
