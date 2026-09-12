// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Identifies a write using the server clock and authenticated client identity.</summary>
/// <param name="CommittedAtUtc">The server-owned logical commit timestamp.</param>
/// <param name="ClientId">The authenticated client identifier.</param>
/// <param name="OperationId">The operation identifier.</param>
internal readonly record struct ServerWriteStamp(DateTimeOffset CommittedAtUtc, string ClientId, OperationId OperationId);
