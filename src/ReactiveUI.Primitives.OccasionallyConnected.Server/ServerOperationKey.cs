// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Identifies one operation within an authenticated client boundary.</summary>
/// <param name="ClientId">The authenticated client identifier supplied by the host.</param>
/// <param name="OperationId">The logical operation identifier.</param>
internal readonly record struct ServerOperationKey(string ClientId, OperationId OperationId);
