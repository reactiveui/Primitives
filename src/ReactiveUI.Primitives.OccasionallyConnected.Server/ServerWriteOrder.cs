// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Orders server-owned write stamps for last-writer-wins conflict resolution.</summary>
internal static class ServerWriteOrder
{
    /// <summary>Compares server commit time, ordinal client identity, and operation identity in that order.</summary>
    /// <param name="candidate">The proposed write stamp.</param>
    /// <param name="current">The previously committed write stamp.</param>
    /// <returns>Whether the proposed stamp strictly follows the current stamp.</returns>
    internal static bool IsNewer(in ServerWriteStamp candidate, in ServerWriteStamp current)
    {
        var timeOrder = candidate.CommittedAtUtc.CompareTo(current.CommittedAtUtc);
        if (timeOrder != 0)
        {
            return timeOrder > 0;
        }

        var clientOrder = StringComparer.Ordinal.Compare(candidate.ClientId, current.ClientId);
        return clientOrder != 0 ? clientOrder > 0 : candidate.OperationId.Value.CompareTo(current.OperationId.Value) > 0;
    }
}
