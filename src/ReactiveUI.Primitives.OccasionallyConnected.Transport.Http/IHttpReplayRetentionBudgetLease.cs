// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Describes a retained-byte owner.</summary>
internal interface IHttpReplayRetentionBudgetLease : IDisposable
{
    /// <summary>Checks whether this lease belongs to the supplied budget.</summary>
    /// <param name="owner">The candidate owner.</param>
    /// <returns>Whether this lease belongs to the candidate owner.</returns>
    bool IsOwnedBy(HttpReplayRetentionBudget owner);

    /// <summary>Updates this lease reservation.</summary>
    /// <param name="newBytes">The replacement retained byte count.</param>
    void Update(long newBytes);
}
