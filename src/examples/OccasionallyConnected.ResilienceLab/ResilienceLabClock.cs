// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Provides a manual server clock that moves only when a scenario advances it.</summary>
/// <param name="utcNow">The initial current time.</param>
[DebuggerDisplay("{_utcNow,nq}")]
internal sealed class ResilienceLabClock(DateTimeOffset utcNow) : TimeProvider
{
    /// <summary>The current time.</summary>
    private DateTimeOffset _utcNow = utcNow;

    /// <inheritdoc/>
    public override DateTimeOffset GetUtcNow() => _utcNow;

    /// <summary>Moves the clock forward.</summary>
    /// <param name="delta">The positive amount to advance.</param>
    internal void Advance(TimeSpan delta) => _utcNow = _utcNow.Add(delta);
}
