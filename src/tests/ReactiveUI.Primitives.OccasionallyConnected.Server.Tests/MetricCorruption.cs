// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Retained metric row corruption cases.</summary>
internal enum MetricCorruption
{
    /// <summary>A blank tenant identifier.</summary>
    BlankTenantId = 0,

    /// <summary>An invalid stream identifier.</summary>
    InvalidStreamId = 1,

    /// <summary>A negative state byte count.</summary>
    NegativeStateBytes = 2,

    /// <summary>A fractional real state byte count.</summary>
    FractionalStateBytes = 3,

    /// <summary>A nonnumeric text state byte count.</summary>
    TextStateBytes = 4,

    /// <summary>A negative cursor byte count.</summary>
    NegativeCursorBytes = 5,

    /// <summary>A negative ledger byte count.</summary>
    NegativeLedgerBytes = 6,
}
