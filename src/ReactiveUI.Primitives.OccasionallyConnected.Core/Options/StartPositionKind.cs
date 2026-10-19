// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Specifies how a subscription begins consuming a stream.</summary>
public enum StartPositionKind
{
    /// <summary>Starts with events published after the subscription begins.</summary>
    Latest = 0,

    /// <summary>Starts with events published at or after a timestamp.</summary>
    FromTimestamp = 1,

    /// <summary>Starts with events at or after a server-assigned sequence.</summary>
    FromSequence = 2,

    /// <summary>Starts with events after a server-issued resume cursor.</summary>
    FromCursor = 3,
}
