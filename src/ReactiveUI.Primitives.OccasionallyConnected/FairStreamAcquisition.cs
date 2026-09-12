// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies an acquired stream head.</summary>
internal sealed class FairStreamAcquisition
{
    /// <summary>Initializes a new instance of the <see cref="FairStreamAcquisition"/> class.</summary>
    /// <param name="streamId">The selected stream identifier.</param>
    internal FairStreamAcquisition(StreamId streamId) => StreamId = streamId;

    /// <summary>Gets the selected stream identifier.</summary>
    internal StreamId StreamId { get; }
}
