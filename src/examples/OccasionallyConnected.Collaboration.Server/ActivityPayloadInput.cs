// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

/// <summary>Represents a client-authored activity payload before server canonicalization.</summary>
internal sealed record ActivityPayloadInput
{
    /// <summary>Gets the activity status text.</summary>
    public string Status { get; init; } = string.Empty;

    /// <summary>Gets the optional activity title.</summary>
    public string? Title { get; init; }

    /// <summary>Gets whether the client payload included the title property.</summary>
    public bool TitleSpecified { get; init; }

    /// <summary>Gets optional activity details.</summary>
    public string? Details { get; init; }

    /// <summary>Gets whether the client payload included the details property.</summary>
    public bool DetailsSpecified { get; init; }
}
