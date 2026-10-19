// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

/// <summary>Represents one bounded activity patch published by the client.</summary>
internal sealed record ActivityUpdate
{
    /// <summary>Gets the required activity status.</summary>
    public required string Status { get; init; }

    /// <summary>Gets the optional activity title patch.</summary>
    public string? Title { get; init; }

    /// <summary>Gets a value indicating whether the title field was present in the patch.</summary>
    public bool TitleSpecified { get; init; }

    /// <summary>Gets the optional activity details patch.</summary>
    public string? Details { get; init; }

    /// <summary>Gets a value indicating whether the details field was present in the patch.</summary>
    public bool DetailsSpecified { get; init; }

    /// <summary>Gets the accepted client identifier when this update was decoded from a canonical server event.</summary>
    public string? AcceptedClientId { get; init; }

    /// <summary>Gets the accepted operation identifier when this update was decoded from a canonical server event.</summary>
    public string? AcceptedOperationId { get; init; }

    /// <summary>Gets the accepted server version when this update was decoded from a canonical server event.</summary>
    public string? AcceptedVersion { get; init; }

    /// <summary>Gets the server acceptance timestamp when this update was decoded from a canonical server event.</summary>
    public string? ServerAcceptedUtc { get; init; }
}
