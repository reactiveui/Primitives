// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

/// <summary>Represents a parsed collaboration client command.</summary>
internal sealed record CollaborationClientCommand
{
    /// <summary>Gets the command kind.</summary>
    public required CollaborationClientCommandKind Kind { get; init; }

    /// <summary>Gets the client options.</summary>
    public required CollaborationClientOptions Options { get; init; }

    /// <summary>Gets the activity update used by publish commands.</summary>
    public ActivityUpdate Update { get; init; } = new() { Status = "active" };
}
