// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

/// <summary>Represents a command that publishes one activity update.</summary>
internal sealed class PublishCollaborationClientCommand : CollaborationClientCommand
{
    /// <summary>Gets the activity update to publish.</summary>
    public required ActivityUpdate Update { get; init; }

    /// <summary>Gets the session options; publish opens the session offline and starts sync itself.</summary>
    public override CollaborationClientOptions SessionOptions => Options with { AutoStart = false };
}
