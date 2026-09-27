// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

/// <summary>Represents a command that watches activity view changes.</summary>
internal sealed class WatchCollaborationClientCommand : CollaborationClientCommand
{
    /// <inheritdoc/>
    public override CollaborationClientOptions SessionOptions => Options;
}
