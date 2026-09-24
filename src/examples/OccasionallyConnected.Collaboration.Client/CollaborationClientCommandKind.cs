// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

/// <summary>Defines collaboration client command kinds.</summary>
internal enum CollaborationClientCommandKind
{
    /// <summary>Publishes one activity update.</summary>
    Publish = 0,

    /// <summary>Watches activity view changes.</summary>
    Watch = 1,
}
