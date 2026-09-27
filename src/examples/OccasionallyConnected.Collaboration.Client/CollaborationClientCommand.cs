// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

/// <summary>Represents a parsed collaboration client command.</summary>
#if NET11_0_OR_GREATER
internal closed class CollaborationClientCommand
#else
internal abstract class CollaborationClientCommand
#endif
{
    /// <summary>Gets the client options.</summary>
    public required CollaborationClientOptions Options { get; init; }

    /// <summary>Gets the options used to open the client session for this command.</summary>
    public abstract CollaborationClientOptions SessionOptions { get; }
}
