// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Specifies the semantic kind of a client-originated synchronization operation.</summary>
public enum SyncOperationType
{
    /// <summary>Appends a new value to the stream.</summary>
    Append = 0,

    /// <summary>Updates existing stream state.</summary>
    Update = 1,

    /// <summary>Deletes existing stream state.</summary>
    Delete = 2,

    /// <summary>Represents a custom domain mutation.</summary>
    Custom = 3,
}
