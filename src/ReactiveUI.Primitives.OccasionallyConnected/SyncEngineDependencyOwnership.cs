// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes whether the internal synchronization engine owns a dependency lifetime.</summary>
internal enum SyncEngineDependencyOwnership
{
    /// <summary>The engine disposes the dependency during engine disposal.</summary>
    Owned = 0,

    /// <summary>The engine borrows the dependency and leaves its disposal to the caller.</summary>
    Borrowed = 1,
}
