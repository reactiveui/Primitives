// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies why a durable store could not write to its storage medium.</summary>
public enum DurableStorageFailure
{
    /// <summary>The storage medium reported a failure that has no more specific kind.</summary>
    Unknown = 0,

    /// <summary>The storage medium or the configured database size limit is full.</summary>
    StorageFull = 1,

    /// <summary>The storage medium reported a read, write, or synchronization I/O error.</summary>
    InputOutput = 2,
}
