// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Configures bounded SQLite local store adapter execution.</summary>
/// <remarks>Worker and pre-worker capture stages use separate counters with the same configured count and logical byte limits.</remarks>
[System.Diagnostics.DebuggerDisplay("WorkerCapacity = {WorkerCapacity}, WorkerCapacityBytes = {WorkerCapacityBytes}")]
public sealed record SqliteLocalStoreAdapterOptions
{
    /// <summary>The default maximum admitted SQLite commands, including the active command.</summary>
    private const int DefaultWorkerCapacity = 1024;

    /// <summary>The default maximum retained caller input bytes admitted to the SQLite worker.</summary>
    private const long DefaultWorkerCapacityBytes = 64L * 1024L * 1024L;

    /// <summary>Gets the maximum admitted SQLite worker commands and pre-worker input captures.</summary>
    public int WorkerCapacity { get; init; } = DefaultWorkerCapacity;

    /// <summary>Gets the maximum logical retained caller input bytes admitted independently to the SQLite worker and capture stages.</summary>
    public long WorkerCapacityBytes { get; init; } = DefaultWorkerCapacityBytes;

    /// <summary>Gets the retention policy used when compacting terminal and reconstructable rows.</summary>
    public RetentionOptions Retention { get; init; } = new();

    /// <summary>Gets the clock used for SQLite commit, lease, retry, and compaction timestamps.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    /// <summary>Validates the configured adapter options.</summary>
    /// <exception cref="ArgumentNullException">A required option object is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A capacity or retention interval is not positive.</exception>
    internal void Validate()
    {
        ArgumentExceptionHelper.ThrowIfNull(Retention);
        ArgumentExceptionHelper.ThrowIfNull(TimeProvider);
        if (WorkerCapacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(WorkerCapacity), WorkerCapacity, "WorkerCapacity must be positive.");
        }

        if (WorkerCapacityBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(WorkerCapacityBytes), WorkerCapacityBytes, "WorkerCapacityBytes must be positive.");
        }

        Retention.Validate();
    }
}
