// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Returns an atomic stream view and requested terminal operation replay entries.</summary>
internal sealed class ServerCommitSnapshot
{
    /// <summary>The requested retained ledger entries.</summary>
    private readonly ReadOnlyCollection<ServerLedgerEntry> _entries;

    /// <summary>Initializes a new instance of the <see cref="ServerCommitSnapshot"/> class.</summary>
    /// <param name="streamKey">The authenticated stream key.</param>
    /// <param name="revision">The stream revision.</param>
    /// <param name="state">The optional canonical server state.</param>
    /// <param name="lastWriteStamp">The optional last-write stamp.</param>
    /// <param name="entries">The complete requested replay entries.</param>
    /// <param name="lastCursor">The optional last server cursor.</param>
    /// <param name="lastEventSequence">The last sidecar event sequence.</param>
    internal ServerCommitSnapshot(
        ServerStreamKey streamKey,
        long revision,
        ServerState? state,
        ServerWriteStamp? lastWriteStamp,
        IReadOnlyList<ServerLedgerEntry> entries,
        string? lastCursor,
        long lastEventSequence)
        : this(new ServerCommitSnapshotOptions
        {
            StreamKey = streamKey,
            Revision = revision,
            State = state,
            LastWriteStamp = lastWriteStamp,
            Entries = entries,
            LastCursor = lastCursor,
            LastEventSequence = lastEventSequence,
            LastGroupSequence = 0,
        })
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ServerCommitSnapshot"/> class.</summary>
    /// <param name="options">The snapshot field composition.</param>
    internal ServerCommitSnapshot(ServerCommitSnapshotOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        ArgumentExceptionHelper.ThrowIfNull(options.Entries);
        StreamKey = options.StreamKey;
        Revision = options.Revision;
        State = options.State;
        LastWriteStamp = options.LastWriteStamp;
        _entries = Copy(options.Entries);
        LastCursor = options.LastCursor;
        LastEventSequence = options.LastEventSequence;
        LastGroupSequence = options.LastGroupSequence;
    }

    /// <summary>Gets the authenticated stream key.</summary>
    internal ServerStreamKey StreamKey { get; }

    /// <summary>Gets the stream revision.</summary>
    internal long Revision { get; }

    /// <summary>Gets the optional canonical server state.</summary>
    internal ServerState? State { get; }

    /// <summary>Gets the optional last-write stamp.</summary>
    internal ServerWriteStamp? LastWriteStamp { get; }

    /// <summary>Gets the complete requested replay entries.</summary>
    internal IReadOnlyList<ServerLedgerEntry> Entries => _entries;

    /// <summary>Gets the last retained server cursor.</summary>
    internal string? LastCursor { get; }

    /// <summary>Gets the last sidecar event sequence.</summary>
    internal long LastEventSequence { get; }

    /// <summary>Gets the complete durable receive group frontier.</summary>
    internal long LastGroupSequence { get; }

    /// <summary>Copies a list while preserving item identity.</summary>
    /// <param name="source">The source list.</param>
    /// <returns>The owned array.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The source exposes an invalid count.</exception>
    private static ReadOnlyCollection<ServerLedgerEntry> Copy(IReadOnlyList<ServerLedgerEntry> source)
    {
        var count = source.Count;
        if (count < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(source), count, "The snapshot entry count is outside the supported bounds.");
        }

        var copy = new ServerLedgerEntry[count];
        for (var index = 0; index < count; index++)
        {
            copy[index] = source[index];
        }

        return Array.AsReadOnly(copy);
    }
}
