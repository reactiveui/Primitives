// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Describes a fully prepared server commit attempt.</summary>
internal sealed class ServerCommitPlan
{
    /// <summary>The finite absolute prepared entry capture bound.</summary>
    private const int MaximumPreparedEntries = 512;

    /// <summary>The prepared new terminal entries.</summary>
    private readonly ReadOnlyCollection<ServerLedgerEntry> _entries;

    /// <summary>Initializes a new instance of the <see cref="ServerCommitPlan"/> class.</summary>
    /// <param name="streamKey">The authenticated stream key.</param>
    /// <param name="expectedRevision">The stream revision observed before preparing effects.</param>
    /// <param name="newState">The optional new canonical server state.</param>
    /// <param name="newWriteStamp">The optional new last-write stamp.</param>
    /// <param name="entries">The complete new terminal entries.</param>
    internal ServerCommitPlan(
        ServerStreamKey streamKey,
        long expectedRevision,
        ServerState? newState,
        ServerWriteStamp? newWriteStamp,
        IReadOnlyList<ServerLedgerEntry> entries)
    {
        ArgumentExceptionHelper.ThrowIfNull(entries);
        StreamKey = streamKey;
        ExpectedRevision = expectedRevision;
        NewState = newState;
        NewWriteStamp = newWriteStamp;
        _entries = Copy(entries);
    }

    /// <summary>Gets the authenticated stream key.</summary>
    internal ServerStreamKey StreamKey { get; }

    /// <summary>Gets the stream revision observed before preparing effects.</summary>
    internal long ExpectedRevision { get; }

    /// <summary>Gets the optional new canonical server state.</summary>
    internal ServerState? NewState { get; }

    /// <summary>Gets the optional new last-write stamp.</summary>
    internal ServerWriteStamp? NewWriteStamp { get; }

    /// <summary>Gets the complete new terminal entries.</summary>
    internal IReadOnlyList<ServerLedgerEntry> Entries => _entries;

    /// <summary>Copies a list while preserving item identity.</summary>
    /// <param name="source">The source list.</param>
    /// <returns>The owned array.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The source contains too many entries.</exception>
    private static ReadOnlyCollection<ServerLedgerEntry> Copy(IReadOnlyList<ServerLedgerEntry> source)
    {
        var count = source.Count;
        if (count is < 0 or > MaximumPreparedEntries)
        {
            throw new ArgumentOutOfRangeException(nameof(source), count, "The terminal entry count is outside the supported bounds.");
        }

        var copy = new ServerLedgerEntry[count];
        for (var index = 0; index < count; index++)
        {
            copy[index] = source[index];
        }

        return Array.AsReadOnly(copy);
    }
}
