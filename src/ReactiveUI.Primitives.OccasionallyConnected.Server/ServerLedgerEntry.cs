// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Retains the complete terminal response for one authenticated operation.</summary>
internal sealed class ServerLedgerEntry
{
    /// <summary>The finite absolute prepared conflict capture bound.</summary>
    private const int MaximumPreparedConflicts = 512;

    /// <summary>The finite absolute prepared event capture bound.</summary>
    private const int MaximumPreparedEvents = 512;

    /// <summary>The retained resolved conflicts.</summary>
    private readonly ReadOnlyCollection<ResolvedConflict> _conflicts;

    /// <summary>The retained remote events produced for the operation.</summary>
    private readonly ReadOnlyCollection<RemoteEvent> _events;

    /// <summary>Initializes a new instance of the <see cref="ServerLedgerEntry"/> class.</summary>
    /// <param name="operationKey">The authenticated operation key.</param>
    /// <param name="fingerprint">The trusted canonical operation intent fingerprint.</param>
    /// <param name="result">The terminal operation result.</param>
    /// <param name="conflicts">The complete resolved conflicts for duplicate response replay.</param>
    /// <param name="events">The complete remote events for duplicate response replay.</param>
    internal ServerLedgerEntry(
        ServerOperationKey operationKey,
        ServerCommitFingerprint fingerprint,
        OperationSyncResult result,
        IReadOnlyList<ResolvedConflict> conflicts,
        IReadOnlyList<RemoteEvent> events)
    {
        ArgumentExceptionHelper.ThrowIfNull(fingerprint);
        ArgumentExceptionHelper.ThrowIfNull(result);
        ArgumentExceptionHelper.ThrowIfNull(conflicts);
        ArgumentExceptionHelper.ThrowIfNull(events);

        OperationKey = operationKey;
        Fingerprint = fingerprint;
        Result = result;
        _conflicts = Copy(conflicts, MaximumPreparedConflicts, nameof(conflicts));
        _events = Copy(events, MaximumPreparedEvents, nameof(events));
    }

    /// <summary>Initializes a new instance of the <see cref="ServerLedgerEntry"/> class.</summary>
    /// <param name="source">The prepared source entry.</param>
    /// <param name="committedAtUtc">The server-owned commit time.</param>
    /// <param name="expiresAtUtc">The inclusive retained replay expiry.</param>
    private ServerLedgerEntry(ServerLedgerEntry source, DateTimeOffset committedAtUtc, DateTimeOffset expiresAtUtc)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        OperationKey = source.OperationKey;
        Fingerprint = source.Fingerprint;
        Result = source.Result;
        _conflicts = source._conflicts;
        _events = source._events;
        CommittedAtUtc = committedAtUtc;
        ExpiresAtUtc = expiresAtUtc;
        IsCommitted = true;
    }

    /// <summary>Gets the authenticated operation key.</summary>
    internal ServerOperationKey OperationKey { get; }

    /// <summary>Gets the trusted canonical operation intent fingerprint.</summary>
    internal ServerCommitFingerprint Fingerprint { get; }

    /// <summary>Gets the original terminal operation result.</summary>
    internal OperationSyncResult Result { get; }

    /// <summary>Gets the complete original conflict resolutions.</summary>
    internal IReadOnlyList<ResolvedConflict> Conflicts => _conflicts;

    /// <summary>Gets the complete original remote events.</summary>
    internal IReadOnlyList<RemoteEvent> Events => _events;

    /// <summary>Gets the server-owned commit time.</summary>
    internal DateTimeOffset CommittedAtUtc { get; }

    /// <summary>Gets the inclusive replay expiry.</summary>
    internal DateTimeOffset ExpiresAtUtc { get; }

    /// <summary>Gets whether the entry has been committed to the journal.</summary>
    internal bool IsCommitted { get; }

    /// <summary>Creates a committed copy with journal-owned retention timestamps.</summary>
    /// <param name="committedAtUtc">The server-owned commit time.</param>
    /// <param name="expiresAtUtc">The inclusive retained replay expiry.</param>
    /// <returns>The committed ledger entry.</returns>
    internal ServerLedgerEntry Commit(DateTimeOffset committedAtUtc, DateTimeOffset expiresAtUtc) =>
        new(this, committedAtUtc, expiresAtUtc);

    /// <summary>Copies a list while preserving item identity.</summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="source">The source list.</param>
    /// <param name="maximumCount">The maximum count.</param>
    /// <param name="parameterName">The source parameter name.</param>
    /// <returns>The owned array.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The source contains too many items.</exception>
    private static ReadOnlyCollection<T> Copy<T>(IReadOnlyList<T> source, int maximumCount, string parameterName)
    {
        var count = source.Count;
        if (count < 0 || count > maximumCount)
        {
            throw new ArgumentOutOfRangeException(parameterName, count, "The item count is outside the supported bounds.");
        }

        var copy = new T[count];
        for (var index = 0; index < count; index++)
        {
            copy[index] = source[index];
        }

        return Array.AsReadOnly(copy);
    }
}
