// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Bounds transient inbox lookups independently of retained store records.</summary>
internal sealed partial class InMemoryLocalStoreAdapter
{
    /// <summary>The input and result buffers reserved by each query.</summary>
    private const long InboxLookupBufferCount = 2;

    /// <summary>The aggregate candidate and encoded-buffer reservations of active inbox lookups.</summary>
    private CapacityUsage _inboxLookupUsage;

    /// <summary>Captures each caller candidate once without holding the store gate.</summary>
    /// <param name="eventIds">The caller-supplied candidates.</param>
    /// <param name="count">The admitted candidate count.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The owned candidate snapshot.</returns>
    private static Guid[] CaptureInboxCandidates(IReadOnlyList<Guid> eventIds, int count, CancellationToken cancellationToken)
    {
        var candidates = new Guid[count];
        for (var index = 0; index < count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            candidates[index] = eventIds[index];
        }

        return candidates;
    }

    /// <summary>Reserves finite input and result capacity before allocating query buffers.</summary>
    /// <param name="count">The caller's advertised candidate count.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The reservation to release after lookup.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The candidate count is negative.</exception>
    /// <exception cref="QueueCapacityExceededException">The active queries exceed configured bounds.</exception>
    /// <remarks>
    /// Query capacity is a separate transient budget using the configured record and encoded-byte limits. It reserves
    /// both candidate and result GUID bytes plus their count fields. Empty queries reserve one record. These logical
    /// encoded-data counters do not measure managed heap bytes or retain ownership after returning a result.
    /// </remarks>
    private CapacityUsage ReserveInboxLookup(int count, CancellationToken cancellationToken)
    {
        if (count < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count), count, "Candidate count cannot be negative.");
        }

        var reservation = new CapacityUsage(Math.Max(1, count), (InboxLookupBufferCount * Int32EncodedBytes) + (InboxLookupBufferCount * GuidEncodedBytes * count));
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            if (reservation.Records <= _maximumRecordCount - _inboxLookupUsage.Records
                && reservation.EncodedBytes <= _maximumEncodedBytes - _inboxLookupUsage.EncodedBytes)
            {
                _inboxLookupUsage = new(_inboxLookupUsage.Records + reservation.Records, _inboxLookupUsage.EncodedBytes + reservation.EncodedBytes);
                return reservation;
            }

            var canFitWhenEmpty = reservation.Records <= _maximumRecordCount && reservation.EncodedBytes <= _maximumEncodedBytes;
            throw new QueueCapacityExceededException("The in-memory inbox lookup capacity would be exceeded.", canFitWhenEmpty);
        }
    }

    /// <summary>Releases a query reservation even when capture, validation, cancellation or lookup fails.</summary>
    /// <param name="reservation">The admitted query capacity.</param>
    private void ReleaseInboxLookup(CapacityUsage reservation)
    {
        lock (_gate)
        {
            _inboxLookupUsage = new(_inboxLookupUsage.Records - reservation.Records, _inboxLookupUsage.EncodedBytes - reservation.EncodedBytes);
        }
    }
}
