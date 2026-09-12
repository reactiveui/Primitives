// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Contains inbox retention operations for the in-memory store.</summary>
internal sealed partial class InMemoryLocalStoreAdapter
{
    /// <summary>Selects expired inbox keys without changing retained state.</summary>
    /// <param name="request">The stream selection.</param>
    /// <param name="nowUtc">The sampled current timestamp.</param>
    /// <returns>The expired keys from streams without unresolved intent.</returns>
    private List<InboxKey> GetExpiredInboxKeys(CompactionRequest request, DateTimeOffset nowUtc)
    {
        HashSet<StreamId> protectedStreams = [];
        foreach (var pair in _operations)
        {
            if (!IsDefinitiveTerminal(pair.Value.Status.State))
            {
                _ = protectedStreams.Add(pair.Value.Operation.StreamId);
            }
        }

        List<InboxKey> expired = [];
        foreach (var pair in _inbox)
        {
            var matchesStream = !request.StreamId.HasValue || pair.Key.StreamId == request.StreamId.Value;
            if (matchesStream && !protectedStreams.Contains(pair.Key.StreamId)
                && nowUtc - pair.Value > _retentionOptions.InboxDeduplicationRetention)
            {
                expired.Add(pair.Key);
            }
        }

        return expired;
    }

    /// <summary>Removes preselected expired inbox entries and releases retained capacity.</summary>
    /// <param name="expired">The keys selected under the store gate.</param>
    /// <returns>The removed records and deterministic encoded bytes.</returns>
    private CompactionResult RemoveExpiredInboxEntries(List<InboxKey> expired)
    {
        var reclaimed = 0L;
        for (var index = 0; index < expired.Count; index++)
        {
            var key = expired[index];
            var capacity = InboxKeyCapacity(key);
            _ = _inbox.Remove(key);
            ApplyCapacity(new(-capacity.Records, -capacity.EncodedBytes));
            reclaimed += capacity.EncodedBytes;
        }

        return new(expired.Count, reclaimed);
    }
}
