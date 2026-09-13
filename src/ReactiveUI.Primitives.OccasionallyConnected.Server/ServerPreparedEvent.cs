// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Describes a prepared domain event before server cursor and origin stamping.</summary>
internal sealed class ServerPreparedEvent
{
    /// <summary>The copied event metadata.</summary>
    private readonly ReadOnlyDictionary<string, string> _metadata;

    /// <summary>Initializes a new instance of the <see cref="ServerPreparedEvent"/> class.</summary>
    /// <param name="eventId">The domain event identifier.</param>
    /// <param name="payload">The event payload.</param>
    /// <param name="metadata">The event metadata.</param>
    internal ServerPreparedEvent(Guid eventId, PayloadEnvelope payload, IReadOnlyDictionary<string, string> metadata)
    {
        ArgumentExceptionHelper.ThrowIfNull(payload);
        ArgumentExceptionHelper.ThrowIfNull(metadata);
        EventId = eventId;
        Payload = payload;
        _metadata = Copy(metadata);
    }

    /// <summary>Gets the domain event identifier.</summary>
    internal Guid EventId { get; }

    /// <summary>Gets the event payload.</summary>
    internal PayloadEnvelope Payload { get; }

    /// <summary>Gets the event metadata.</summary>
    internal IReadOnlyDictionary<string, string> Metadata => _metadata;

    /// <summary>Copies metadata using ordinal keys.</summary>
    /// <param name="metadata">The metadata to copy.</param>
    /// <returns>The immutable metadata copy.</returns>
    private static ReadOnlyDictionary<string, string> Copy(IReadOnlyDictionary<string, string> metadata)
    {
        var copy = new Dictionary<string, string>(metadata.Count, StringComparer.Ordinal);
        foreach (var item in metadata)
        {
            copy.Add(item.Key, item.Value);
        }

        return new(copy);
    }
}
