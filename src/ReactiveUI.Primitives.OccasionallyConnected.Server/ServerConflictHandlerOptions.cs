// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Configures the conflict-resolving server operation handler.</summary>
[System.Diagnostics.DebuggerDisplay("{Streams.Count}")]
public sealed record ServerConflictHandlerOptions
{
    /// <summary>The default maximum resolver and domain event proposals accepted for one operation.</summary>
    private const int DefaultMaximumProducedEvents = 512;

    /// <summary>Gets the registered streams this handler may process.</summary>
    public required IReadOnlyList<ServerConflictStreamRegistration> Streams
    {
        get;
        init => field = ServerCollectionCopy.List(value, nameof(Streams));
    }

    /// <summary>Gets the maximum resolver and domain event proposal count accepted for one operation.</summary>
    public int MaximumProducedEvents { get; init; } = DefaultMaximumProducedEvents;

    /// <summary>Validates the configured handler options.</summary>
    /// <exception cref="ArgumentOutOfRangeException">No streams are configured.</exception>
    /// <exception cref="ArgumentNullException">A stream registration is missing.</exception>
    /// <exception cref="ArgumentException">A stream registration is malformed.</exception>
    /// <exception cref="InvalidOperationException">A stream is registered more than once.</exception>
    public void Validate()
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumProducedEvents);
        if (Streams.Count == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(Streams), Streams.Count, "At least one stream registration is required.");
        }

        var streams = new HashSet<StreamId>();
        for (var index = 0; index < Streams.Count; index++)
        {
            var registration = Streams[index];
            ArgumentExceptionHelper.ThrowIfNull(registration, nameof(Streams));
            registration.Validate();
            if (streams.Add(registration.StreamId))
            {
                continue;
            }

            throw new InvalidOperationException("A stream can only be registered once.");
        }
    }
}
