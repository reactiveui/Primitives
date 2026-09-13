// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Creates built-in CRDT server stream registrations.</summary>
public static class CrdtServerStreamRegistration
{
    /// <summary>Creates the CRDT server stream registration.</summary>
    /// <param name="options">The registration options.</param>
    /// <returns>The server stream registration.</returns>
    /// <exception cref="ArgumentException">The stream identifier is malformed.</exception>
    /// <exception cref="ArgumentNullException">A required option is missing.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The CRDT kind or bounds are invalid.</exception>
    public static ServerConflictStreamRegistration Create(CrdtServerStreamRegistrationOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        if (options.StreamId.Value is null)
        {
            throw new ArgumentException("A CRDT stream registration requires a stream identifier.", nameof(options));
        }

        ServerCommitJournalGuard.ValidateText(options.StreamId.Value, nameof(options));
        var resolver = new CrdtResolver(new() { Kind = options.Kind, Bounds = options.Bounds, VersionFactory = options.VersionFactory });
        return new()
        {
            StreamId = options.StreamId,
            InitialStateFactory = new CrdtInitialStateFactory(new() { Kind = options.Kind, Bounds = options.Bounds, InitialVersion = options.InitialVersion }),
            LastWriterWinsResolver = resolver,
            MergeResolver = resolver,
            CustomResolver = resolver,
            DomainHandler = new CrdtServerDomainHandler(new() { Kind = options.Kind, Bounds = options.Bounds }),
        };
    }
}
