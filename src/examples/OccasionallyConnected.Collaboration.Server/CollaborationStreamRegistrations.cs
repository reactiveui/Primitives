// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

/// <summary>Creates the stream registrations served by the collaboration server example.</summary>
public static class CollaborationStreamRegistrations
{
    /// <summary>The maximum CRDT counter component count accepted by the example.</summary>
    private const int MaximumCounterComponents = 32;

    /// <summary>The maximum CRDT dot binding count accepted by the example.</summary>
    private const int MaximumDotBindings = 256;

    /// <summary>The maximum CRDT tombstone count accepted by the example.</summary>
    private const int MaximumTombstones = 256;

    /// <summary>The maximum CRDT element count accepted by the example.</summary>
    private const int MaximumElements = 128;

    /// <summary>The maximum CRDT element byte count accepted by the example.</summary>
    private const int MaximumElementBytes = 256;

    /// <summary>The maximum LWW register byte count accepted by the example.</summary>
    private const int MaximumRegisterBytes = 4096;

    /// <summary>The maximum encoded CRDT byte count accepted by the example.</summary>
    private const int MaximumEncodedBytes = 16 * 1024;

    /// <summary>Gets the custom activity stream identifier.</summary>
    public static StreamId ActivityStream { get; } = new("collaboration/activity");

    /// <summary>Gets the grow-only counter stream identifier.</summary>
    public static StreamId GCounterStream { get; } = new("collaboration/crdt/g-counter");

    /// <summary>Gets the positive-negative counter stream identifier.</summary>
    public static StreamId PNCounterStream { get; } = new("collaboration/crdt/pn-counter");

    /// <summary>Gets the observed-remove set stream identifier.</summary>
    public static StreamId ORSetStream { get; } = new("collaboration/crdt/or-set");

    /// <summary>Gets the last-writer-wins register stream identifier.</summary>
    public static StreamId LwwRegisterStream { get; } = new("collaboration/crdt/lww-register");

    /// <summary>Creates all public stream registrations demonstrated by the server example.</summary>
    /// <returns>The configured stream registrations.</returns>
    public static IReadOnlyList<ServerConflictStreamRegistration> CreateAll() =>
    [
        CreateActivityRegistration(),
        CreateCrdtRegistration(GCounterStream, CrdtKind.GCounter),
        CreateCrdtRegistration(PNCounterStream, CrdtKind.PNCounter),
        CreateCrdtRegistration(ORSetStream, CrdtKind.ORSet),
        CreateCrdtRegistration(LwwRegisterStream, CrdtKind.LwwRegister),
    ];

    /// <summary>Creates the custom activity stream registration.</summary>
    /// <returns>The custom activity stream registration.</returns>
    private static ServerConflictStreamRegistration CreateActivityRegistration()
    {
        var lastWriterWinsResolver = new LastWriterWinsResolver(new() { VersionFactory = new ActivityVersionFactory() });
        return new()
        {
            StreamId = ActivityStream,
            InitialStateFactory = new ActivityInitialStateFactory(),
            LastWriterWinsResolver = lastWriterWinsResolver,
            MergeResolver = new ActivityConflictResolver(),
            CustomResolver = new ActivityConflictResolver(),
            DomainHandler = new ActivityDomainHandler(),
        };
    }

    /// <summary>Creates one CRDT stream registration.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="kind">The CRDT family.</param>
    /// <returns>The CRDT stream registration.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ServerConflictStreamRegistration CreateCrdtRegistration(StreamId streamId, CrdtKind kind) =>
        CrdtServerStreamRegistration.Create(new()
        {
            StreamId = streamId,
            Kind = kind,
            Bounds = new()
            {
                MaximumCounterComponents = MaximumCounterComponents,
                MaximumDotBindings = MaximumDotBindings,
                MaximumTombstones = MaximumTombstones,
                MaximumElements = MaximumElements,
                MaximumElementBytes = MaximumElementBytes,
                MaximumRegisterBytes = MaximumRegisterBytes,
                MaximumEncodedBytes = MaximumEncodedBytes,
            },
        });
}
