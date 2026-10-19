// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="MergeableInitialStateFactory"/>.</summary>
public sealed class MergeableInitialStateFactoryTests
{
    /// <summary>The invalid CRDT kind backing value.</summary>
    private const int InvalidKindValue = -1;

    /// <summary>The default initial version.</summary>
    private const string InitialVersion = "crdt-v0";

    /// <summary>The stream.</summary>
    private static readonly StreamId Stream = new("crdt/initial");

    /// <summary>Verifies default initial state factory output composes with the default sequential version prefix.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateInitialStateAsyncUsesDefaultMergeableVersionAndStatePayload()
    {
        var factory = new MergeableInitialStateFactory(new() { Kind = MergeableKind.IncrementOnlyCounter });

        var result = await factory.CreateInitialStateAsync(Stream, CancellationToken.None);
        var state = MergeableCodec.DecodeState(result.State.Payload);

        await Assert.That(result.StreamId).IsEqualTo(Stream);
        await Assert.That(result.Version).IsEqualTo(InitialVersion);
        await Assert.That(result.State.ContentType).IsEqualTo(MergeableServerPayloads.ContentType);
        await Assert.That(state.Kind).IsEqualTo(MergeableKind.IncrementOnlyCounter);
    }

    /// <summary>Verifies invalid initial state kind configuration is rejected during construction.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConstructorRejectsInvalidKind()
    {
        static void Act() => _ = new MergeableInitialStateFactory(new() { Kind = (MergeableKind)InvalidKindValue });

        await Assert.That(Act).ThrowsExactly<ArgumentOutOfRangeException>();
    }
}
