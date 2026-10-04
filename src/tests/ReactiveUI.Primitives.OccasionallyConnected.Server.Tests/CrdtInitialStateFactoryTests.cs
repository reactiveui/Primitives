// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="CrdtInitialStateFactory"/>.</summary>
public sealed class CrdtInitialStateFactoryTests
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
    public async Task CreateInitialStateAsyncUsesDefaultCrdtVersionAndStatePayload()
    {
        var factory = new CrdtInitialStateFactory(new() { Kind = CrdtKind.GCounter });

        var result = await factory.CreateInitialStateAsync(Stream, CancellationToken.None);
        var state = CrdtCodec.DecodeState(result.State.Payload);

        await Assert.That(result.StreamId).IsEqualTo(Stream);
        await Assert.That(result.Version).IsEqualTo(InitialVersion);
        await Assert.That(result.State.ContentType).IsEqualTo(CrdtServerPayloads.ContentType);
        await Assert.That(state.Kind).IsEqualTo(CrdtKind.GCounter);
    }

    /// <summary>Verifies invalid initial state kind configuration is rejected during construction.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConstructorRejectsInvalidKind()
    {
        static void Act() => _ = new CrdtInitialStateFactory(new() { Kind = (CrdtKind)InvalidKindValue });

        await Assert.That(Act).ThrowsExactly<ArgumentOutOfRangeException>();
    }
}
