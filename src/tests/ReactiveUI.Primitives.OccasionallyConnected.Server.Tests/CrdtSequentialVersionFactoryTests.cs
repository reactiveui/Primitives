// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="CrdtSequentialVersionFactory"/>.</summary>
public sealed class CrdtSequentialVersionFactoryTests
{
    /// <summary>The tenant.</summary>
    private const string Tenant = "tenant";

    /// <summary>The client.</summary>
    private const string Client = "client";

    /// <summary>The canonical zero CRDT version.</summary>
    private const string ZeroVersion = "crdt-v0";

    /// <summary>The first CRDT version.</summary>
    private const string FirstVersion = "crdt-v1";

    /// <summary>The noncanonical leading-zero version.</summary>
    private const string LeadingZeroVersion = "crdt-v01";

    /// <summary>The incompatible version.</summary>
    private const string IncompatibleVersion = "v0";

    /// <summary>The missing sequence version.</summary>
    private const string MissingSequenceVersion = "crdt-v";

    /// <summary>The invalid sequence version.</summary>
    private const string InvalidSequenceVersion = "crdt-vx";

    /// <summary>The maximum version.</summary>
    private const string MaximumVersion = "crdt-v9223372036854775807";

    /// <summary>The durable sequence.</summary>
    private const int DurableSequence = 1;

    /// <summary>The stream.</summary>
    private static readonly StreamId Stream = new("crdt/version");

    /// <summary>Verifies default versions increment from the default CRDT initial version.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateNextVersionIncrementsDefaultInitialVersion()
    {
        var factory = new CrdtSequentialVersionFactory();

        var result = factory.CreateNextVersion(Context(ZeroVersion), Operation());

        await Assert.That(result).IsEqualTo(FirstVersion);
    }

    /// <summary>Verifies leading-zero sequences are rejected as noncanonical.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateNextVersionRejectsLeadingZeroSequence()
    {
        var factory = new CrdtSequentialVersionFactory();

        void Act() => _ = factory.CreateNextVersion(Context(LeadingZeroVersion), Operation());

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies incompatible prefixes are rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateNextVersionRejectsIncompatiblePrefix()
    {
        var factory = new CrdtSequentialVersionFactory();

        void Act() => _ = factory.CreateNextVersion(Context(IncompatibleVersion), Operation());

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies missing sequences are rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateNextVersionRejectsMissingSequence()
    {
        var factory = new CrdtSequentialVersionFactory();

        void Act() => _ = factory.CreateNextVersion(Context(MissingSequenceVersion), Operation());

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies non-digit sequences are rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateNextVersionRejectsInvalidSequence()
    {
        var factory = new CrdtSequentialVersionFactory();

        void Act() => _ = factory.CreateNextVersion(Context(InvalidSequenceVersion), Operation());

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies numeric overflow is surfaced.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateNextVersionRejectsOverflow()
    {
        var factory = new CrdtSequentialVersionFactory();

        void Act() => _ = factory.CreateNextVersion(Context(MaximumVersion), Operation());

        await Assert.That(Act).ThrowsExactly<OverflowException>();
    }

    /// <summary>Creates a conflict context with the supplied current version.</summary>
    /// <param name="version">The current version.</param>
    /// <returns>The conflict context.</returns>
    private static ConflictContext Context(string version)
    {
        var operation = Operation();
        return new(
            new(Stream, version, CrdtServerPayloads.CreateState(CrdtFunctions.Empty(CrdtKind.GCounter))),
            [operation],
            new(Client, Tenant),
            new() { CandidateWrite = new() { ClientId = Client, OperationId = operation.OperationId, CommittedAtUtc = DateTimeOffset.UnixEpoch } });
    }

    /// <summary>Creates a sync operation.</summary>
    /// <returns>The operation.</returns>
    private static SyncOperation Operation() =>
        new()
        {
            OperationId = OperationId.New(),
            StreamId = Stream,
            ClientSequence = DurableSequence,
            TimestampUtc = DateTimeOffset.UnixEpoch,
            BaseVersion = ZeroVersion,
            Type = SyncOperationType.Update,
            Payload = CrdtServerPayloads.CreateInput(CrdtInput.ForMutation(CrdtMutation.GCounterSet(Client, DurableSequence))),
        };
}
