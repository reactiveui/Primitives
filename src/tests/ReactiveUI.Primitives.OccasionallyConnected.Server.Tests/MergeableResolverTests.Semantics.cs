// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="MergeableResolver"/>.</summary>
/// <content>CRDT merge semantics.</content>
public sealed partial class MergeableResolverTests
{
    /// <summary>The lower OR-set sequence.</summary>
    private const int LowerSequence = 7;

    /// <summary>The higher OR-set sequence.</summary>
    private const int HigherSequence = 9;

    /// <summary>The PN-counter positive component.</summary>
    private const int PositiveComponent = 4;

    /// <summary>The PN-counter negative component.</summary>
    private const int NegativeComponent = 6;

    /// <summary>The expected PN-counter value.</summary>
    private const int PnCounterValue = -2;

    /// <summary>The expected double item count.</summary>
    private const int DoubleCount = 2;

    /// <summary>The one-count component used for overflow checks.</summary>
    private const int OverflowDelta = 1;

    /// <summary>The new LWW text.</summary>
    private const string NewText = "new";

    /// <summary>The alpha OR-set element text.</summary>
    private const string AlphaText = "alpha";

    /// <summary>The beta OR-set element text.</summary>
    private const string BetaText = "beta";

    /// <summary>Verifies PN-counter positive and negative components merge independently.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResolveAsyncMergesPnCounterComponentsIndependently()
    {
        var operation = Operation(
            OperationId.New(),
            MergeableInput.ForChange(MergeableChange.CounterSet(Client, PositiveComponent, NegativeComponent)));
        var resolver = new MergeableResolver(new() { Kind = MergeableKind.Counter, VersionFactory = new MergeableSequentialVersionFactory() });
        var result = await resolver.ResolveAsync(
            Context(MergeableFunctions.Empty(MergeableKind.Counter), operation, Client),
            CancellationToken.None);
        var resolved = DecodeState(result.Conflicts[0].ResolvedPayload);

        await Assert.That(resolved.IncrementsByClient[Client]).IsEqualTo(PositiveComponent);
        await Assert.That(resolved.DecrementsByClient[Client]).IsEqualTo(NegativeComponent);
        await Assert.That(resolved.Value.Counter).IsEqualTo(PnCounterValue);
    }

    /// <summary>Verifies G-counter aggregate overflow rejects the mutation without escaping as a host fault.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResolveAsyncRejectsIncrementOnlyCounterAggregateOverflow()
    {
        var current = MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter),
            MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(ForeignClient, long.MaxValue)),
            ForeignClient,
            DurableSequence,
            MergeableBounds.Default);
        var operation = Operation(
            OperationId.New(),
            MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(Client, OverflowDelta)));
        var resolver = new MergeableResolver(new() { Kind = MergeableKind.IncrementOnlyCounter, VersionFactory = new MergeableSequentialVersionFactory() });

        var result = await resolver.ResolveAsync(Context(current, operation, Client), CancellationToken.None);

        await Assert.That(result.AcceptedOperations).IsEmpty();
        await Assert.That(result.RejectedOperations[0].ReasonCode).IsEqualTo(InvalidMutationReason);
        await Assert.That(result.ServerVersion).IsEqualTo(InitialVersion);
    }

    /// <summary>Verifies PN-counter aggregate overflow rejects the mutation without escaping as a host fault.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResolveAsyncRejectsPnCounterAggregateOverflow()
    {
        var current = MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.Counter),
            MergeableInput.ForChange(MergeableChange.CounterSet(ForeignClient, long.MaxValue, 0)),
            ForeignClient,
            DurableSequence,
            MergeableBounds.Default);
        var operation = Operation(
            OperationId.New(),
            MergeableInput.ForChange(MergeableChange.CounterSet(Client, OverflowDelta, 0)));
        var resolver = new MergeableResolver(new() { Kind = MergeableKind.Counter, VersionFactory = new MergeableSequentialVersionFactory() });

        var result = await resolver.ResolveAsync(Context(current, operation, Client), CancellationToken.None);

        await Assert.That(result.AcceptedOperations).IsEmpty();
        await Assert.That(result.RejectedOperations[0].ReasonCode).IsEqualTo(InvalidMutationReason);
        await Assert.That(result.ServerVersion).IsEqualTo(InitialVersion);
    }

    /// <summary>Verifies OR-set add dots use the durable operation sequence even after higher dots exist.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResolveAsyncUsesDurableClientSequenceForOrSetDot()
    {
        var current = MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.AddWinsSet),
            MergeableInput.ForChange(MergeableChange.AddWinsSetAdd(Bytes(BetaText))),
            Client,
            HigherSequence,
            MergeableBounds.Default);
        var operation = Operation(
            OperationId.New(),
            MergeableInput.ForChange(MergeableChange.AddWinsSetAdd(Bytes(AlphaText)))) with
        {
            ClientSequence = LowerSequence,
        };
        var resolver = new MergeableResolver(new() { Kind = MergeableKind.AddWinsSet, VersionFactory = new MergeableSequentialVersionFactory() });

        var result = await resolver.ResolveAsync(Context(current, operation, Client), CancellationToken.None);
        var resolved = DecodeState(result.Conflicts[0].ResolvedPayload);

        await Assert.That(resolved.Elements).Count().IsEqualTo(DoubleCount);
        await Assert.That(resolved.Elements[0].Tag.ClientSequence).IsEqualTo(LowerSequence);
        await Assert.That(resolved.Elements[1].Tag.ClientSequence).IsEqualTo(HigherSequence);
        await Assert.That(resolved.Value.Elements).Count().IsEqualTo(DoubleCount);
    }

    /// <summary>Verifies retained tombstones prevent delayed remove-before-add resurrection.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResolveAsyncRetainsOrSetRemoveBeforeAdd()
    {
        var tombstone = new TaggedElement { Tag = new() { ClientId = Client, ClientSequence = LowerSequence }, Element = Bytes(AlphaText) };
        var current = new MergeableState { Kind = MergeableKind.AddWinsSet, RemovedTags = [tombstone] };
        var operation = Operation(
            OperationId.New(),
            MergeableInput.ForChange(MergeableChange.AddWinsSetAdd(Bytes(AlphaText)))) with
        {
            ClientSequence = LowerSequence,
        };
        var resolver = new MergeableResolver(new() { Kind = MergeableKind.AddWinsSet, VersionFactory = new MergeableSequentialVersionFactory() });

        var result = await resolver.ResolveAsync(Context(current, operation, Client), CancellationToken.None);
        var resolved = DecodeState(result.Conflicts[0].ResolvedPayload);

        await Assert.That(resolved.Elements).Count().IsEqualTo(SingleCount);
        await Assert.That(resolved.RemovedTags).Count().IsEqualTo(SingleCount);
        await Assert.That(resolved.Value.Elements).IsEmpty();
    }

    /// <summary>Verifies reusing one OR-set dot for different bytes rejects the operation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResolveAsyncRejectsOrSetDotRebind()
    {
        var current = MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.AddWinsSet),
            MergeableInput.ForChange(MergeableChange.AddWinsSetAdd(Bytes(AlphaText))),
            Client,
            DurableSequence,
            MergeableBounds.Default);
        var operation = Operation(
            OperationId.New(),
            MergeableInput.ForChange(MergeableChange.AddWinsSetAdd(Bytes(BetaText))));
        var resolver = new MergeableResolver(new() { Kind = MergeableKind.AddWinsSet, VersionFactory = new MergeableSequentialVersionFactory() });

        var result = await resolver.ResolveAsync(Context(current, operation, Client), CancellationToken.None);

        await Assert.That(result.AcceptedOperations).IsEmpty();
        await Assert.That(result.RejectedOperations[0].ReasonCode).IsEqualTo(InvalidMutationReason);
    }

    /// <summary>Verifies LWW register mutations keep the trusted server write stamp.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResolveAsyncUsesTrustedLwwStamp()
    {
        var operationId = OperationId.New();
        var clientStamp = new ConflictWriteStamp { ClientId = ForeignClient, OperationId = OperationId.New(), CommittedAtUtc = Start.AddDays(1) };
        var operation = Operation(
            operationId,
            MergeableInput.ForChange(MergeableChange.LastWriterWinsValueSet(Bytes(NewText), clientStamp)));
        var resolver = new MergeableResolver(new() { Kind = MergeableKind.LastWriterWinsValue, VersionFactory = new MergeableSequentialVersionFactory() });

        var result = await resolver.ResolveAsync(
            Context(MergeableFunctions.Empty(MergeableKind.LastWriterWinsValue), operation, Client),
            CancellationToken.None);
        var resolved = DecodeState(result.Conflicts[0].ResolvedPayload);

        await Assert.That(Encoding.UTF8.GetString(resolved.LatestValue.ToArray())).IsEqualTo(NewText);
        await Assert.That(resolved.LatestWriteStamp?.ClientId).IsEqualTo(Client);
        await Assert.That(resolved.LatestWriteStamp?.OperationId).IsEqualTo(operationId);
        await Assert.That(resolved.LatestWriteStamp?.CommittedAtUtc).IsEqualTo(Start);
    }

    /// <summary>Creates a conflict context.</summary>
    /// <param name="state">The CRDT state.</param>
    /// <param name="operation">The operation.</param>
    /// <param name="clientId">The authenticated client id.</param>
    /// <returns>The context.</returns>
    private static ConflictContext Context(
        MergeableState state,
        SyncOperation operation,
        string clientId) =>
        new(
            new(Stream, InitialVersion, MergeableServerPayloads.CreateState(state)),
            [operation],
            new(clientId, Tenant),
            new() { CandidateWrite = new() { ClientId = clientId, OperationId = operation.OperationId, CommittedAtUtc = Start } });

    /// <summary>Creates UTF-8 bytes.</summary>
    /// <param name="text">The text.</param>
    /// <returns>The bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);
}
