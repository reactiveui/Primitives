// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests the public pure CRDT mutation boundary.</summary>
public sealed class CrdtFunctionsTests
{
    /// <summary>The test client identifier.</summary>
    private const string ClientId = "client";

    /// <summary>Verifies persisted register provenance has a valid client and operation identity.</summary>
    /// <param name="missingOperation">Whether the operation identity is missing.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RegisterRejectsInvalidWriteIdentity(bool missingOperation)
    {
        var stamp = new ConflictWriteStamp
        {
            CommittedAtUtc = DateTimeOffset.UnixEpoch,
            ClientId = missingOperation ? ClientId : string.Empty,
            OperationId = new(missingOperation ? Guid.Empty : Guid.NewGuid()),
        };
        var state = new CrdtState { Kind = CrdtKind.LwwRegister, RegisterStamp = stamp };
        var input = CrdtInput.ForMutation(CrdtMutation.LwwRegisterSet(ReadOnlyMemory<byte>.Empty, stamp));
        await Assert.That(() => CrdtFunctions.ValidateState(state)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => CrdtCodec.ValidateInput(input)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies actor byte bounds are enforced at the validation boundary before encoding.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CounterRejectsActorBeyondConfiguredByteBound()
    {
        var input = CrdtInput.ForMutation(CrdtMutation.GCounterSet(ClientId, 1));
        var bounds = new CrdtBounds { MaximumClientIdUtf8Bytes = 1 };
        await Assert.That(() => CrdtCodec.ValidateInput(input, bounds)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies state snapshots cannot persist duplicate causal bindings.</summary>
    /// <param name="tombstones">Whether the duplicate is in removed bindings.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task EncodeRejectsDuplicateStateDots(bool tombstones)
    {
        var binding = new CrdtDotElement { Dot = new() { ClientId = ClientId, ClientSequence = 1 }, Element = ReadOnlyMemory<byte>.Empty };
        var duplicates = new[] { binding, binding };
        var state = tombstones
            ? new CrdtState { Kind = CrdtKind.ORSet, Tombstones = duplicates }
            : new CrdtState { Kind = CrdtKind.ORSet, DotBindings = duplicates };
        await Assert.That(() => CrdtCodec.EncodeState(state)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies an input cannot carry both union alternatives.</summary>
    /// <param name="kind">The claimed input kind.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(CrdtInputKind.Mutation)]
    [Arguments(CrdtInputKind.AuthoritativeState)]
    public async Task EncodeRejectsAmbiguousInput(CrdtInputKind kind)
    {
        var input = new CrdtInput { Kind = kind, Mutation = CrdtMutation.GCounterSet(ClientId, 1), State = CrdtFunctions.Empty(CrdtKind.GCounter) };
        await Assert.That(() => CrdtCodec.EncodeInput(input)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies removal dots must be valid and unique before serialization.</summary>
    /// <param name="duplicate">Whether the invalid input repeats one valid dot.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RemovalRejectsInvalidObservedDots(bool duplicate)
    {
        var dot = new CrdtDot { ClientId = duplicate ? ClientId : string.Empty, ClientSequence = 1 };
        var dots = duplicate ? new[] { dot, dot } : new[] { dot };
        var input = CrdtInput.ForMutation(CrdtMutation.ORSetRemove(ReadOnlyMemory<byte>.Empty, dots));
        await Assert.That(() => CrdtCodec.EncodeInput(input)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies unrelated mutation fields are rejected on both pure and serialization paths.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CounterMutationRejectsUnrelatedRegisterBytes()
    {
        const string actor = ClientId;
        var mutation = CrdtMutation.GCounterSet(actor, 1) with { Bytes = new byte[] { 1 } };
        var input = CrdtInput.ForMutation(mutation);
        await Assert.That(() => CrdtCodec.EncodeInput(input)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => CrdtFunctions.ApplyLocal(CrdtFunctions.Empty(CrdtKind.GCounter), input, actor, 1))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies public values reject invalid state metadata rather than hiding it.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ValueRejectsMetadataFromAnotherStateKind()
    {
        var state = new CrdtState { Kind = CrdtKind.GCounter, PNCounterPositiveComponents = new Dictionary<string, long> { [ClientId] = 1 } };
        await Assert.That(() => state.Value).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies unknown mutation kinds cannot be persisted.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task EncodeRejectsUnknownMutationKind()
    {
        const int unknownKind = 255;
        var input = CrdtInput.ForMutation(new CrdtMutation { Kind = (CrdtMutationKind)unknownKind });
        await Assert.That(() => CrdtCodec.EncodeInput(input)).ThrowsExactly<InvalidOperationException>();
    }
}
