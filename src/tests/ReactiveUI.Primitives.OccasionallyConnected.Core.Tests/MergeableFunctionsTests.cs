// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests the public pure CRDT mutation boundary.</summary>
public sealed partial class MergeableFunctionsTests
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
        var state = new MergeableState { Kind = MergeableKind.LastWriterWinsValue, LatestWriteStamp = stamp };
        var input = MergeableInput.ForChange(MergeableChange.LastWriterWinsValueSet(ReadOnlyMemory<byte>.Empty, stamp));
        await Assert.That(() => MergeableFunctions.ValidateState(state)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => MergeableCodec.ValidateInput(input)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies actor byte bounds are enforced at the validation boundary before encoding.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CounterRejectsActorBeyondConfiguredByteBound()
    {
        var input = MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(ClientId, 1));
        var bounds = new MergeableBounds { MaximumClientIdUtf8Bytes = 1 };
        await Assert.That(() => MergeableCodec.ValidateInput(input, bounds)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies state snapshots cannot persist duplicate causal bindings.</summary>
    /// <param name="tombstones">Whether the duplicate is in removed bindings.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task EncodeRejectsDuplicateStateDots(bool tombstones)
    {
        var binding = new TaggedElement { Tag = new() { ClientId = ClientId, ClientSequence = 1 }, Element = ReadOnlyMemory<byte>.Empty };
        var duplicates = new[] { binding, binding };
        var state = tombstones
            ? new MergeableState { Kind = MergeableKind.AddWinsSet, RemovedTags = duplicates }
            : new MergeableState { Kind = MergeableKind.AddWinsSet, Elements = duplicates };
        await Assert.That(() => MergeableCodec.EncodeState(state)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies removal dots must be valid and unique before serialization.</summary>
    /// <param name="duplicate">Whether the invalid input repeats one valid dot.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RemovalRejectsInvalidObservedDots(bool duplicate)
    {
        var dot = new ElementTag { ClientId = duplicate ? ClientId : string.Empty, ClientSequence = 1 };
        var dots = duplicate ? new[] { dot, dot } : new[] { dot };
        var input = MergeableInput.ForChange(MergeableChange.AddWinsSetRemove(ReadOnlyMemory<byte>.Empty, dots));
        await Assert.That(() => MergeableCodec.EncodeInput(input)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies public values reject invalid state metadata rather than hiding it.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ValueRejectsMetadataFromAnotherStateKind()
    {
        var state = new MergeableState { Kind = MergeableKind.IncrementOnlyCounter, IncrementsByClient = new Dictionary<string, long> { [ClientId] = 1 } };
        await Assert.That(() => state.Value).ThrowsExactly<InvalidOperationException>();
    }
}
