// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests deterministic last-writer register merges.</summary>
public sealed partial class CrdtFunctionsTests
{
    /// <summary>Checks bytewise tie-breaking for null and equal authoritative stamps.</summary>
    /// <param name="stamped">Whether to use an authoritative stamp.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RegisterTiesAreCommutativeAssociativeAndIdempotent(bool stamped)
    {
        const byte largestValue = 2;
        ConflictWriteStamp? stamp = stamped
            ? new() { ClientId = ClientId, CommittedAtUtc = DateTimeOffset.UnixEpoch, OperationId = new(new Guid("00000000-0000-0000-0000-000000000001")) }
            : null;
        var first = new CrdtState { Kind = CrdtKind.LwwRegister, RegisterValue = new byte[] { 1 }, RegisterStamp = stamp };
        var second = first with { RegisterValue = new byte[] { 1, 0 } };
        var third = first with { RegisterValue = new byte[] { largestValue } };

        await AssertSameState(CrdtFunctions.Merge(first, second), CrdtFunctions.Merge(second, first));
        await AssertSameState(CrdtFunctions.Merge(first, first), first);
        await AssertSameState(
            CrdtFunctions.Merge(CrdtFunctions.Merge(first, second), third),
            CrdtFunctions.Merge(first, CrdtFunctions.Merge(second, third)));
        await AssertSameState(CrdtFunctions.Merge(first, second), second);
        await AssertSameState(CrdtFunctions.Merge(second, third), third);
    }

    /// <summary>Checks equivalent timestamp offsets cannot change canonical merged bytes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RegisterEqualInstantsHaveCanonicalStamp()
    {
        var stamp = new ConflictWriteStamp { ClientId = ClientId, CommittedAtUtc = DateTimeOffset.UnixEpoch, OperationId = new(new Guid("00000000-0000-0000-0000-000000000001")) };
        var first = new CrdtState { Kind = CrdtKind.LwwRegister, RegisterValue = new byte[] { 1 }, RegisterStamp = stamp };
        var second = first with { RegisterStamp = stamp with { CommittedAtUtc = stamp.CommittedAtUtc.ToOffset(TimeSpan.FromHours(1)) } };
        await AssertSameState(CrdtFunctions.Merge(first, second), CrdtFunctions.Merge(second, first));
        await Assert.That(CrdtFunctions.Merge(first, second).RegisterStamp!.CommittedAtUtc.Offset).IsEqualTo(TimeSpan.Zero);
    }

    /// <summary>Compares complete canonical state bytes.</summary>
    /// <param name="left">The first state.</param>
    /// <param name="right">The second state.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertSameState(CrdtState left, CrdtState right) =>
        await Assert.That(CrdtCodec.EncodeState(left).SequenceEqual(CrdtCodec.EncodeState(right))).IsTrue();
}
