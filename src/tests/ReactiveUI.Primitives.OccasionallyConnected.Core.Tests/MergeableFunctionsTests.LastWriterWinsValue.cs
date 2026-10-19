// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests deterministic last-writer register merges.</summary>
public sealed partial class MergeableFunctionsTests
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
        var first = new MergeableState { Kind = MergeableKind.LastWriterWinsValue, LatestValue = new byte[] { 1 }, LatestWriteStamp = stamp };
        var second = first with { LatestValue = new byte[] { 1, 0 } };
        var third = first with { LatestValue = new byte[] { largestValue } };

        await AssertSameState(MergeableFunctions.Merge(first, second), MergeableFunctions.Merge(second, first));
        await AssertSameState(MergeableFunctions.Merge(first, first), first);
        await AssertSameState(
            MergeableFunctions.Merge(MergeableFunctions.Merge(first, second), third),
            MergeableFunctions.Merge(first, MergeableFunctions.Merge(second, third)));
        await AssertSameState(MergeableFunctions.Merge(first, second), second);
        await AssertSameState(MergeableFunctions.Merge(second, third), third);
    }

    /// <summary>Checks equivalent timestamp offsets cannot change canonical merged bytes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RegisterEqualInstantsHaveCanonicalStamp()
    {
        var stamp = new ConflictWriteStamp { ClientId = ClientId, CommittedAtUtc = DateTimeOffset.UnixEpoch, OperationId = new(new Guid("00000000-0000-0000-0000-000000000001")) };
        var first = new MergeableState { Kind = MergeableKind.LastWriterWinsValue, LatestValue = new byte[] { 1 }, LatestWriteStamp = stamp };
        var second = first with { LatestWriteStamp = stamp with { CommittedAtUtc = stamp.CommittedAtUtc.ToOffset(TimeSpan.FromHours(1)) } };
        await AssertSameState(MergeableFunctions.Merge(first, second), MergeableFunctions.Merge(second, first));
        await Assert.That(MergeableFunctions.Merge(first, second).LatestWriteStamp!.CommittedAtUtc.Offset).IsEqualTo(TimeSpan.Zero);
    }

    /// <summary>Compares complete canonical state bytes.</summary>
    /// <param name="left">The first state.</param>
    /// <param name="right">The second state.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertSameState(MergeableState left, MergeableState right) =>
        await Assert.That(MergeableCodec.EncodeState(left).SequenceEqual(MergeableCodec.EncodeState(right))).IsTrue();
}
