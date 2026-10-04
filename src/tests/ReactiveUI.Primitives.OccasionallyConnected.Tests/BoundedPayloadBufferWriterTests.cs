// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="JsonPayloadSerializer.BoundedPayloadBufferWriter"/>.</summary>
public sealed class BoundedPayloadBufferWriterTests
{
    /// <summary>The small writer limit used by tests.</summary>
    private const int SmallLimit = 4;

    /// <summary>The larger writer limit used by growth tests.</summary>
    private const int GrowthLimit = 300;

    /// <summary>The negative advance count used by rejection tests.</summary>
    private const int NegativeCount = -1;

    /// <summary>Verifies memory writes are captured in the final payload copy.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetMemoryWritesPayloadBytes()
    {
        JsonPayloadSerializer.BoundedPayloadBufferWriter writer = new(SmallLimit);
        var memory = writer.GetMemory(SmallLimit);
        memory.Span[0] = (byte)'t';
        memory.Span[1] = (byte)'e';
        memory.Span[2] = (byte)'s';
        memory.Span[3] = (byte)'t';

        writer.Advance(SmallLimit);

        await Assert.That(Encoding.UTF8.GetString(writer.ToArray())).IsEqualTo("test");
    }

    /// <summary>Verifies span writes can grow within the configured limit.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetSpanGrowsWithinLimit()
    {
        JsonPayloadSerializer.BoundedPayloadBufferWriter writer = new(GrowthLimit);
        var span = writer.GetSpan(GrowthLimit);
        span[0] = (byte)'a';

        writer.Advance(1);

        await Assert.That(Encoding.UTF8.GetString(writer.ToArray())).IsEqualTo("a");
    }

    /// <summary>Verifies negative advance counts are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AdvanceRejectsNegativeCount()
    {
        JsonPayloadSerializer.BoundedPayloadBufferWriter writer = new(SmallLimit);

        var exception = Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => writer.Advance(NegativeCount));

        await Assert.That(exception.ParamName).IsEqualTo("count");
    }

    /// <summary>Verifies advance cannot move beyond the configured limit.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AdvanceRejectsCountBeyondLimit()
    {
        JsonPayloadSerializer.BoundedPayloadBufferWriter writer = new(SmallLimit);

        var exception = Assert.ThrowsExactly<PayloadSchemaException>(() => writer.Advance(SmallLimit + 1));

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.PayloadTooLarge);
    }

    /// <summary>Verifies an excessive scratch request cannot cause an unbounded allocation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetMemoryRejectsSizeHintBeyondLimit()
    {
        JsonPayloadSerializer.BoundedPayloadBufferWriter writer = new(SmallLimit);

        var exception = Assert.ThrowsExactly<PayloadSchemaException>(() => writer.GetMemory(int.MaxValue));

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.PayloadTooLarge);
    }

    /// <summary>Verifies requested spans cannot exceed the configured limit.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetSpanRejectsSizeHintBeyondRemainingLimit()
    {
        JsonPayloadSerializer.BoundedPayloadBufferWriter writer = new(SmallLimit);
        writer.Advance(SmallLimit);

        var exception = Assert.ThrowsExactly<PayloadSchemaException>(() => writer.GetSpan());

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.PayloadTooLarge);
    }
}
