// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpBoundedBufferWriter"/>.</summary>
public sealed class HttpBoundedBufferWriterTests
{
    /// <summary>The small bounded writer capacity.</summary>
    private const int SmallCapacity = 8;

    /// <summary>The exact committed byte count.</summary>
    private const int CommittedByteCount = 2;

    /// <summary>The expanded bounded writer capacity.</summary>
    private const int ExpandedCapacity = 512;

    /// <summary>The requested contiguous memory length.</summary>
    private const int RequestedMemoryLength = 300;

    /// <summary>The single-byte bounded writer capacity.</summary>
    private const int SingleByteCapacity = 1;

    /// <summary>The oversized contiguous span request.</summary>
    private const int OversizedSpanRequest = 4098;

    /// <summary>The first sample byte.</summary>
    private const byte FirstByte = 1;

    /// <summary>The second sample byte.</summary>
    private const byte SecondByte = 2;

    /// <summary>Verifies written bytes are copied into a right-sized result.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ToArrayCopiesCommittedBytes()
    {
        var writer = new HttpBoundedBufferWriter(SmallCapacity);
        var span = writer.GetSpan();
        span[0] = FirstByte;
        span[1] = SecondByte;

        writer.Advance(CommittedByteCount);
        var bytes = writer.ToArray();

        await Assert.That(bytes.Length).IsEqualTo(CommittedByteCount);
        await Assert.That(bytes[0]).IsEqualTo(FirstByte);
        await Assert.That(bytes[1]).IsEqualTo(SecondByte);
    }

    /// <summary>Verifies a large memory hint expands the buffer within the allocation bound.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GetMemoryExpandsWithinBound()
    {
        var writer = new HttpBoundedBufferWriter(ExpandedCapacity);

        var memory = writer.GetMemory(RequestedMemoryLength);

        await Assert.That(memory.Length).IsGreaterThanOrEqualTo(RequestedMemoryLength);
    }

    /// <summary>Verifies over-advancing past the configured byte limit fails.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdvanceRejectsBytesBeyondMaximum()
    {
        var writer = new HttpBoundedBufferWriter(SingleByteCapacity);

        await Assert.That(() => writer.Advance(CommittedByteCount)).ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies requests for more contiguous scratch space than allowed fail.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GetSpanRejectsHintBeyondAllocationBound()
    {
        var writer = new HttpBoundedBufferWriter(SingleByteCapacity);

        await Assert.That(() => writer.GetSpan(OversizedSpanRequest)).ThrowsExactly<HttpRemoteTransportException>();
    }
}
