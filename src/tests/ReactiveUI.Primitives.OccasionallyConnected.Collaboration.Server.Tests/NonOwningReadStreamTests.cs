// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests;

/// <summary>Tests for <see cref="NonOwningReadStream"/>.</summary>
public sealed class NonOwningReadStreamTests
{
    /// <summary>The initial byte position used by seek forwarding tests.</summary>
    private const int InitialPosition = 2;

    /// <summary>The synchronous read buffer size.</summary>
    private const int SynchronousReadBufferSize = 3;

    /// <summary>The memory asynchronous read buffer size.</summary>
    private const int MemoryAsyncReadBufferSize = 4;

    /// <summary>The expected forwarded flush count.</summary>
    private const int ExpectedForwardedFlushCount = 1;

    /// <summary>The expected first synchronous read text.</summary>
    private const string ExpectedArrayReadText = "cde";

    /// <summary>The expected second synchronous read text.</summary>
    private const string ExpectedSpanReadText = "fgh";

    /// <summary>The expected memory asynchronous read text.</summary>
    private const string ExpectedMemoryAsyncReadText = "abcd";

    /// <summary>Verifies readable and seekable members are forwarded to the borrowed stream.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReadMembersForwardToBorrowedStream()
    {
        await using var inner = CreateBorrowedStream();
        await using var stream = new NonOwningReadStream(inner);

        await Assert.That(stream.CanRead).IsTrue();
        await Assert.That(stream.CanSeek).IsTrue();
        await Assert.That(stream.CanWrite).IsFalse();
        await Assert.That(stream.Length).IsEqualTo(inner.Length);
        await Assert.That(stream.Position).IsEqualTo(0);

        stream.Position = InitialPosition;
        await Assert.That(inner.Position).IsEqualTo(InitialPosition);

        var result = ReadSynchronously(stream);
        await Assert.That(result.ArrayRead).IsEqualTo(SynchronousReadBufferSize);
        await Assert.That(result.ArrayText).IsEqualTo(ExpectedArrayReadText);
        await Assert.That(result.PositionAfterArrayRead).IsEqualTo(InitialPosition + SynchronousReadBufferSize);
        await Assert.That(result.SpanRead).IsEqualTo(SynchronousReadBufferSize);
        await Assert.That(result.SpanText).IsEqualTo(ExpectedSpanReadText);
        await Assert.That(result.PositionAfterSpanRead)
            .IsEqualTo(InitialPosition + SynchronousReadBufferSize + SynchronousReadBufferSize);
        await Assert.That(result.SeekPosition).IsEqualTo(InitialPosition);
        await Assert.That(stream.Position).IsEqualTo(InitialPosition);
        await Assert.That(inner.FlushCount).IsEqualTo(ExpectedForwardedFlushCount);
    }

    /// <summary>Verifies asynchronous read and flush members are forwarded to the borrowed stream.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AsyncMembersForwardToBorrowedStream()
    {
        await using var inner = CreateBorrowedStream();
        await using var stream = new NonOwningReadStream(inner);
        var memoryBuffer = new byte[MemoryAsyncReadBufferSize];

        var memoryRead = await stream
            .ReadAsync(memoryBuffer.AsMemory(), CancellationToken.None)
            .ConfigureAwait(false);
        var memoryText = Encoding.UTF8.GetString(memoryBuffer, 0, memoryRead);
        var positionAfterMemoryRead = stream.Position;
        await stream.FlushAsync(CancellationToken.None).ConfigureAwait(false);

        await Assert.That(memoryRead).IsEqualTo(MemoryAsyncReadBufferSize);
        await Assert.That(memoryText).IsEqualTo(ExpectedMemoryAsyncReadText);
        await Assert.That(positionAfterMemoryRead).IsEqualTo(MemoryAsyncReadBufferSize);
        await Assert.That(inner.FlushAsyncCount).IsEqualTo(ExpectedForwardedFlushCount);
    }

    /// <summary>Verifies read cancellation is forwarded to the borrowed stream.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AsyncReadForwardsCancellationToBorrowedStream()
    {
        await using var inner = new CancellationAwareReadStream();
        await using var stream = new NonOwningReadStream(inner);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        var buffer = new byte[MemoryAsyncReadBufferSize];

        _ = await Assert.ThrowsExactlyAsync<OperationCanceledException>(() =>
            stream.ReadAsync(buffer.AsMemory(), cancellation.Token).AsTask());
        await Assert.That(inner.ObservedCancellation).IsTrue();
    }

    /// <summary>Verifies disposing the wrapper leaves the borrowed stream open.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeLeavesBorrowedStreamOpen()
    {
        await using var inner = CreateBorrowedStream();
        var value = DisposeWrapperAndReadBorrowed(inner);

        await Assert.That(value).IsGreaterThanOrEqualTo(0);
    }

    /// <summary>Verifies write members stay disabled even when the borrowed stream supports writes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WriteMembersAreRejected()
    {
        await using var inner = CreateBorrowedStream();
        await using var stream = new NonOwningReadStream(inner);
        var buffer = new byte[SynchronousReadBufferSize];

        await Assert.That(() => stream.SetLength(0)).ThrowsExactly<NotSupportedException>();
        await Assert.That(() => stream.Write(buffer, 0, buffer.Length)).ThrowsExactly<NotSupportedException>();
    }

    /// <summary>Creates a readable borrowed stream.</summary>
    /// <returns>The borrowed stream.</returns>
    private static TrackingMemoryStream CreateBorrowedStream() =>
        new("abcdefghi"u8.ToArray());

    /// <summary>Disposes the wrapper and reads the borrowed stream afterward.</summary>
    /// <param name="inner">The borrowed stream.</param>
    /// <returns>The byte read after wrapper disposal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int DisposeWrapperAndReadBorrowed(Stream inner)
    {
        var stream = new NonOwningReadStream(inner);
        stream.Dispose();
        inner.Position = 0;
        return inner.ReadByte();
    }

    /// <summary>Exercises synchronous forwarding members outside the async test flow.</summary>
    /// <param name="stream">The wrapper stream.</param>
    /// <returns>The synchronous operation results.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SynchronousReadResult ReadSynchronously(Stream stream)
    {
        var arrayBuffer = new byte[SynchronousReadBufferSize];
        var arrayRead = stream.Read(arrayBuffer, 0, arrayBuffer.Length);
        var arrayText = Encoding.UTF8.GetString(arrayBuffer, 0, arrayRead);
        var positionAfterArrayRead = stream.Position;
        var spanBuffer = new byte[SynchronousReadBufferSize];
        var spanRead = stream.Read(spanBuffer.AsSpan());
        var spanText = Encoding.UTF8.GetString(spanBuffer, 0, spanRead);
        var positionAfterSpanRead = stream.Position;
        var seekPosition = stream.Seek(InitialPosition, SeekOrigin.Begin);
        stream.Flush();
        return new(
            arrayRead,
            arrayText,
            positionAfterArrayRead,
            spanRead,
            spanText,
            positionAfterSpanRead,
            seekPosition);
    }

    /// <summary>Memory stream that records flush delegation.</summary>
    /// <param name="buffer">The borrowed stream bytes.</param>
    private sealed class TrackingMemoryStream(byte[] buffer) : MemoryStream(buffer)
    {
        /// <summary>Gets the number of synchronous flush calls.</summary>
        internal int FlushCount { get; private set; }

        /// <summary>Gets the number of asynchronous flush calls.</summary>
        internal int FlushAsyncCount { get; private set; }

        /// <inheritdoc />
        public override void Flush()
        {
            FlushCount++;
            base.Flush();
        }

        /// <inheritdoc />
        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            FlushAsyncCount++;
            return base.FlushAsync(cancellationToken);
        }
    }

    /// <summary>Borrowed stream that observes cancellation passed through by the wrapper.</summary>
    private sealed class CancellationAwareReadStream : MemoryStream
    {
        /// <summary>Gets whether a canceled token reached the borrowed stream.</summary>
        internal bool ObservedCancellation { get; private set; }

        /// <inheritdoc />
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            ObservedCancellation = cancellationToken.IsCancellationRequested;
            cancellationToken.ThrowIfCancellationRequested();
            return base.ReadAsync(buffer, cancellationToken);
        }
    }

    /// <summary>The results of forwarded synchronous read operations.</summary>
    /// <param name="ArrayRead">The byte-array read count.</param>
    /// <param name="ArrayText">The byte-array read text.</param>
    /// <param name="PositionAfterArrayRead">The position after the byte-array read.</param>
    /// <param name="SpanRead">The span read count.</param>
    /// <param name="SpanText">The span read text.</param>
    /// <param name="PositionAfterSpanRead">The position after the span read.</param>
    /// <param name="SeekPosition">The position returned by seek.</param>
    private sealed record SynchronousReadResult(
        int ArrayRead,
        string ArrayText,
        long PositionAfterArrayRead,
        int SpanRead,
        string SpanText,
        long PositionAfterSpanRead,
        long SeekPosition);
}
