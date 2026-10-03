// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqlitePayloadStreamIntegrity"/>.</summary>
public sealed class SqlitePayloadStreamIntegrityTests
{
    /// <summary>The first deterministic payload byte.</summary>
    private const byte FirstPayloadByte = 1;

    /// <summary>The second deterministic payload byte.</summary>
    private const byte SecondPayloadByte = 2;

    /// <summary>The third deterministic payload byte.</summary>
    private const byte ThirdPayloadByte = 3;

    /// <summary>The matching sample payload length.</summary>
    private const int MatchingPayloadLength = 3;

    /// <summary>The mismatched sample payload length.</summary>
    private const int MismatchedPayloadLength = 4;

    /// <summary>The byte count returned by the one-byte chunked stream.</summary>
    private const int OneByteChunk = 1;

    /// <summary>The chunk size used for payload hash tests.</summary>
    private const int HashChunkBytes = 257;

    /// <summary>The expected disposal count after a rejected stream is owned and disposed.</summary>
    private const int DisposedOnce = 1;

    /// <summary>The payload size used to verify hashing does not request the full payload in one read.</summary>
    private const int LargePayloadBytes = 64 * 1024;

    /// <summary>Verifies accepted stream length returns the owned stream.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStreamLengthMatches_ThenAcceptLengthReturnsOriginalStream()
    {
        await using var stream = new MemoryStream([FirstPayloadByte, SecondPayloadByte, ThirdPayloadByte]);

        var accepted = SqlitePayloadStreamIntegrity.AcceptLength(stream, MatchingPayloadLength, "length mismatch");

        await Assert.That(ReferenceEquals(accepted, stream)).IsTrue();
    }

    /// <summary>Verifies rejected stream length disposes the owned stream before throwing.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStreamLengthMismatches_ThenAcceptLengthDisposesStream()
    {
        var stream = new TrackingMemoryStream([FirstPayloadByte, SecondPayloadByte, ThirdPayloadByte]);
        Action accept = () => SqlitePayloadStreamIntegrity.AcceptLength(stream, MismatchedPayloadLength, "length mismatch");

        await Assert.That(accept).ThrowsExactly<InvalidOperationException>();
        await Assert.That(stream.DisposeCount).IsEqualTo(DisposedOnce);
    }

    /// <summary>Verifies exact reads tolerate streams that return fewer bytes than requested per read.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStreamReturnsShortReads_ThenReadExactlyReturnsExpectedBytes()
    {
        var stream = new ChunkedReadStream([FirstPayloadByte, SecondPayloadByte, ThirdPayloadByte], maximumReadBytes: OneByteChunk);

        var bytes = SqlitePayloadStreamIntegrity.ReadExactly(stream, MatchingPayloadLength, "truncated");

        await Assert.That(bytes.SequenceEqual([FirstPayloadByte, SecondPayloadByte, ThirdPayloadByte])).IsTrue();
    }

    /// <summary>Verifies truncated streams fail closed before returning partial payload bytes.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStreamEndsBeforeExpectedLength_ThenReadExactlyThrows()
    {
        var stream = new ChunkedReadStream([FirstPayloadByte, SecondPayloadByte], maximumReadBytes: OneByteChunk);
        Action read = () => SqlitePayloadStreamIntegrity.ReadExactly(stream, MatchingPayloadLength, "truncated");

        await Assert.That(read).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies matching canonical hashes validate without materializing another stream.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenCanonicalHashMatches_ThenValidateCanonicalSha256HashSucceeds()
    {
        var payload = CreateLargePayload();
        await using var hashStream = new MemoryStream(payload);
        var expectedHash = SqlitePayloadStreamIntegrity.ComputeSha256PayloadHash(hashStream);
        var stream = new ChunkedReadStream(payload, maximumReadBytes: HashChunkBytes);
        static void Validate(ChunkedReadStream stream, string expectedHash) =>
            SqlitePayloadStreamIntegrity.ValidateCanonicalSha256Hash(stream, expectedHash, "hash mismatch");

        Validate(stream, expectedHash);

        await Assert.That(stream.Position).IsEqualTo(stream.Length);
        await Assert.That(stream.MaximumRequestedReadBytes).IsLessThan(payload.Length);
    }

    /// <summary>Verifies mismatched canonical hashes fail closed.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenCanonicalHashMismatches_ThenValidateCanonicalSha256HashThrows()
    {
        await using var hashStream = new MemoryStream("expected"u8.ToArray());
        var expectedHash = SqlitePayloadStreamIntegrity.ComputeSha256PayloadHash(hashStream);
        var payload = CreateLargePayload();
        var stream = new ChunkedReadStream(payload, maximumReadBytes: HashChunkBytes);
        void Validate() => SqlitePayloadStreamIntegrity.ValidateCanonicalSha256Hash(stream, expectedHash, "hash mismatch");

        await Assert.That(Validate).ThrowsExactly<InvalidOperationException>();
        await Assert.That(stream.MaximumRequestedReadBytes).IsLessThan(payload.Length);
    }

    /// <summary>Creates a deterministic large payload.</summary>
    /// <returns>The payload bytes.</returns>
    private static byte[] CreateLargePayload()
    {
        var payload = new byte[LargePayloadBytes];
        for (var index = 0; index < payload.Length; index++)
        {
            payload[index] = (byte)(index % byte.MaxValue);
        }

        return payload;
    }

    /// <summary>A memory stream that records disposal.</summary>
    private sealed class TrackingMemoryStream : MemoryStream
    {
        /// <summary>Initializes a new instance of the <see cref="TrackingMemoryStream"/> class.</summary>
        /// <param name="buffer">The stream buffer.</param>
        public TrackingMemoryStream(byte[] buffer)
            : base(buffer)
        {
        }

        /// <summary>Gets the number of times the stream was disposed.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                DisposeCount++;
            }

            base.Dispose(disposing);
        }
    }

    /// <summary>A stream that returns a bounded number of bytes per read.</summary>
    /// <param name="bytes">The payload bytes.</param>
    /// <param name="maximumReadBytes">The maximum bytes returned by each read.</param>
    private sealed class ChunkedReadStream(byte[] bytes, int maximumReadBytes) : Stream
    {
        /// <summary>The payload bytes.</summary>
        private readonly byte[] _bytes = bytes;

        /// <summary>The maximum bytes returned by each read.</summary>
        private readonly int _maximumReadBytes = maximumReadBytes;

        /// <summary>The current read position.</summary>
        private long _position;

        /// <inheritdoc/>
        public override bool CanRead => true;

        /// <inheritdoc/>
        public override bool CanSeek => false;

        /// <inheritdoc/>
        public override bool CanWrite => false;

        /// <inheritdoc/>
        public override long Length => _bytes.Length;

        /// <summary>Gets the largest byte count requested by a read.</summary>
        public int MaximumRequestedReadBytes { get; private set; }

        /// <inheritdoc/>
        public override long Position
        {
            get => _position;
            set => throw new NotSupportedException();
        }

        /// <inheritdoc/>
        public override void Flush()
        {
        }

        /// <inheritdoc/>
        public override int Read(byte[] buffer, int offset, int count)
        {
            MaximumRequestedReadBytes = Math.Max(MaximumRequestedReadBytes, count);
            if (_position == _bytes.Length)
            {
                return 0;
            }

            var read = Math.Min(Math.Min(count, _maximumReadBytes), checked((int)(_bytes.Length - _position)));
            Array.Copy(_bytes, _position, buffer, offset, read);
            _position += read;
            return read;
        }

        /// <inheritdoc/>
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        /// <inheritdoc/>
        public override void SetLength(long value) => throw new NotSupportedException();

        /// <inheritdoc/>
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
