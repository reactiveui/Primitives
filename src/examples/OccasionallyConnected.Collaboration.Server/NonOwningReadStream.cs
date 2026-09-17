// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

/// <summary>Wraps a borrowed stream so disposing HTTP content does not close the ASP.NET-owned request body.</summary>
internal sealed class NonOwningReadStream : Stream
{
    /// <summary>Initializes a new instance of the <see cref="NonOwningReadStream"/> class.</summary>
    /// <param name="inner">The ASP.NET-owned request body stream.</param>
    internal NonOwningReadStream(Stream inner)
    {
        ArgumentNullException.ThrowIfNull(inner);
        BorrowedInner = inner;
    }

    /// <inheritdoc/>
    public override bool CanRead => BorrowedInner.CanRead;

    /// <inheritdoc/>
    public override bool CanSeek => BorrowedInner.CanSeek;

    /// <inheritdoc/>
    public override bool CanWrite => false;

    /// <inheritdoc/>
    public override long Length => BorrowedInner.Length;

    /// <inheritdoc/>
    public override long Position
    {
        get => BorrowedInner.Position;
        set => BorrowedInner.Position = value;
    }

    /// <summary>Gets the ASP.NET-owned request body stream.</summary>
    private Stream BorrowedInner { get; }

    /// <inheritdoc/>
    public override void Flush() =>
        BorrowedInner.Flush();

    /// <inheritdoc/>
    public override Task FlushAsync(CancellationToken cancellationToken) =>
        BorrowedInner.FlushAsync(cancellationToken);

    /// <inheritdoc/>
    public override int Read(byte[] buffer, int offset, int count) =>
        BorrowedInner.Read(buffer, offset, count);

    /// <inheritdoc/>
    public override int Read(Span<byte> buffer) =>
        BorrowedInner.Read(buffer);

    /// <inheritdoc/>
    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        BorrowedInner.ReadAsync(buffer, offset, count, cancellationToken);

    /// <inheritdoc/>
    public override ValueTask<int> ReadAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken = default) =>
        BorrowedInner.ReadAsync(buffer, cancellationToken);

    /// <inheritdoc/>
    public override long Seek(long offset, SeekOrigin origin) =>
        BorrowedInner.Seek(offset, origin);

    /// <inheritdoc/>
    public override void SetLength(long value) =>
        throw new NotSupportedException();

    /// <inheritdoc/>
    public override void Write(byte[] buffer, int offset, int count) =>
        throw new NotSupportedException();
}
