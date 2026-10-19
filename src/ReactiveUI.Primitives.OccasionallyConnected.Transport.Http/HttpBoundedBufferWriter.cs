// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Buffers;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Writes UTF-8 protocol bytes while enforcing an exact byte limit.</summary>
internal sealed class HttpBoundedBufferWriter : IBufferWriter<byte>
{
    /// <summary>The default initial buffer length.</summary>
    private const int InitialBufferLength = 256;

    /// <summary>The factor used when expanding the bounded serialization buffer.</summary>
    private const int BufferGrowthFactor = 2;

    /// <summary>The scratch space allowed for the JSON writer's fixed-size growth requests.</summary>
    private const int JsonWriterScratchBytes = 4096;

    /// <summary>The maximum number of bytes this writer can store.</summary>
    private readonly int _maximumLength;

    /// <summary>The bounded allocation limit, separate from the committed byte limit.</summary>
    private readonly int _maximumBufferLength;

    /// <summary>The backing buffer.</summary>
    private byte[] _buffer;

    /// <summary>The number of committed bytes.</summary>
    private int _written;

    /// <summary>Initializes a new instance of the <see cref="HttpBoundedBufferWriter"/> class.</summary>
    /// <param name="maximumLength">The maximum byte count.</param>
    internal HttpBoundedBufferWriter(int maximumLength)
    {
        _maximumLength = maximumLength;
        _maximumBufferLength = (int)Math.Min(int.MaxValue, (long)maximumLength + JsonWriterScratchBytes);
        _buffer = new byte[Math.Min(maximumLength, InitialBufferLength)];
    }

    /// <inheritdoc/>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
    /// <exception cref="HttpRemoteTransportException"><paramref name="count"/> exceeds the configured byte limit.</exception>
    public void Advance(int count)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(count);
        if (count > _maximumLength - _written)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        _written += count;
    }

    /// <inheritdoc/>
    /// <exception cref="HttpRemoteTransportException"><paramref name="sizeHint"/> cannot fit within the bounded allocation limit.</exception>
    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(_written);
    }

    /// <inheritdoc/>
    /// <exception cref="HttpRemoteTransportException"><paramref name="sizeHint"/> cannot fit within the bounded allocation limit.</exception>
    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(_written);
    }

    /// <summary>Copies the written bytes into a right-sized array.</summary>
    /// <returns>The copied bytes.</returns>
    internal byte[] ToArray()
    {
        var payload = new byte[_written];
        Array.Copy(_buffer, payload, _written);
        return payload;
    }

    /// <summary>Ensures a contiguous write span is available.</summary>
    /// <param name="sizeHint">The requested span size.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="sizeHint"/> is negative.</exception>
    /// <exception cref="HttpRemoteTransportException"><paramref name="sizeHint"/> cannot fit within the bounded allocation limit.</exception>
    private void EnsureCapacity(int sizeHint)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(sizeHint);
        var requiredHint = sizeHint == 0 ? 1 : sizeHint;
        if (_written == _maximumLength || requiredHint > _maximumBufferLength - _written)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        var requiredLength = _written + requiredHint;
        if (requiredLength <= _buffer.Length)
        {
            return;
        }

        var doubledLength = (int)Math.Min((long)_buffer.Length * BufferGrowthFactor, _maximumBufferLength);
        var nextLength = Math.Max(requiredLength, doubledLength);
        Array.Resize(ref _buffer, nextLength);
    }
}
