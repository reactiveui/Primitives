// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Crdt;

/// <summary>Encodes and decodes built-in CRDT contracts using a deterministic bounded binary format.</summary>
public static partial class CrdtCodec
{
    /// <summary>Reads bounded CRDT bytes.</summary>
    internal sealed class Reader
    {
        /// <summary>The number of bytes in a GUID.</summary>
        private const int GuidLength = 16;

        /// <summary>The minimum byte budget reserved per item when checking count headers.</summary>
        private const int MinimumCountItemBytes = 1;

        /// <summary>The encoded payload bytes.</summary>
        private readonly byte[] _payload;

        /// <summary>The CRDT bounds.</summary>
        private readonly CrdtBounds _bounds;

        /// <summary>The current read offset.</summary>
        private int _offset;

        /// <summary>Initializes a new instance of the <see cref="Reader"/> class.</summary>
        /// <param name="payload">The payload.</param>
        /// <param name="bounds">The bounds.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        public Reader(ReadOnlyMemory<byte> payload, CrdtBounds bounds)
        {
            bounds.Validate();
            if (payload.Length > bounds.MaximumEncodedBytes)
            {
                throw new InvalidOperationException("The CRDT encoded payload exceeds configured bounds.");
            }

            _payload = payload.ToArray();
            _bounds = bounds;
            _offset = 0;
        }

        /// <summary>Reads and validates the payload header.</summary>
        /// <param name="expectedPayloadType">The expectedPayloadType.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal void ReadHeader(byte expectedPayloadType)
        {
            if (ReadByte() != Magic0 || ReadByte() != Magic1 || ReadByte() != Magic2 || ReadByte() != Magic3)
            {
                throw new InvalidOperationException("The CRDT payload magic is invalid.");
            }

            if (ReadByte() != Version)
            {
                throw new InvalidOperationException("The CRDT payload version is not supported.");
            }

            if (ReadByte() == expectedPayloadType)
            {
                return;
            }

            throw new InvalidOperationException("The CRDT payload type is invalid.");
        }

        /// <summary>Reads an input.</summary>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal CrdtInput ReadInput()
        {
            var kind = (CrdtInputKind)ReadByte();
            if (kind == CrdtInputKind.Mutation)
            {
                return CrdtInput.ForMutation(ReadMutation());
            }

            if (kind != CrdtInputKind.AuthoritativeState)
            {
                throw new InvalidOperationException("The CRDT input kind is not supported.");
            }

            return CrdtInput.ForAuthoritativeState(ReadStateBody());
        }

        /// <summary>Reads a state body.</summary>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal CrdtState ReadStateBody()
        {
            var kind = (CrdtKind)ReadByte();
            return new()
            {
                Kind = kind,
                GCounterComponents = ReadComponents(),
                PNCounterPositiveComponents = ReadComponents(),
                PNCounterNegativeComponents = ReadComponents(),
                DotBindings = ReadDotElements(_bounds.MaximumDotBindings),
                Tombstones = ReadDotElements(_bounds.MaximumTombstones),
                RegisterValue = ReadBytes(_bounds.MaximumRegisterBytes),
                RegisterStamp = ReadStamp(),
            };
        }

        /// <summary>Reads a mutation body.</summary>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal CrdtMutation ReadMutation()
        {
            var kind = (CrdtMutationKind)ReadByte();
            return new()
            {
                Kind = kind,
                ActorId = ReadOptionalString(),
                GCounterComponent = ReadInt64(),
                PNCounterPositiveComponent = ReadInt64(),
                PNCounterNegativeComponent = ReadInt64(),
                Bytes = ReadBytes(_bounds.MaximumRegisterBytes),
                ObservedDots = ReadDots(_bounds.MaximumTombstones),
                RegisterStamp = ReadStamp(),
            };
        }

        /// <summary>Rejects unread trailing bytes.</summary>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal void ThrowIfTrailingData()
        {
            if (_offset == _payload.Length)
            {
                return;
            }

            throw new InvalidOperationException("The CRDT payload contains trailing data.");
        }

        /// <summary>Compares dot elements by dot and element bytes.</summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private static int CompareDotElements(CrdtDotElement left, CrdtDotElement right)
        {
            var dot = left.Dot.CompareTo(right.Dot);
            return dot != 0 ? dot : CompareBytes(left.ElementSpan, right.ElementSpan);
        }

        /// <summary>Compares byte spans lexicographically.</summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private static int CompareBytes(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
        {
            var shared = Math.Min(left.Length, right.Length);
            for (var index = 0; index < shared; index++)
            {
                var comparison = left[index].CompareTo(right[index]);
                if (comparison != 0)
                {
                    return comparison;
                }
            }

            return left.Length.CompareTo(right.Length);
        }

        /// <summary>Reads canonical ordered counter components.</summary>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private Dictionary<string, long> ReadComponents()
        {
            var count = ReadCount(_bounds.MaximumCounterComponents);
            Dictionary<string, long> result = [with(capacity: count, comparer: StringComparer.Ordinal)];
            string? previous = null;
            for (var index = 0; index < count; index++)
            {
                var key = ReadString();
                if (previous is not null && string.CompareOrdinal(previous, key) >= 0)
                {
                    throw new InvalidOperationException("The CRDT component keys are not canonical.");
                }

                previous = key;
                result.Add(key, ReadInt64());
            }

            return result;
        }

        /// <summary>Reads canonical ordered dot elements.</summary>
        /// <param name="maximumCount">The maximumCount.</param>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private List<CrdtDotElement> ReadDotElements(int maximumCount)
        {
            var count = ReadCount(maximumCount);
            List<CrdtDotElement> result = [with(capacity: count)];
            CrdtDotElement? previous = null;
            for (var index = 0; index < count; index++)
            {
                var item = new CrdtDotElement { Dot = ReadDot(), Element = ReadBytes(_bounds.MaximumElementBytes) };
                if (previous is not null && CompareDotElements(previous, item) >= 0)
                {
                    throw new InvalidOperationException("The CRDT dot bindings are not canonical.");
                }

                previous = item;
                result.Add(item);
            }

            return result;
        }

        /// <summary>Reads canonical ordered dots.</summary>
        /// <param name="maximumCount">The maximumCount.</param>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private List<CrdtDot> ReadDots(int maximumCount)
        {
            var count = ReadCount(maximumCount);
            List<CrdtDot> result = [with(capacity: count)];
            CrdtDot? previous = null;
            for (var index = 0; index < count; index++)
            {
                var dot = ReadDot();
                if (previous is not null && previous.CompareTo(dot) >= 0)
                {
                    throw new InvalidOperationException("The CRDT observed dots are not canonical.");
                }

                previous = dot;
                result.Add(dot);
            }

            return result;
        }

        /// <summary>Reads one dot.</summary>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private CrdtDot ReadDot() => new() { ClientId = ReadString(), ClientSequence = ReadInt64() };

        /// <summary>Reads an optional write stamp.</summary>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private ConflictWriteStamp? ReadStamp()
        {
            var present = ReadByte();
            if (present == 0)
            {
                return null;
            }

            if (present != 1)
            {
                throw new InvalidOperationException("The CRDT write stamp marker is invalid.");
            }

            var utcTicks = ReadInt64();
            var offsetMinutes = ReadInt16();
            var utc = new DateTimeOffset(utcTicks, TimeSpan.Zero);
            return new() { CommittedAtUtc = utc.ToOffset(TimeSpan.FromMinutes(offsetMinutes)), ClientId = ReadString(), OperationId = new(ReadGuid()) };
        }

        /// <summary>Reads an optional string.</summary>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private string? ReadOptionalString()
        {
            var present = ReadByte();
            if (present == 0)
            {
                return null;
            }

            if (present == 1)
            {
                return ReadString();
            }

            throw new InvalidOperationException("The CRDT optional string marker is invalid.");
        }

        /// <summary>Reads a bounded UTF-8 string.</summary>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private string ReadString()
        {
            var length = ReadLength(_bounds.MaximumClientIdUtf8Bytes);
            var bytes = ReadRaw(length);
            try
            {
#if NETFRAMEWORK
                return StrictUtf8.GetString(bytes.ToArray());
#else
                return StrictUtf8.GetString(bytes);
#endif
            }
            catch (DecoderFallbackException exception)
            {
                throw new InvalidOperationException("The CRDT string is not valid UTF-8.", exception);
            }
        }

        /// <summary>Reads bounded bytes.</summary>
        /// <param name="maximumLength">The maximumLength.</param>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private byte[] ReadBytes(int maximumLength)
        {
            var length = ReadLength(maximumLength);
            var bytes = ReadRaw(length);
            var copy = new byte[length];
            bytes.CopyTo(copy);
            return copy;
        }

        /// <summary>Reads one byte.</summary>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private byte ReadByte()
        {
            EnsureAvailable(1);
            var value = _payload[_offset];
            _offset++;
            return value;
        }

        /// <summary>Reads a big-endian 16-bit integer.</summary>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private short ReadInt16()
        {
            EnsureAvailable(Int16ByteCount);
            short value = 0;
            for (var index = 0; index < Int16ByteCount; index++)
            {
                value = (short)((value << BitsPerByte) | _payload[_offset + index]);
            }

            _offset += Int16ByteCount;
            return value;
        }

        /// <summary>Reads a big-endian 32-bit integer.</summary>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private int ReadInt32()
        {
            EnsureAvailable(Int32ByteCount);
            var value = 0;
            for (var index = 0; index < Int32ByteCount; index++)
            {
                value = (value << BitsPerByte) | _payload[_offset + index];
            }

            _offset += Int32ByteCount;
            return value;
        }

        /// <summary>Reads a big-endian 64-bit integer.</summary>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private long ReadInt64()
        {
            EnsureAvailable(Int64ByteCount);
            long value = 0;
            for (var index = 0; index < Int64ByteCount; index++)
            {
                value = (value << BitsPerByte) | _payload[_offset + index];
            }

            _offset += Int64ByteCount;
            return value;
        }

        /// <summary>Reads a GUID.</summary>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private Guid ReadGuid()
        {
            var bytes = ReadRaw(GuidLength);
#if NETFRAMEWORK
            return new(bytes.ToArray());
#else
            return new(bytes);
#endif
        }

        /// <summary>Reads a bounded count.</summary>
        /// <param name="maximumCount">The maximumCount.</param>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private int ReadCount(int maximumCount)
        {
            var count = ReadInt32();
            if (count < 0 || count > maximumCount || count > GetRemaining() / MinimumCountItemBytes)
            {
                throw new InvalidOperationException("The CRDT count exceeds configured or remaining bounds.");
            }

            return count;
        }

        /// <summary>Reads a bounded length.</summary>
        /// <param name="maximumLength">The maximumLength.</param>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private int ReadLength(int maximumLength)
        {
            var length = ReadInt32();
            if (length < 0 || length > maximumLength || length > GetRemaining())
            {
                throw new InvalidOperationException("The CRDT byte length exceeds configured or remaining bounds.");
            }

            return length;
        }

        /// <summary>Reads raw bytes.</summary>
        /// <param name="length">The length.</param>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private ReadOnlySpan<byte> ReadRaw(int length)
        {
            EnsureAvailable(length);
            var span = _payload.AsSpan(_offset, length);
            _offset += length;
            return span;
        }

        /// <summary>Gets the remaining byte count.</summary>
        /// <returns>The result.</returns>
        private int GetRemaining() => _payload.Length - _offset;

        /// <summary>Ensures the payload has enough remaining bytes.</summary>
        /// <param name="count">The count.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private void EnsureAvailable(int count)
        {
            if (count <= GetRemaining())
            {
                return;
            }

            throw new InvalidOperationException("The CRDT payload ended unexpectedly.");
        }
    }
}
