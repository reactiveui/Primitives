// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Crdt;

/// <summary>Encodes and decodes built-in CRDT contracts using a deterministic bounded binary format.</summary>
public static partial class CrdtCodec
{
    /// <summary>Writes bounded CRDT bytes.</summary>
    internal sealed class Writer
    {
        /// <summary>The initial writer buffer length.</summary>
        private const int InitialWriterBytes = 256;

        /// <summary>The writer growth factor.</summary>
        private const int BufferGrowthFactor = 2;

        /// <summary>The CRDT bounds.</summary>
        private readonly CrdtBounds _bounds;

        /// <summary>The mutable output buffer.</summary>
        private byte[] _buffer;

        /// <summary>The count of bytes written.</summary>
        private int _written;

        /// <summary>Initializes a new instance of the <see cref="Writer"/> class.</summary>
        /// <param name="bounds">The bounds.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        public Writer(CrdtBounds bounds)
        {
            bounds.Validate();
            _bounds = bounds;
            _buffer = new byte[Math.Min(InitialWriterBytes, bounds.MaximumEncodedBytes)];
        }

        /// <summary>Writes the CRDT header.</summary>
        /// <param name="payloadType">The payloadType.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal void WriteHeader(byte payloadType)
        {
            WriteByte(Magic0);
            WriteByte(Magic1);
            WriteByte(Magic2);
            WriteByte(Magic3);
            WriteByte(Version);
            WriteByte(payloadType);
        }

        /// <summary>Writes an input body.</summary>
        /// <param name="input">The input.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal void WriteInput(CrdtInput input)
        {
            WriteByte((byte)input.Kind);
            if (input.Kind == CrdtInputKind.Mutation)
            {
                var mutation = input.Mutation;
                ArgumentExceptionHelper.ThrowIfNull(mutation);
                WriteMutation(mutation);
                return;
            }

            var state = input.State;
            ArgumentExceptionHelper.ThrowIfNull(state);
            WriteStateBody(state);
        }

        /// <summary>Writes a state body.</summary>
        /// <param name="state">The state.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal void WriteStateBody(CrdtState state)
        {
            WriteByte((byte)state.Kind);
            WriteComponents(state.GCounterComponents);
            WriteComponents(state.PNCounterPositiveComponents);
            WriteComponents(state.PNCounterNegativeComponents);
            WriteDotElements(state.DotBindings);
            WriteDotElements(state.Tombstones);
            WriteBytes(state.RegisterValueSpan);
            WriteStamp(state.RegisterStamp);
        }

        /// <summary>Writes a mutation body.</summary>
        /// <param name="mutation">The mutation.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal void WriteMutation(CrdtMutation mutation)
        {
            WriteByte((byte)mutation.Kind);
            WriteOptionalString(mutation.ActorId);
            WriteInt64(mutation.GCounterComponent);
            WriteInt64(mutation.PNCounterPositiveComponent);
            WriteInt64(mutation.PNCounterNegativeComponent);
            WriteBytes(mutation.ByteSpan);
            WriteDots(mutation.ObservedDots);
            WriteStamp(mutation.RegisterStamp);
        }

        /// <summary>Writes canonical ordered counter components.</summary>
        /// <param name="components">The components.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal void WriteComponents(IReadOnlyDictionary<string, long> components)
        {
            List<KeyValuePair<string, long>> ordered = [with(capacity: components.Count)];
            foreach (var pair in components)
            {
                ordered.Add(new(pair.Key, pair.Value));
            }

            ordered.Sort(static (left, right) => string.CompareOrdinal(left.Key, right.Key));
            WriteInt32(ordered.Count);
            for (var index = 0; index < ordered.Count; index++)
            {
                WriteString(ordered[index].Key);
                WriteInt64(ordered[index].Value);
            }
        }

        /// <summary>Writes canonical ordered dot element bindings.</summary>
        /// <param name="elements">The elements.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal void WriteDotElements(IReadOnlyList<CrdtDotElement> elements)
        {
            List<CrdtDotElement> ordered = [with(capacity: elements.Count)];
            for (var index = 0; index < elements.Count; index++)
            {
                ordered.Add(elements[index]);
            }

            ordered.Sort(CompareDotElements);
            WriteInt32(ordered.Count);
            for (var index = 0; index < ordered.Count; index++)
            {
                WriteDot(ordered[index].Dot);
                WriteBytes(ordered[index].ElementSpan);
            }
        }

        /// <summary>Writes canonical ordered dots.</summary>
        /// <param name="dots">The dots.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal void WriteDots(IReadOnlyList<CrdtDot> dots)
        {
            List<CrdtDot> ordered = [with(capacity: dots.Count)];
            for (var index = 0; index < dots.Count; index++)
            {
                ordered.Add(dots[index]);
            }

            ordered.Sort(static (left, right) => left.CompareTo(right));
            WriteInt32(ordered.Count);
            for (var index = 0; index < ordered.Count; index++)
            {
                WriteDot(ordered[index]);
            }
        }

        /// <summary>Writes an optional write stamp.</summary>
        /// <param name="stamp">The stamp.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal void WriteStamp(ConflictWriteStamp? stamp)
        {
            WriteByte(stamp is null ? (byte)0 : (byte)1);
            if (stamp is null)
            {
                return;
            }

            WriteInt64(stamp.CommittedAtUtc.UtcDateTime.Ticks);
            WriteInt16(checked((short)stamp.CommittedAtUtc.Offset.TotalMinutes));
            WriteString(stamp.ClientId);
            WriteGuid(stamp.OperationId.Value);
        }

        /// <summary>Writes one byte.</summary>
        /// <param name="value">The value.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal void WriteByte(byte value)
        {
            Ensure(1);
            _buffer[_written] = value;
            _written++;
        }

        /// <summary>Writes a big-endian 16-bit integer.</summary>
        /// <param name="value">The value.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal void WriteInt16(short value)
        {
            Ensure(Int16ByteCount);
            for (var shift = (Int16ByteCount - 1) * BitsPerByte; shift >= 0; shift -= BitsPerByte)
            {
                _buffer[_written] = (byte)((value >> shift) & byte.MaxValue);
                _written++;
            }
        }

        /// <summary>Writes a big-endian 32-bit integer.</summary>
        /// <param name="value">The value.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal void WriteInt32(int value)
        {
            Ensure(Int32ByteCount);
            for (var shift = (Int32ByteCount - 1) * BitsPerByte; shift >= 0; shift -= BitsPerByte)
            {
                _buffer[_written] = (byte)((value >> shift) & byte.MaxValue);
                _written++;
            }
        }

        /// <summary>Writes a big-endian 64-bit integer.</summary>
        /// <param name="value">The value.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal void WriteInt64(long value)
        {
            Ensure(Int64ByteCount);
            for (var shift = (Int64ByteCount - 1) * BitsPerByte; shift >= 0; shift -= BitsPerByte)
            {
                _buffer[_written] = (byte)((value >> shift) & byte.MaxValue);
                _written++;
            }
        }

        /// <summary>Writes a UTF-8 identity already checked by state or input validation.</summary>
        /// <param name="value">The value.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal void WriteString(string value)
        {
            var bytes = StrictUtf8.GetBytes(value);
            WriteInt32(bytes.Length);
            WriteRaw(bytes);
        }

        /// <summary>Writes a nullable string marker and value.</summary>
        /// <param name="value">The value.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal void WriteOptionalString(string? value)
        {
            WriteByte(value is null ? (byte)0 : (byte)1);
            if (value is null)
            {
                return;
            }

            WriteString(value);
        }

        /// <summary>Writes length-prefixed bytes.</summary>
        /// <param name="value">The value.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal void WriteBytes(ReadOnlySpan<byte> value)
        {
            WriteInt32(value.Length);
            WriteRaw(value);
        }

        /// <summary>Returns the written bytes.</summary>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        internal byte[] ToArray()
        {
            var result = new byte[_written];
            Array.Copy(_buffer, result, _written);
            return result;
        }

        /// <summary>Compares dot elements by dot and element bytes.</summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns>The result.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int CompareDotElements(CrdtDotElement left, CrdtDotElement right) =>
            left.Dot.CompareTo(right.Dot);

        /// <summary>Writes a dot.</summary>
        /// <param name="dot">The dot.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private void WriteDot(CrdtDot dot)
        {
            WriteString(dot.ClientId);
            WriteInt64(dot.ClientSequence);
        }

        /// <summary>Writes a GUID using the runtime round-trip byte order.</summary>
        /// <param name="value">The value.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private void WriteGuid(Guid value)
        {
            var bytes = value.ToByteArray();
            WriteRaw(bytes);
        }

        /// <summary>Writes raw bytes.</summary>
        /// <param name="source">The source.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private void WriteRaw(ReadOnlySpan<byte> source)
        {
            Ensure(source.Length);
            source.CopyTo(_buffer.AsSpan(_written));
            _written += source.Length;
        }

        /// <summary>Ensures the writer has capacity for a write.</summary>
        /// <param name="count">The count.</param>
        /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
        private void Ensure(int count)
        {
            if (count > _bounds.MaximumEncodedBytes - _written)
            {
                throw new InvalidOperationException("The CRDT encoded payload exceeds configured bounds.");
            }

            var required = _written + count;
            if (required <= _buffer.Length)
            {
                return;
            }

            var next = _buffer.Length;
            while (next < required)
            {
                next = checked(next * BufferGrowthFactor);
            }

            if (next > _bounds.MaximumEncodedBytes)
            {
                next = _bounds.MaximumEncodedBytes;
            }

            Array.Resize(ref _buffer, next);
        }
    }
}
