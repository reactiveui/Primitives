// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Computes a bounded canonical fingerprint for one authenticated operation intent.</summary>
internal static class CanonicalOperationFingerprint
{
    /// <summary>The reusable hash staging buffer length.</summary>
    private const int HashBufferLength = 256;

    /// <summary>The encoded length of an operation identifier.</summary>
    private const int GuidLength = 16;

    /// <summary>The encoded length of a 32-bit scalar.</summary>
    private const int Int32Length = 4;

    /// <summary>The encoded length of a 64-bit scalar.</summary>
    private const int Int64Length = 8;

    /// <summary>The encoded length of a Boolean presence marker.</summary>
    private const int BooleanLength = 1;

    /// <summary>The number of scalar fields in the operation policy.</summary>
    private const int PolicyFieldCount = 4;

    /// <summary>The number of bits in one byte.</summary>
    private const int BitsPerByte = 8;

    /// <summary>The third byte index in a 32-bit scalar.</summary>
    private const int ThirdByteIndex = 2;

    /// <summary>The fourth byte index in a 32-bit scalar.</summary>
    private const int FourthByteIndex = 3;

    /// <summary>The canonical domain separator and wire-format version.</summary>
    private static readonly byte[] DomainVersion = "ReactiveUI.Primitives.OccasionallyConnected.CanonicalOperationFingerprint/v1"u8.ToArray();

    /// <summary>The strict UTF-8 encoder used by the canonical wire format.</summary>
    private static readonly Encoding CanonicalEncoding = new UTF8Encoding(false, true);

    /// <summary>Computes an owned SHA-256 fingerprint of an authenticated operation intent.</summary>
    /// <param name="authenticatedTenant">The authenticated tenant scope.</param>
    /// <param name="authenticatedClient">The authenticated client scope.</param>
    /// <param name="operation">The operation intent to fingerprint.</param>
    /// <param name="maximumEncodedBytes">The inclusive maximum canonical byte count.</param>
    /// <returns>The owned SHA-256 fingerprint.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="operation"/> or a required operation member is null.</exception>
    /// <exception cref="ArgumentException">An identifier is blank or the canonical byte count exceeds <paramref name="maximumEncodedBytes"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maximumEncodedBytes"/> is not positive.</exception>
    /// <exception cref="EncoderFallbackException">Canonical text contains malformed UTF-16.</exception>
    internal static byte[] Compute(string authenticatedTenant, string authenticatedClient, SyncOperation operation, int maximumEncodedBytes)
    {
        ArgumentExceptionHelper.ThrowIfNull(operation);
        if (maximumEncodedBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maximumEncodedBytes), maximumEncodedBytes, null);
        }

        ValidateIdentifier(authenticatedTenant, nameof(authenticatedTenant));
        ValidateIdentifier(authenticatedClient, nameof(authenticatedClient));
        PreflightCanonicalOperation(authenticatedTenant, authenticatedClient, operation, maximumEncodedBytes);

        using var hash = new CanonicalHash();
        var buffer = new byte[HashBufferLength];
        WriteCanonicalOperation(hash, buffer, authenticatedTenant, authenticatedClient, operation);
        return hash.GetHashAndReset();
    }

    /// <summary>Validates the authenticated identity scope.</summary>
    /// <param name="value">The identity value.</param>
    /// <param name="parameterName">The source parameter name.</param>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="value"/> is blank.</exception>
    /// <exception cref="EncoderFallbackException"><paramref name="value"/> contains malformed UTF-16.</exception>
    private static void ValidateIdentifier(string value, string parameterName)
    {
        ArgumentExceptionHelper.ThrowIfNull(value, parameterName);
        _ = CanonicalEncoding.GetByteCount(value);
        if (!string.IsNullOrWhiteSpace(value))
        {
            return;
        }

        throw new ArgumentException("Authenticated identity is invalid.", parameterName);
    }

    /// <summary>Checks the canonical byte count before allocating buffers, sorting metadata, or copying payload bytes.</summary>
    /// <param name="authenticatedTenant">The authenticated tenant scope.</param>
    /// <param name="authenticatedClient">The authenticated client scope.</param>
    /// <param name="operation">The operation intent to fingerprint.</param>
    /// <param name="maximumEncodedBytes">The inclusive maximum canonical byte count.</param>
    /// <exception cref="ArgumentNullException">A required operation member is null.</exception>
    /// <exception cref="ArgumentException">The canonical byte count exceeds <paramref name="maximumEncodedBytes"/>.</exception>
    /// <exception cref="EncoderFallbackException">Canonical text contains malformed UTF-16.</exception>
    private static void PreflightCanonicalOperation(
        string authenticatedTenant,
        string authenticatedClient,
        SyncOperation operation,
        int maximumEncodedBytes)
    {
        var payload = operation.Payload;
        var policy = operation.Policy;
        var metadata = operation.Metadata;
        ArgumentExceptionHelper.ThrowIfNull(payload, nameof(operation.Payload));
        ArgumentExceptionHelper.ThrowIfNull(policy, nameof(operation.Policy));
        ArgumentExceptionHelper.ThrowIfNull(metadata, nameof(operation.Metadata));

        var remaining = maximumEncodedBytes;
        Consume(ref remaining, Int32Length + DomainVersion.Length);
        ConsumeText(ref remaining, authenticatedTenant, nameof(authenticatedTenant));
        ConsumeText(ref remaining, authenticatedClient, nameof(authenticatedClient));
        Consume(ref remaining, GuidLength);
        ConsumeText(ref remaining, operation.StreamId.Value, nameof(operation.StreamId));
        Consume(ref remaining, Int64Length);
        ConsumeOptionalText(ref remaining, operation.BaseVersion, nameof(operation.BaseVersion));
        Consume(ref remaining, Int32Length);
        ConsumePayload(ref remaining, payload);
        Consume(ref remaining, Int32Length * PolicyFieldCount);
        ConsumeMetadata(ref remaining, metadata);
    }

    /// <summary>Writes the canonical operation byte stream into the supplied hash.</summary>
    /// <param name="hash">The incremental hash.</param>
    /// <param name="buffer">The reusable staging buffer.</param>
    /// <param name="authenticatedTenant">The authenticated tenant scope.</param>
    /// <param name="authenticatedClient">The authenticated client scope.</param>
    /// <param name="operation">The operation intent to fingerprint.</param>
    private static void WriteCanonicalOperation(
        CanonicalHash hash,
        byte[] buffer,
        string authenticatedTenant,
        string authenticatedClient,
        SyncOperation operation)
    {
        AppendInt32(hash, buffer, DomainVersion.Length);
        AppendBytes(hash, DomainVersion);
        AppendText(hash, buffer, authenticatedTenant);
        AppendText(hash, buffer, authenticatedClient);
        AppendBytes(hash, operation.OperationId.Value.ToByteArray());
        AppendText(hash, buffer, operation.StreamId.Value);
        AppendInt64(hash, buffer, operation.ClientSequence);
        AppendOptionalText(hash, buffer, operation.BaseVersion);
        AppendInt32(hash, buffer, (int)operation.Type);
        WritePayload(hash, buffer, operation.Payload);
        AppendInt32(hash, buffer, (int)operation.Policy.DeliveryGuarantee);
        AppendInt32(hash, buffer, (int)operation.Policy.Durability);
        AppendInt32(hash, buffer, operation.Policy.Priority);
        AppendInt32(hash, buffer, (int)operation.Policy.ConflictPolicy);
        WriteMetadata(hash, buffer, operation.Metadata);
    }

    /// <summary>Accounts for the payload envelope and actual payload bytes.</summary>
    /// <param name="remaining">The remaining byte budget.</param>
    /// <param name="payload">The payload envelope.</param>
    /// <exception cref="ArgumentException">The payload exceeds the byte budget.</exception>
    /// <exception cref="EncoderFallbackException">Payload metadata contains malformed UTF-16.</exception>
    private static void ConsumePayload(ref int remaining, PayloadEnvelope payload)
    {
        ConsumeText(ref remaining, payload.ContractId, nameof(payload.ContractId));
        Consume(ref remaining, Int32Length);
        ConsumeText(ref remaining, payload.ContentType, nameof(payload.ContentType));
        ConsumeText(ref remaining, payload.PayloadHash, nameof(payload.PayloadHash));
        Consume(ref remaining, Int32Length);
        Consume(ref remaining, payload.PayloadLength);
    }

    /// <summary>Accounts for metadata entries without depending on insertion order.</summary>
    /// <param name="remaining">The remaining byte budget.</param>
    /// <param name="metadata">The operation metadata.</param>
    /// <exception cref="ArgumentException">Metadata exceeds the byte budget.</exception>
    /// <exception cref="EncoderFallbackException">Metadata contains malformed UTF-16.</exception>
    private static void ConsumeMetadata(ref int remaining, IReadOnlyDictionary<string, string> metadata)
    {
        Consume(ref remaining, Int32Length);
        foreach (var entry in metadata)
        {
            ConsumeText(ref remaining, entry.Key, nameof(metadata));
            ConsumeText(ref remaining, entry.Value, nameof(metadata));
        }
    }

    /// <summary>Accounts for optional text and its null marker.</summary>
    /// <param name="remaining">The remaining byte budget.</param>
    /// <param name="value">The optional text.</param>
    /// <param name="parameterName">The source parameter name.</param>
    /// <exception cref="ArgumentException">The value exceeds the byte budget.</exception>
    /// <exception cref="EncoderFallbackException"><paramref name="value"/> contains malformed UTF-16.</exception>
    private static void ConsumeOptionalText(ref int remaining, string? value, string parameterName)
    {
        Consume(ref remaining, BooleanLength);
        if (value is null)
        {
            return;
        }

        ConsumeText(ref remaining, value, parameterName);
    }

    /// <summary>Accounts for length-prefixed strict UTF-8 text.</summary>
    /// <param name="remaining">The remaining byte budget.</param>
    /// <param name="value">The text value.</param>
    /// <param name="parameterName">The source parameter name.</param>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is null.</exception>
    /// <exception cref="ArgumentException">The value exceeds the byte budget.</exception>
    /// <exception cref="EncoderFallbackException"><paramref name="value"/> contains malformed UTF-16.</exception>
    private static void ConsumeText(ref int remaining, string value, string parameterName)
    {
        ArgumentExceptionHelper.ThrowIfNull(value, parameterName);
        Consume(ref remaining, Int32Length);
        Consume(ref remaining, CanonicalEncoding.GetByteCount(value));
    }

    /// <summary>Accounts for a known non-negative byte count.</summary>
    /// <param name="remaining">The remaining byte budget.</param>
    /// <param name="count">The byte count to consume.</param>
    /// <exception cref="ArgumentException"><paramref name="count"/> exceeds the remaining byte budget.</exception>
    private static void Consume(ref int remaining, int count)
    {
        if (count <= remaining)
        {
            remaining -= count;
            return;
        }

        throw new ArgumentException("Canonical operation size exceeds the configured maximum.", "maximumEncodedBytes");
    }

    /// <summary>Writes the payload envelope and actual payload bytes.</summary>
    /// <param name="hash">The incremental hash.</param>
    /// <param name="buffer">The reusable staging buffer.</param>
    /// <param name="payload">The payload envelope.</param>
    private static void WritePayload(CanonicalHash hash, byte[] buffer, PayloadEnvelope payload)
    {
        AppendText(hash, buffer, payload.ContractId);
        AppendInt32(hash, buffer, payload.SchemaVersion);
        AppendText(hash, buffer, payload.ContentType);
        AppendText(hash, buffer, payload.PayloadHash);
        AppendInt32(hash, buffer, payload.PayloadLength);
#if NET5_0_OR_GREATER
        hash.AppendData(payload.Payload.Span);
#else
        AppendMemory(hash, buffer, payload.Payload);
#endif
    }

    /// <summary>Writes metadata entries in exact ordinal key order.</summary>
    /// <param name="hash">The incremental hash.</param>
    /// <param name="buffer">The reusable staging buffer.</param>
    /// <param name="metadata">The operation metadata.</param>
    private static void WriteMetadata(CanonicalHash hash, byte[] buffer, IReadOnlyDictionary<string, string> metadata)
    {
        var sortedMetadata = new KeyValuePair<string, string>[metadata.Count];
        var index = 0;
        foreach (var entry in metadata)
        {
            sortedMetadata[index] = entry;
            index++;
        }

        Array.Sort(sortedMetadata, static (left, right) => StringComparer.Ordinal.Compare(left.Key, right.Key));
        AppendInt32(hash, buffer, sortedMetadata.Length);
        foreach (var entry in sortedMetadata)
        {
            AppendText(hash, buffer, entry.Key);
            AppendText(hash, buffer, entry.Value);
        }
    }

    /// <summary>Appends a 32-bit scalar in little-endian order.</summary>
    /// <param name="hash">The incremental hash.</param>
    /// <param name="buffer">The reusable staging buffer.</param>
    /// <param name="value">The scalar value.</param>
    private static void AppendInt32(CanonicalHash hash, byte[] buffer, int value)
    {
        buffer[0] = (byte)value;
        buffer[1] = (byte)(value >> BitsPerByte);
        buffer[ThirdByteIndex] = (byte)(value >> (BitsPerByte * ThirdByteIndex));
        buffer[FourthByteIndex] = (byte)(value >> (BitsPerByte * FourthByteIndex));
        hash.AppendData(buffer, 0, Int32Length);
    }

    /// <summary>Appends a 64-bit scalar in little-endian order.</summary>
    /// <param name="hash">The incremental hash.</param>
    /// <param name="buffer">The reusable staging buffer.</param>
    /// <param name="value">The scalar value.</param>
    private static void AppendInt64(CanonicalHash hash, byte[] buffer, long value)
    {
        for (var index = 0; index < Int64Length; index++)
        {
            buffer[index] = (byte)(value >> (index * BitsPerByte));
        }

        hash.AppendData(buffer, 0, Int64Length);
    }

    /// <summary>Appends optional text with an explicit null marker.</summary>
    /// <param name="hash">The incremental hash.</param>
    /// <param name="buffer">The reusable staging buffer.</param>
    /// <param name="value">The optional text.</param>
    private static void AppendOptionalText(CanonicalHash hash, byte[] buffer, string? value)
    {
        buffer[0] = value is null ? (byte)0 : (byte)1;
        hash.AppendData(buffer, 0, BooleanLength);
        if (value is null)
        {
            return;
        }

        AppendText(hash, buffer, value);
    }

    /// <summary>Appends length-prefixed strict UTF-8 text.</summary>
    /// <param name="hash">The incremental hash.</param>
    /// <param name="buffer">The reusable staging buffer.</param>
    /// <param name="value">The text value.</param>
    /// <exception cref="EncoderFallbackException"><paramref name="value"/> contains malformed UTF-16.</exception>
    private static void AppendText(CanonicalHash hash, byte[] buffer, string value)
    {
        AppendInt32(hash, buffer, CanonicalEncoding.GetByteCount(value));
        var encoder = CanonicalEncoding.GetEncoder();
        var offset = 0;
        var completed = value.Length == 0;
#if NET5_0_OR_GREATER
        while (!completed)
        {
            encoder.Convert(value.AsSpan(offset), buffer, true, out var charsUsed, out var bytesUsed, out completed);
            hash.AppendData(buffer, 0, bytesUsed);
            offset += charsUsed;
        }
#else
        var chars = value.ToCharArray();
        while (!completed)
        {
            encoder.Convert(chars, offset, chars.Length - offset, buffer, 0, buffer.Length, true, out var charsUsed, out var bytesUsed, out completed);
            hash.AppendData(buffer, 0, bytesUsed);
            offset += charsUsed;
        }
#endif
    }

#if !NET5_0_OR_GREATER
    /// <summary>Appends payload bytes in bounded chunks for target frameworks without span hashing.</summary>
    /// <param name="hash">The incremental hash.</param>
    /// <param name="buffer">The reusable staging buffer.</param>
    /// <param name="value">The payload bytes.</param>
    private static void AppendMemory(CanonicalHash hash, byte[] buffer, ReadOnlyMemory<byte> value)
    {
        for (var offset = 0; offset < value.Length; offset += buffer.Length)
        {
            var count = Math.Min(buffer.Length, value.Length - offset);
            value.Span.Slice(offset, count).CopyTo(buffer);
            hash.AppendData(buffer, 0, count);
        }
    }
#endif

    /// <summary>Appends owned bytes without adding a length prefix.</summary>
    /// <param name="hash">The incremental hash.</param>
    /// <param name="value">The bytes to append.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AppendBytes(CanonicalHash hash, byte[] value) => hash.AppendData(value, 0, value.Length);

    /// <summary>Provides append-only SHA-256 hashing across all target frameworks.</summary>
    private sealed class CanonicalHash : IDisposable
    {
#if NET5_0_OR_GREATER
        /// <summary>The modern incremental hash implementation.</summary>
        private readonly IncrementalHash _hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
#else
        /// <summary>The framework hash implementation.</summary>
        private readonly SHA256 _hash = SHA256.Create();
#endif

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _hash.Dispose();

        /// <summary>Appends bytes from a caller-owned buffer.</summary>
        /// <param name="buffer">The source buffer.</param>
        /// <param name="offset">The source offset.</param>
        /// <param name="count">The byte count.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void AppendData(byte[] buffer, int offset, int count)
        {
#if NET5_0_OR_GREATER
            _hash.AppendData(buffer, offset, count);
#else
            _ = _hash.TransformBlock(buffer, offset, count, null, 0);
#endif
        }

#if NET5_0_OR_GREATER
        /// <summary>Appends bytes from a read-only span.</summary>
        /// <param name="value">The source bytes.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void AppendData(ReadOnlySpan<byte> value) => _hash.AppendData(value);
#endif

        /// <summary>Finalizes and returns the SHA-256 hash.</summary>
        /// <returns>The hash bytes.</returns>
        internal byte[] GetHashAndReset()
        {
#if NET5_0_OR_GREATER
            return _hash.GetHashAndReset();
#else
            _ = _hash.TransformFinalBlock([], 0, 0);
            return _hash.Hash ?? [];
#endif
        }
    }
}
