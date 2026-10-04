// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Creates bounded payload evidence for local quarantine records.</summary>
public static class LocalPayloadQuarantineEvidenceFactory
{
    /// <summary>The default maximum evidence prefix bytes.</summary>
    private const int DefaultMaximumEvidenceByteCount = 4096;

    /// <summary>The UTF-16 code unit count for a surrogate-pair scalar.</summary>
    private const int SurrogatePairCodeUnitCount = 2;

    /// <summary>The UTF-8 byte count for a two-byte scalar.</summary>
    private const int TwoByteScalarUtf8ByteCount = 2;

    /// <summary>The UTF-8 byte count for a three-byte scalar.</summary>
    private const int ThreeByteScalarUtf8ByteCount = 3;

    /// <summary>The UTF-8 byte count for a four-byte scalar.</summary>
    private const int FourByteScalarUtf8ByteCount = 4;

    /// <summary>The strict UTF-8 encoding used for metadata validation.</summary>
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(false, true);

    /// <summary>Gets the default maximum evidence prefix bytes.</summary>
    public static int DefaultMaximumEvidenceBytes => DefaultMaximumEvidenceByteCount;

    /// <summary>Creates evidence from an optional payload envelope.</summary>
    /// <param name="envelope">The envelope, when one could be formed.</param>
    /// <param name="maximumEvidenceBytes">The maximum UTF-8 bytes retained for each metadata field and payload prefix.</param>
    /// <returns>The bounded evidence.</returns>
    public static LocalPayloadQuarantineEvidence FromEnvelope(PayloadEnvelope? envelope, int maximumEvidenceBytes)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(maximumEvidenceBytes);
        if (envelope is null)
        {
            return new(null, null, null, 0, null, ReadOnlyMemory<byte>.Empty);
        }

        var prefix = envelope.CopyPayloadPrefix(maximumEvidenceBytes);
        return FromEvidence(
            new(envelope.ContractId, envelope.SchemaVersion, envelope.ContentType, envelope.PayloadLength, envelope.PayloadHash, prefix),
            maximumEvidenceBytes);
    }

    /// <summary>Creates normalized evidence from caller or provider supplied evidence.</summary>
    /// <param name="evidence">The supplied evidence.</param>
    /// <param name="maximumEvidenceBytes">The maximum UTF-8 bytes retained for each metadata field and payload prefix.</param>
    /// <returns>The normalized bounded evidence.</returns>
    /// <exception cref="ArgumentException">The evidence has malformed metadata or an impossible prefix length.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="evidence"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The evidence or maximum length is negative.</exception>
    public static LocalPayloadQuarantineEvidence FromEvidence(
        LocalPayloadQuarantineEvidence evidence,
        int maximumEvidenceBytes)
    {
        ArgumentExceptionHelper.ThrowIfNull(evidence);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(maximumEvidenceBytes);
        if (evidence.PayloadLength < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(evidence), evidence.PayloadLength, "Evidence payload length must not be negative.");
        }

        if (evidence.PayloadPrefixLength > evidence.PayloadLength)
        {
            throw new ArgumentException("Evidence payload prefix length must not exceed the original payload length.", nameof(evidence));
        }

        var maximumPrefixBytes = Math.Min(maximumEvidenceBytes, evidence.PayloadLength);
        return new(
            TrimEvidenceMetadata(evidence.ContractId, maximumEvidenceBytes),
            evidence.SchemaVersion,
            TrimEvidenceMetadata(evidence.ContentType, maximumEvidenceBytes),
            evidence.PayloadLength,
            TrimEvidenceMetadata(evidence.PayloadHash, maximumEvidenceBytes),
            evidence.CopyPayloadPrefix(maximumPrefixBytes));
    }

    /// <summary>Trims evidence metadata to a UTF-8 byte boundary.</summary>
    /// <param name="value">The metadata value.</param>
    /// <param name="maximumBytes">The maximum UTF-8 bytes.</param>
    /// <returns>The trimmed metadata.</returns>
    /// <exception cref="ArgumentException">The metadata contains malformed Unicode.</exception>
    private static string? TrimEvidenceMetadata(string? value, int maximumBytes)
    {
        if (value is null)
        {
            return null;
        }

        int byteCount;
        try
        {
            byteCount = StrictUtf8.GetByteCount(value);
        }
        catch (EncoderFallbackException exception)
        {
            throw new ArgumentException("Evidence metadata must be well-formed Unicode.", nameof(value), exception);
        }

        if (byteCount <= maximumBytes)
        {
            return value;
        }

        var bytes = 0;
        var endIndex = 0;
        for (var index = 0; index < value.Length;)
        {
            var scalarBytes = GetUtf8ScalarByteCount(value, index, out var charCount);
            if (bytes + scalarBytes > maximumBytes)
            {
                break;
            }

            bytes += scalarBytes;
            index += charCount;
            endIndex = index;
        }

#if NET8_0_OR_GREATER
        return value[..endIndex];
#else
        return value.Remove(endIndex);
#endif
    }

    /// <summary>Gets the UTF-8 byte count for one valid scalar in a string.</summary>
    /// <param name="value">The value containing the scalar.</param>
    /// <param name="index">The scalar start index.</param>
    /// <param name="charCount">The scalar UTF-16 code unit count.</param>
    /// <returns>The UTF-8 byte count.</returns>
    private static int GetUtf8ScalarByteCount(string value, int index, out int charCount)
    {
        var character = value[index];
        if (char.IsHighSurrogate(character))
        {
            charCount = SurrogatePairCodeUnitCount;
            return FourByteScalarUtf8ByteCount;
        }

        charCount = 1;
        if (character <= 0x7F)
        {
            return 1;
        }

        return character <= 0x7FF ? TwoByteScalarUtf8ByteCount : ThreeByteScalarUtf8ByteCount;
    }
}
