// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Encrypts and authenticates SQLite column values with AES-256-GCM under provider-supplied keys.</summary>
/// <remarks>
/// <para>
/// Envelope version 1 has the layout <c>version | key id length | key id | nonce | tag | ciphertext</c>. The version and
/// the key id length take one byte each, the key id is ASCII, the nonce takes 12 bytes, and the tag takes 16 bytes. The
/// whole header is part of the associated data.
/// </para>
/// <para>
/// Each provider key is expanded with HKDF-SHA256 into a dedicated AES-256 key for this purpose. Every value gets a fresh
/// random 96-bit nonce and a 128-bit tag. .NET Framework has no AES-GCM implementation, so protection is available only on
/// .NET 8 or later.
/// </para>
/// </remarks>
internal sealed class SqliteRecordProtection
{
    /// <summary>The current envelope format version.</summary>
    internal const byte EnvelopeVersion = 1;

    /// <summary>The bytes before the key identifier: the version and the key identifier length.</summary>
    internal const int HeaderPrefixBytes = 2;

    /// <summary>The AES-GCM nonce length in bytes.</summary>
    internal const int NonceBytes = 12;

    /// <summary>The AES-GCM tag length in bytes.</summary>
    internal const int TagBytes = 16;

    /// <summary>The largest supported key identifier length in bytes.</summary>
    internal const int MaximumKeyIdBytes = 64;

    /// <summary>The smallest possible envelope overhead: the header prefix, a one-character key id, nonce, and tag.</summary>
    internal const int MinimumEnvelopeOverhead = HeaderPrefixBytes + 1 + NonceBytes + TagBytes;

    /// <summary>The largest possible envelope overhead.</summary>
    internal const int MaximumEnvelopeOverhead = HeaderPrefixBytes + MaximumKeyIdBytes + NonceBytes + TagBytes;

    /// <summary>The index of the key identifier length byte.</summary>
    private const int KeyIdLengthIndex = 1;

    /// <summary>The smallest printable ASCII character allowed in a key identifier.</summary>
    private const byte FirstPrintableAscii = 0x21;

    /// <summary>The largest printable ASCII character allowed in a key identifier.</summary>
    private const byte LastPrintableAscii = 0x7E;

#if NET8_0_OR_GREATER
    /// <summary>The derived AES key length in bytes.</summary>
    private const int DerivedKeyBytes = 32;
#endif

    /// <summary>The key provider.</summary>
    private readonly ILocalStoreKeyProvider _keyProvider;

    /// <summary>Initializes a new instance of the <see cref="SqliteRecordProtection"/> class.</summary>
    /// <param name="keyProvider">The key provider.</param>
    private SqliteRecordProtection(ILocalStoreKeyProvider keyProvider) => _keyProvider = keyProvider;

#if NET8_0_OR_GREATER
    /// <summary>Gets the HKDF info value that separates this key use from any other use of the provider key.</summary>
    private static ReadOnlySpan<byte> DerivationInfo => "ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite/record-aead/aes-256-gcm/v1"u8;
#endif

    /// <summary>Creates record protection for the supplied key provider.</summary>
    /// <param name="keyProvider">The key provider.</param>
    /// <returns>The record protection.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="keyProvider"/> is null.</exception>
    /// <exception cref="PlatformNotSupportedException">The platform has no AES-GCM implementation.</exception>
    internal static SqliteRecordProtection Create(ILocalStoreKeyProvider keyProvider)
    {
        ArgumentExceptionHelper.ThrowIfNull(keyProvider);
        ThrowIfUnsupported();
        return new(keyProvider);
    }

    /// <summary>Throws when the platform has no AES-GCM implementation.</summary>
    /// <exception cref="PlatformNotSupportedException">The platform has no AES-GCM implementation.</exception>
    internal static void ThrowIfUnsupported()
    {
#if NET8_0_OR_GREATER
        if (AesGcm.IsSupported)
        {
            return;
        }

        throw new PlatformNotSupportedException("SQLite encryption at rest requires AES-GCM, which this platform does not provide.");
#else
        throw new PlatformNotSupportedException(
            "SQLite encryption at rest requires .NET 8 or later because .NET Framework does not provide AES-GCM.");
#endif
    }

    /// <summary>Reads the key identifier from an envelope without authenticating it.</summary>
    /// <param name="envelope">The envelope bytes.</param>
    /// <returns>The key identifier, or null when the envelope header is malformed.</returns>
    internal static string? TryReadKeyId(ReadOnlySpan<byte> envelope)
    {
        if (envelope.Length < MinimumEnvelopeOverhead || envelope[0] != EnvelopeVersion)
        {
            return null;
        }

        var keyIdLength = envelope[KeyIdLengthIndex];
        if (keyIdLength is 0 or > MaximumKeyIdBytes || envelope.Length < HeaderPrefixBytes + keyIdLength + NonceBytes + TagBytes)
        {
            return null;
        }

        var characters = new char[keyIdLength];
        for (var index = 0; index < keyIdLength; index++)
        {
            var value = envelope[HeaderPrefixBytes + index];
            if (value is < FirstPrintableAscii or > LastPrintableAscii)
            {
                return null;
            }

            characters[index] = (char)value;
        }

        return new(characters);
    }

    /// <summary>Gets the identifier of the key that protects new values.</summary>
    /// <returns>The current key identifier.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal string GetCurrentKeyId() => GetValidatedCurrentKey().KeyId;

    /// <summary>Encrypts and authenticates one value under the current key.</summary>
    /// <param name="plaintext">The value bytes.</param>
    /// <param name="associatedData">Creates the associated data for the envelope header.</param>
    /// <returns>The envelope bytes.</returns>
    /// <exception cref="PlatformNotSupportedException">The platform has no AES-GCM implementation.</exception>
    internal byte[] Protect(ReadOnlySpan<byte> plaintext, Func<byte[], byte[]> associatedData)
    {
#if NET8_0_OR_GREATER
        var key = GetValidatedCurrentKey();
        var keyId = Encoding.ASCII.GetBytes(key.KeyId);
        var headerLength = HeaderPrefixBytes + keyId.Length;
        var envelope = new byte[headerLength + NonceBytes + TagBytes + plaintext.Length];
        envelope[0] = EnvelopeVersion;
        envelope[KeyIdLengthIndex] = (byte)keyId.Length;
        keyId.CopyTo(envelope, HeaderPrefixBytes);
        var header = envelope.AsSpan(0, headerLength).ToArray();
        var nonce = envelope.AsSpan(headerLength, NonceBytes);
        var tag = envelope.AsSpan(headerLength + NonceBytes, TagBytes);
        var ciphertext = envelope.AsSpan(headerLength + NonceBytes + TagBytes);
        RandomNumberGenerator.Fill(nonce);
        var derivedKey = DeriveKey(key);
        try
        {
            using var aes = new AesGcm(derivedKey, TagBytes);
            aes.Encrypt(nonce, plaintext, ciphertext, tag, associatedData(header));
        }
        finally
        {
            CryptographicOperations.ZeroMemory(derivedKey);
        }

        return envelope;
#else
        _ = plaintext.Length;
        _ = associatedData;
        _ = GetValidatedCurrentKey();
        throw new PlatformNotSupportedException("SQLite encryption at rest requires .NET 8 or later.");
#endif
    }

    /// <summary>Authenticates and decrypts one envelope.</summary>
    /// <param name="envelope">The envelope bytes.</param>
    /// <param name="associatedData">Creates the associated data for the envelope header.</param>
    /// <returns>The value bytes.</returns>
    /// <exception cref="LocalStoreRecordAuthenticationException">The envelope is malformed, its key is unavailable, or authentication fails.</exception>
    /// <exception cref="PlatformNotSupportedException">The platform has no AES-GCM implementation.</exception>
    internal byte[] Unprotect(ReadOnlySpan<byte> envelope, Func<byte[], byte[]> associatedData)
    {
#if NET8_0_OR_GREATER
        var keyId = TryReadKeyId(envelope)
            ?? throw new LocalStoreRecordAuthenticationException("A persisted SQLite record envelope is malformed.");
        var key = _keyProvider.GetKey(keyId)
            ?? throw new LocalStoreRecordAuthenticationException($"The key '{keyId}' that protects a persisted SQLite record is not available.");
        if (!string.Equals(key.KeyId, keyId, StringComparison.Ordinal))
        {
            throw new LocalStoreRecordAuthenticationException("The key provider returned a key with a different identifier.");
        }

        var headerLength = HeaderPrefixBytes + envelope[KeyIdLengthIndex];
        var header = envelope[..headerLength].ToArray();
        var nonce = envelope.Slice(headerLength, NonceBytes);
        var tag = envelope.Slice(headerLength + NonceBytes, TagBytes);
        var ciphertext = envelope[(headerLength + NonceBytes + TagBytes)..];
        var plaintext = new byte[ciphertext.Length];
        var derivedKey = DeriveKey(key);
        try
        {
            using var aes = new AesGcm(derivedKey, TagBytes);
            aes.Decrypt(nonce, ciphertext, tag, plaintext, associatedData(header));
        }
        catch (CryptographicException exception)
        {
            throw new LocalStoreRecordAuthenticationException("A persisted SQLite record failed authentication.", exception);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(derivedKey);
        }

        return plaintext;
#else
        _ = envelope.Length;
        _ = associatedData;
        _ = GetValidatedCurrentKey();
        throw new PlatformNotSupportedException("SQLite encryption at rest requires .NET 8 or later.");
#endif
    }

#if NET8_0_OR_GREATER
    /// <summary>Derives the AES-256 key for one provider key.</summary>
    /// <param name="key">The provider key.</param>
    /// <returns>The derived key; the caller zeroes it after use.</returns>
    private static byte[] DeriveKey(LocalStoreKey key)
    {
        var derivedKey = new byte[DerivedKeyBytes];
        HKDF.DeriveKey(HashAlgorithmName.SHA256, key.KeyMaterial, derivedKey, ReadOnlySpan<byte>.Empty, DerivationInfo);
        return derivedKey;
    }
#endif

    /// <summary>Gets and validates the provider's current key.</summary>
    /// <returns>The current key.</returns>
    /// <exception cref="InvalidOperationException">The provider returned no key.</exception>
    private LocalStoreKey GetValidatedCurrentKey() =>
        _keyProvider.GetCurrentKey() ?? throw new InvalidOperationException("The local store key provider returned no current key.");
}
