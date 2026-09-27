// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Security.Cryptography;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Represents one identified key used to protect local store records at rest.</summary>
/// <remarks>
/// The key material is at least 256 bits. <see cref="ToString"/> and the debugger display show only
/// <see cref="KeyId"/>, so the key material does not leak into logs.
/// </remarks>
[DebuggerDisplay("KeyId = {KeyId,nq}")]
public sealed class LocalStoreKey
{
    /// <summary>The minimum key material length in bytes (256 bits).</summary>
    private const int MinimumKeyBytes = 32;

    /// <summary>The maximum key material length in bytes.</summary>
    private const int MaximumKeyBytes = 1024;

    /// <summary>The maximum key identifier length in characters.</summary>
    private const int MaximumKeyIdLength = 64;

    /// <summary>The key material.</summary>
    private readonly byte[] _keyMaterial;

    /// <summary>Initializes a new instance of the <see cref="LocalStoreKey"/> class.</summary>
    /// <param name="keyId">The stable key identifier stored with protected records.</param>
    /// <param name="keyMaterial">The secret key material; the instance keeps its own copy.</param>
    /// <exception cref="ArgumentNullException"><paramref name="keyId"/> or <paramref name="keyMaterial"/> is null.</exception>
    /// <exception cref="ArgumentException">The key identifier or key material length is invalid.</exception>
    public LocalStoreKey(string keyId, byte[] keyMaterial)
    {
        ArgumentExceptionHelper.ThrowIfNull(keyMaterial);
        ValidateKeyId(keyId);
        if (keyMaterial.Length is < MinimumKeyBytes or > MaximumKeyBytes)
        {
            throw new ArgumentException("Local store key material must be between 32 and 1024 bytes.", nameof(keyMaterial));
        }

        KeyId = keyId;
        _keyMaterial = (byte[])keyMaterial.Clone();
    }

    /// <summary>Gets the stable key identifier stored with protected records.</summary>
    public string KeyId { get; }

    /// <summary>Gets the secret key material.</summary>
    public ReadOnlySpan<byte> KeyMaterial => _keyMaterial;

    /// <summary>Creates a key with fresh 256-bit random key material.</summary>
    /// <param name="keyId">The stable key identifier stored with protected records.</param>
    /// <returns>The new key.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="keyId"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="keyId"/> is invalid.</exception>
    public static LocalStoreKey CreateRandom(string keyId)
    {
        var material = new byte[MinimumKeyBytes];
        using (var generator = RandomNumberGenerator.Create())
        {
            generator.GetBytes(material);
        }

        return new(keyId, material);
    }

    /// <summary>Validates a key identifier.</summary>
    /// <param name="keyId">The key identifier.</param>
    /// <exception cref="ArgumentNullException"><paramref name="keyId"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="keyId"/> is empty, too long, or uses unsupported characters.</exception>
    public static void ValidateKeyId(string keyId)
    {
        ArgumentExceptionHelper.ThrowIfNull(keyId);
        if (keyId.Length is 0 or > MaximumKeyIdLength)
        {
            throw new ArgumentException("Local store key identifiers must contain 1 to 64 characters.", nameof(keyId));
        }

        foreach (var character in keyId)
        {
            if (!IsKeyIdCharacter(character))
            {
                throw new ArgumentException("Local store key identifiers may use only ASCII letters, digits, '.', '-', '_' and ':'.", nameof(keyId));
            }
        }
    }

    /// <inheritdoc/>
    public override string ToString() => $"LocalStoreKey {KeyId}";

    /// <summary>Determines whether a character is allowed in a key identifier.</summary>
    /// <param name="character">The character.</param>
    /// <returns>Whether the character is allowed.</returns>
    private static bool IsKeyIdCharacter(char character) =>
        character is (>= 'a' and <= 'z') or (>= 'A' and <= 'Z') or (>= '0' and <= '9') or '.' or '-' or '_' or ':';
}
