// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Validates and reads persisted payload streams.</summary>
internal static class SqlitePayloadStreamIntegrity
{
    /// <summary>Accepts an opened stream only when its observed length matches the projected length.</summary>
    /// <typeparam name="TStream">The stream type.</typeparam>
    /// <param name="payload">The opened payload stream.</param>
    /// <param name="expectedLength">The projected payload length.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The accepted stream.</returns>
    /// <exception cref="InvalidOperationException">The stream length does not match.</exception>
    internal static TStream AcceptLength<TStream>(TStream payload, long expectedLength, string message)
        where TStream : Stream
    {
        if (payload.Length == expectedLength)
        {
            return payload;
        }

        payload.Dispose();
        throw new InvalidOperationException(message);
    }

    /// <summary>Reads the exact expected number of bytes from a payload stream.</summary>
    /// <param name="payload">The payload stream.</param>
    /// <param name="payloadLength">The expected payload length.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The payload bytes.</returns>
    /// <exception cref="InvalidOperationException">The stream ended before the expected length.</exception>
    internal static byte[] ReadExactly(Stream payload, long payloadLength, string message)
    {
        var bytes = new byte[checked((int)payloadLength)];
        ReadExactly(payload, bytes, message);
        return bytes;
    }

    /// <summary>Validates the payload stream against the expected canonical SHA-256 hash.</summary>
    /// <param name="payload">The payload stream.</param>
    /// <param name="expectedPayloadHash">The expected payload hash.</param>
    /// <param name="message">The failure message.</param>
    /// <exception cref="InvalidOperationException">The stream hash does not match.</exception>
    internal static void ValidateCanonicalSha256Hash(Stream payload, string expectedPayloadHash, string message)
    {
        var computedHash = ComputeSha256PayloadHash(payload);
        if (PayloadHashEquals(computedHash, expectedPayloadHash))
        {
            return;
        }

        throw new InvalidOperationException(message);
    }

    /// <summary>Computes the canonical SHA-256 payload hash for a stream.</summary>
    /// <param name="payload">The payload stream.</param>
    /// <returns>The formatted payload hash.</returns>
    internal static string ComputeSha256PayloadHash(Stream payload)
    {
        using var sha256 = SHA256.Create();
        var hash = sha256.ComputeHash(payload);
        return $"sha256-{Convert.ToBase64String(hash)}";
    }

    /// <summary>Reads a stream fully into the supplied buffer.</summary>
    /// <param name="payload">The payload stream.</param>
    /// <param name="buffer">The target buffer.</param>
    /// <param name="message">The failure message.</param>
    /// <exception cref="InvalidOperationException">The stream ended before the expected length.</exception>
    private static void ReadExactly(Stream payload, byte[] buffer, string message)
    {
        for (var offset = 0; offset < buffer.Length;)
        {
            var read = payload.Read(buffer, offset, buffer.Length - offset);
            if (read == 0)
            {
                throw new InvalidOperationException(message);
            }

            offset += read;
        }
    }

    /// <summary>Determines whether two payload hashes match.</summary>
    /// <param name="left">The first hash.</param>
    /// <param name="right">The second hash.</param>
    /// <returns>Whether the hashes match.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool PayloadHashEquals(string left, string right) =>
#if NET5_0_OR_GREATER
        CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(left), Encoding.UTF8.GetBytes(right));
#else
        string.Equals(left, right, StringComparison.Ordinal);
#endif
}
