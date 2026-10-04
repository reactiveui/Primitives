// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Creates and parses opaque receive group cursors.</summary>
internal static class ServerReceiveGroupCursor
{
    /// <summary>The receive group cursor prefix.</summary>
    private const string Prefix = "ocg";

    /// <summary>The required cursor part count.</summary>
    private const int CursorPartCount = 3;

    /// <summary>The receive group cursor segment separator.</summary>
    private const string Separator = ":";

    /// <summary>The byte count retained from the stream binding digest.</summary>
    private const int BindingByteCount = 16;

    /// <summary>The second byte offset in a fixed integer encoding.</summary>
    private const int SecondByteOffset = 1;

    /// <summary>The third byte offset in a fixed integer encoding.</summary>
    private const int ThirdByteOffset = 2;

    /// <summary>The fourth byte offset in a fixed integer encoding.</summary>
    private const int FourthByteOffset = 3;

    /// <summary>Creates a cursor for a stream group sequence.</summary>
    /// <param name="streamKey">The authenticated stream key.</param>
    /// <param name="groupSequence">The group sequence.</param>
    /// <returns>The cursor text.</returns>
    /// <exception cref="ArgumentException">The stream key is malformed.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The sequence is negative.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static string Create(ServerStreamKey streamKey, long groupSequence)
    {
        ServerCommitJournalGuard.ValidateStreamKey(streamKey);
        if (groupSequence < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(groupSequence), groupSequence, null);
        }

        return $"{Prefix}{Separator}{CreateStreamBinding(streamKey)}{Separator}{groupSequence.ToString(CultureInfo.InvariantCulture)}";
    }

    /// <summary>Returns whether a cursor uses the receive group cursor namespace.</summary>
    /// <param name="cursor">The cursor text.</param>
    /// <returns>Whether the cursor starts with the receive group prefix.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool IsGroupCursor(string cursor) =>
        cursor.StartsWith($"{Prefix}{Separator}", StringComparison.Ordinal);

    /// <summary>Parses and validates a cursor for the requested stream.</summary>
    /// <param name="streamKey">The requested stream key.</param>
    /// <param name="cursor">The cursor text.</param>
    /// <returns>The decoded group sequence.</returns>
    /// <exception cref="ArgumentException">The cursor is malformed or belongs to another stream.</exception>
    internal static long Parse(ServerStreamKey streamKey, string cursor)
    {
        ServerCommitJournalGuard.ValidateStreamKey(streamKey);
        ServerCommitJournalGuard.ValidateCursor(cursor);
        var parts = cursor.Split(':');
        EnsureCursorShape(parts, cursor);
        return ParseSequence(streamKey, parts, cursor);
    }

    /// <summary>Creates a compact stream binding for the authenticated tenant and stream.</summary>
    /// <param name="streamKey">The stream key.</param>
    /// <returns>The unpadded base64url binding.</returns>
    private static string CreateStreamBinding(ServerStreamKey streamKey)
    {
        var bindingInput = CreateBindingInput(streamKey);
#if NET8_0_OR_GREATER
        var hash = SHA256.HashData(bindingInput);
#else
        using var sha256 = SHA256.Create();
        var hash = sha256.ComputeHash(bindingInput);
#endif
        return Encode(hash, BindingByteCount);
    }

    /// <summary>Creates an unambiguous stream binding input from length-prefixed identity bytes.</summary>
    /// <param name="streamKey">The stream key.</param>
    /// <returns>The binding input bytes.</returns>
    private static byte[] CreateBindingInput(ServerStreamKey streamKey)
    {
        var tenant = Encoding.UTF8.GetBytes(streamKey.TenantId);
        var stream = Encoding.UTF8.GetBytes(streamKey.StreamId.Value);
        var bytes = new byte[sizeof(int) + tenant.Length + sizeof(int) + stream.Length];
        WriteInt32BigEndian(bytes, 0, tenant.Length);
        Buffer.BlockCopy(tenant, 0, bytes, sizeof(int), tenant.Length);
        var streamLengthOffset = sizeof(int) + tenant.Length;
        WriteInt32BigEndian(bytes, streamLengthOffset, stream.Length);
        Buffer.BlockCopy(stream, 0, bytes, streamLengthOffset + sizeof(int), stream.Length);
        return bytes;
    }

    /// <summary>Writes an integer using fixed big-endian bytes.</summary>
    /// <param name="bytes">The destination bytes.</param>
    /// <param name="offset">The write offset.</param>
    /// <param name="value">The value.</param>
    private static void WriteInt32BigEndian(byte[] bytes, int offset, int value)
    {
        bytes[offset] = (byte)(value >> 24);
        bytes[offset + SecondByteOffset] = (byte)(value >> 16);
        bytes[offset + ThirdByteOffset] = (byte)(value >> 8);
        bytes[offset + FourthByteOffset] = (byte)value;
    }

    /// <summary>Encodes cursor bytes as unpadded base64url.</summary>
    /// <param name="value">The bytes.</param>
    /// <param name="length">The byte count to encode.</param>
    /// <returns>The encoded value.</returns>
    private static string Encode(byte[] value, int length)
    {
        var encoded = Convert.ToBase64String(value, 0, length);
        return encoded.TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }

    /// <summary>Validates cursor split shape.</summary>
    /// <param name="parts">The cursor parts.</param>
    /// <param name="cursor">The original cursor.</param>
    /// <exception cref="ArgumentException">The cursor is malformed.</exception>
    private static void EnsureCursorShape(string[] parts, string cursor)
    {
        if (parts.Length == CursorPartCount
            && parts[0].Length != 0
            && parts[1].Length != 0
            && parts[2].Length != 0)
        {
            return;
        }

        throw new ArgumentException("The receive group cursor is malformed.", nameof(cursor));
    }

    /// <summary>Parses the sequence after stream binding validation.</summary>
    /// <param name="streamKey">The requested stream key.</param>
    /// <param name="parts">The cursor parts.</param>
    /// <param name="cursor">The original cursor.</param>
    /// <returns>The parsed sequence.</returns>
    /// <exception cref="ArgumentException">The cursor belongs to another stream or has an invalid sequence.</exception>
    private static long ParseSequence(
        ServerStreamKey streamKey,
        string[] parts,
        string cursor)
    {
        if (string.Equals(parts[0], Prefix, StringComparison.Ordinal)
            && string.Equals(parts[1], CreateStreamBinding(streamKey), StringComparison.Ordinal)
            && long.TryParse(parts[2], NumberStyles.None, CultureInfo.InvariantCulture, out var sequence)
            && sequence >= 0)
        {
            return sequence;
        }

        throw new ArgumentException("The receive group cursor does not belong to the requested stream.", nameof(cursor));
    }
}
