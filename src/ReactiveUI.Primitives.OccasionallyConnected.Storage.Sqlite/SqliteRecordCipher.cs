// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Protects SQLite column values for one store identity.</summary>
/// <remarks>
/// BLOB columns hold the raw envelope. TEXT columns hold the Base64 form of the envelope, so their SQLite storage class
/// stays TEXT and existing storage-class checks still apply.
/// </remarks>
internal sealed class SqliteRecordCipher
{
    /// <summary>The number of bytes in one Base64 block.</summary>
    private const int Base64BlockBytes = 3;

    /// <summary>The number of characters in one Base64 block.</summary>
    private const int Base64BlockCharacters = 4;

    /// <summary>The strict UTF-8 encoding used for decrypted text.</summary>
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(false, true);

    /// <summary>The record protection.</summary>
    private readonly SqliteRecordProtection _protection;

    /// <summary>The store identity bound into every value.</summary>
    private readonly string _storeIdentity;

    /// <summary>Initializes a new instance of the <see cref="SqliteRecordCipher"/> class.</summary>
    /// <param name="protection">The record protection.</param>
    /// <param name="storeIdentity">The store identity bound into every value.</param>
    internal SqliteRecordCipher(SqliteRecordProtection protection, string storeIdentity)
    {
        _protection = protection;
        _storeIdentity = storeIdentity;
    }

    /// <summary>Gets the record protection.</summary>
    internal SqliteRecordProtection Protection => _protection;

    /// <summary>Gets the cipher attached to a connection, if the store protects records.</summary>
    /// <param name="connection">The connection.</param>
    /// <returns>The cipher, or null for a plaintext store.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static SqliteRecordCipher? For(SqliteConnection? connection) => (connection as SqliteProtectedConnection)?.Cipher;

    /// <summary>Gets a Base64 text length that can hold a protected value with the supplied plaintext byte count.</summary>
    /// <param name="plaintextBytes">The plaintext byte count.</param>
    /// <returns>The protected text length upper bound.</returns>
    internal static long ProtectedTextLengthUpperBound(long plaintextBytes)
    {
        var envelopeBytes = checked(plaintextBytes + SqliteRecordProtection.MaximumEnvelopeOverhead);
        return checked((envelopeBytes + Base64BlockBytes - 1) / Base64BlockBytes * Base64BlockCharacters);
    }

    /// <summary>Gets an upper bound of the plaintext byte count held by a protected Base64 text value.</summary>
    /// <param name="storedLength">The stored text length.</param>
    /// <returns>The plaintext byte count upper bound.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static long PlaintextUpperBoundFromText(long storedLength) =>
        Math.Max(0, (storedLength / Base64BlockCharacters * Base64BlockBytes) - SqliteRecordProtection.MinimumEnvelopeOverhead);

    /// <summary>Gets an upper bound of the plaintext byte count held by several protected Base64 text values.</summary>
    /// <param name="storedLength">The total stored text length.</param>
    /// <param name="valueCount">The number of values.</param>
    /// <returns>The total plaintext byte count upper bound.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static long PlaintextUpperBoundFromText(long storedLength, long valueCount) =>
        Math.Max(0, (storedLength / Base64BlockCharacters * Base64BlockBytes) - (valueCount * SqliteRecordProtection.MinimumEnvelopeOverhead));

    /// <summary>Gets an upper bound of the plaintext byte count held by a protected BLOB value.</summary>
    /// <param name="storedLength">The stored BLOB length.</param>
    /// <returns>The plaintext byte count upper bound.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static long PlaintextUpperBoundFromBlob(long storedLength) =>
        Math.Max(0, storedLength - SqliteRecordProtection.MinimumEnvelopeOverhead);

    /// <summary>Gets the key identifier that protects a stored BLOB value.</summary>
    /// <param name="stored">The stored envelope.</param>
    /// <returns>The key identifier, or null when the envelope is malformed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static string? ReadBytesKeyId(byte[] stored) => SqliteRecordProtection.TryReadKeyId(stored);

    /// <summary>Gets the key identifier that protects a stored TEXT value.</summary>
    /// <param name="stored">The stored Base64 envelope.</param>
    /// <returns>The key identifier, or null when the envelope is malformed.</returns>
    internal static string? ReadTextKeyId(string stored)
    {
        try
        {
            return SqliteRecordProtection.TryReadKeyId(Convert.FromBase64String(stored));
        }
        catch (FormatException)
        {
            return null;
        }
    }

    /// <summary>Protects a BLOB value.</summary>
    /// <param name="value">The plaintext bytes.</param>
    /// <param name="context">The record context.</param>
    /// <param name="column">The column name.</param>
    /// <returns>The envelope bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal byte[] ProtectBytes(ReadOnlySpan<byte> value, SqliteRecordContext context, string column) =>
        _protection.Protect(value, header => context.CreateAssociatedData(_storeIdentity, column, header));

    /// <summary>Authenticates and decrypts a BLOB value.</summary>
    /// <param name="stored">The envelope bytes.</param>
    /// <param name="context">The record context.</param>
    /// <param name="column">The column name.</param>
    /// <returns>The plaintext bytes.</returns>
    /// <exception cref="LocalStoreRecordAuthenticationException">The value fails authentication.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal byte[] UnprotectBytes(ReadOnlySpan<byte> stored, SqliteRecordContext context, string column) =>
        _protection.Unprotect(stored, header => context.CreateAssociatedData(_storeIdentity, column, header));

    /// <summary>Protects a TEXT value.</summary>
    /// <param name="value">The plaintext text.</param>
    /// <param name="context">The record context.</param>
    /// <param name="column">The column name.</param>
    /// <returns>The Base64 envelope.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal string ProtectText(string value, SqliteRecordContext context, string column) =>
        Convert.ToBase64String(ProtectBytes(StrictUtf8.GetBytes(value), context, column));

    /// <summary>Authenticates and decrypts a TEXT value.</summary>
    /// <param name="stored">The Base64 envelope.</param>
    /// <param name="context">The record context.</param>
    /// <param name="column">The column name.</param>
    /// <returns>The plaintext text.</returns>
    /// <exception cref="LocalStoreRecordAuthenticationException">The value fails authentication.</exception>
    internal string UnprotectText(string stored, SqliteRecordContext context, string column)
    {
        byte[] envelope;
        try
        {
            envelope = Convert.FromBase64String(stored);
        }
        catch (FormatException exception)
        {
            throw new LocalStoreRecordAuthenticationException("A persisted SQLite record envelope is malformed.", exception);
        }

        var plaintext = UnprotectBytes(envelope, context, column);
        try
        {
            return StrictUtf8.GetString(plaintext);
        }
        catch (DecoderFallbackException exception)
        {
            throw new LocalStoreRecordAuthenticationException("A persisted SQLite record holds invalid text.", exception);
        }
    }
}
