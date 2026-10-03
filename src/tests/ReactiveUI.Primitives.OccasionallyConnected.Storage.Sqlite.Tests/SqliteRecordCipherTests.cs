// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteRecordCipher"/>.</summary>
public sealed class SqliteRecordCipherTests
{
    /// <summary>The store identity used by tests.</summary>
    private const string StoreIdentity = "store-a";

    /// <summary>The key identifier used by tests.</summary>
    private const string KeyId = "cipher-key";

    /// <summary>The plaintext used by tests.</summary>
    private const string Plaintext = "sensitive-value";

    /// <summary>The text length used for bound checks.</summary>
    private const int LongTextLength = 100;

    /// <summary>An envelope version this build does not understand.</summary>
    private const byte UnknownEnvelopeVersion = 2;

    /// <summary>The key length used by tests.</summary>
    private const int KeyBytes = 32;

    /// <summary>The offset of the key identifier in the envelope.</summary>
    private const int KeyIdOffset = 2;

    /// <summary>The stream used by tests.</summary>
    private static readonly StreamId Stream = new("sensor/cipher");

    /// <summary>Gets the plaintext bytes used by tests.</summary>
    private static ReadOnlySpan<byte> PlaintextBytes => "sensitive-value"u8;

    /// <summary>Verifies the envelope carries the version, the key identifier, a nonce, a tag, and the ciphertext.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ProtectBytesWritesVersionedEnvelopeWithFreshNonce()
    {
        var cipher = CreateCipher(StoreIdentity);
        var context = SqliteRecordContext.Stream(Stream);
        var first = cipher.ProtectBytes(PlaintextBytes, context, SqliteRecordContext.ServerCursorColumn);
        var second = cipher.ProtectBytes(PlaintextBytes, context, SqliteRecordContext.ServerCursorColumn);

        await Assert.That(first[0]).IsEqualTo(SqliteRecordProtection.EnvelopeVersion);
        await Assert.That(Encoding.ASCII.GetString(first, KeyIdOffset, first[1])).IsEqualTo(KeyId);
        await Assert.That(first.Length).IsEqualTo(KeyIdOffset + KeyId.Length + SqliteRecordProtection.NonceBytes + SqliteRecordProtection.TagBytes + Plaintext.Length);
        await Assert.That(first.AsSpan().SequenceEqual(second)).IsFalse();
        await Assert.That(SqliteRecordCipher.ReadBytesKeyId(first)).IsEqualTo(KeyId);
        await Assert.That(Encoding.UTF8.GetString(cipher.UnprotectBytes(first, context, SqliteRecordContext.ServerCursorColumn))).IsEqualTo(Plaintext);
    }

    /// <summary>Verifies the associated data binds store identity, column, row, and payload metadata.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task UnprotectRejectsCiphertextMovedToAnotherStoreColumnOrRow()
    {
        var cipher = CreateCipher(StoreIdentity);
        var context = SqliteRecordContext.Stream(Stream);
        var stored = cipher.ProtectText(Plaintext, context, SqliteRecordContext.ServerCursorColumn);
        var otherStore = CreateCipher("store-b");
        var payloadContext = context.WithPayloadMetadata("reading", 1, "application/json");

        await Assert.That(cipher.UnprotectText(stored, context, SqliteRecordContext.ServerCursorColumn)).IsEqualTo(Plaintext);
        await Assert.That(() => otherStore.UnprotectText(stored, context, SqliteRecordContext.ServerCursorColumn))
            .ThrowsExactly<LocalStoreRecordAuthenticationException>();
        await Assert.That(() => cipher.UnprotectText(stored, context, SqliteRecordContext.CursorColumn))
            .ThrowsExactly<LocalStoreRecordAuthenticationException>();
        await Assert.That(() => cipher.UnprotectText(stored, SqliteRecordContext.Stream(new("sensor/other")), SqliteRecordContext.ServerCursorColumn))
            .ThrowsExactly<LocalStoreRecordAuthenticationException>();
        await Assert.That(() => cipher.UnprotectText(stored, payloadContext, SqliteRecordContext.ServerCursorColumn))
            .ThrowsExactly<LocalStoreRecordAuthenticationException>();
    }

    /// <summary>Verifies a changed header, a truncated envelope, and malformed text fail authentication.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task UnprotectRejectsMalformedAndTamperedEnvelopes()
    {
        var cipher = CreateCipher(StoreIdentity);
        var context = SqliteRecordContext.Stream(Stream);
        var envelope = cipher.ProtectBytes(PlaintextBytes, context, SqliteRecordContext.ServerCursorColumn);
        var wrongVersion = (byte[])envelope.Clone();
        wrongVersion[0] = UnknownEnvelopeVersion;
        var truncated = envelope.AsSpan(0, SqliteRecordProtection.MinimumEnvelopeOverhead - 1).ToArray();

        await Assert.That(() => cipher.UnprotectBytes(wrongVersion, context, SqliteRecordContext.ServerCursorColumn))
            .ThrowsExactly<LocalStoreRecordAuthenticationException>();
        await Assert.That(() => cipher.UnprotectBytes(truncated, context, SqliteRecordContext.ServerCursorColumn))
            .ThrowsExactly<LocalStoreRecordAuthenticationException>();
        await Assert.That(() => cipher.UnprotectText("not base64 !", context, SqliteRecordContext.ServerCursorColumn))
            .ThrowsExactly<LocalStoreRecordAuthenticationException>();
        await Assert.That(SqliteRecordCipher.ReadTextKeyId("not base64 !")).IsNull();
        await Assert.That(SqliteRecordCipher.ReadBytesKeyId(truncated)).IsNull();
    }

    /// <summary>Verifies a key the provider no longer holds fails authentication instead of returning data.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task UnprotectRejectsEnvelopeWhoseKeyIsMissing()
    {
        var context = SqliteRecordContext.Stream(Stream);
        var envelope = CreateCipher(StoreIdentity).ProtectBytes(PlaintextBytes, context, SqliteRecordContext.ServerCursorColumn);
        var otherKey = new StaticLocalStoreKeyProvider(new("other-key", new byte[KeyBytes]));
        var cipher = new SqliteRecordCipher(SqliteRecordProtection.Create(otherKey), StoreIdentity);

        await Assert.That(() => cipher.UnprotectBytes(envelope, context, SqliteRecordContext.ServerCursorColumn))
            .ThrowsExactly<LocalStoreRecordAuthenticationException>();
    }

    /// <summary>Verifies capacity bounds convert between plaintext and stored lengths conservatively.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task LengthBoundsAreConservative()
    {
        var cipher = CreateCipher(StoreIdentity);
        var text = new string('x', LongTextLength);
        var stored = cipher.ProtectText(text, SqliteRecordContext.Stream(Stream), SqliteRecordContext.ServerCursorColumn);

        await Assert.That((long)stored.Length).IsLessThanOrEqualTo(SqliteRecordCipher.ProtectedTextLengthUpperBound(text.Length));
        await Assert.That(SqliteRecordCipher.PlaintextUpperBoundFromText(stored.Length)).IsGreaterThanOrEqualTo(text.Length);
        await Assert.That(SqliteRecordCipher.PlaintextUpperBoundFromBlob(SqliteRecordProtection.MinimumEnvelopeOverhead)).IsEqualTo(0);
    }

    /// <summary>Creates a cipher for a store identity.</summary>
    /// <param name="storeIdentity">The store identity.</param>
    /// <returns>The cipher.</returns>
    private static SqliteRecordCipher CreateCipher(string storeIdentity)
    {
        var material = new byte[KeyBytes];
        Array.Fill(material, (byte)0x5A);
        var provider = new StaticLocalStoreKeyProvider(new(KeyId, material));
        return new(SqliteRecordProtection.Create(provider), storeIdentity);
    }
}
