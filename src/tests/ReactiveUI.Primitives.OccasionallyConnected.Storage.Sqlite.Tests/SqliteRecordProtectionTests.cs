// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteRecordProtection"/>.</summary>
public sealed class SqliteRecordProtectionTests
{
    /// <summary>Verifies malformed key identifiers are rejected before a provider is consulted.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task MalformedEnvelopeHeadersHaveNoKeyIdentifier()
    {
        var provider = new StaticLocalStoreKeyProvider(new LocalStoreKey("valid-key", new byte[32]));
        var protection = SqliteRecordProtection.Create(provider);
        var envelope = protection.Protect("value"u8, static header => header);
        var emptyKeyId = (byte[])envelope.Clone();
        emptyKeyId[1] = 0;
        var oversizedKeyId = (byte[])envelope.Clone();
        oversizedKeyId[1] = SqliteRecordProtection.MaximumKeyIdBytes + 1;
        var truncatedKeyId = (byte[])envelope.Clone();
        truncatedKeyId[1] = SqliteRecordProtection.MaximumKeyIdBytes;
        var controlCharacter = (byte[])envelope.Clone();
        controlCharacter[2] = 0x20;
        var nonAsciiCharacter = (byte[])envelope.Clone();
        nonAsciiCharacter[2] = 0x7F;

        await Assert.That(SqliteRecordProtection.TryReadKeyId(emptyKeyId)).IsNull();
        await Assert.That(SqliteRecordProtection.TryReadKeyId(oversizedKeyId)).IsNull();
        await Assert.That(SqliteRecordProtection.TryReadKeyId(truncatedKeyId)).IsNull();
        await Assert.That(SqliteRecordProtection.TryReadKeyId(controlCharacter)).IsNull();
        await Assert.That(SqliteRecordProtection.TryReadKeyId(nonAsciiCharacter)).IsNull();
    }

    /// <summary>Verifies a provider cannot substitute another key identifier during decryption.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ProviderReturningDifferentKeyIdentifierFailsAuthentication()
    {
        var originalKey = new LocalStoreKey("first", new byte[32]);
        var original = SqliteRecordProtection.Create(new StaticLocalStoreKeyProvider(originalKey));
        var envelope = original.Protect("value"u8, static header => header);
        var mismatched = SqliteRecordProtection.Create(new MismatchedKeyProvider());

        await Assert.That(() => mismatched.Unprotect(envelope, static header => header))
            .ThrowsExactly<LocalStoreRecordAuthenticationException>();
    }

    /// <summary>Supplies a different key identity for every lookup.</summary>
    private sealed class MismatchedKeyProvider : ILocalStoreKeyProvider
    {
        /// <inheritdoc/>
        public LocalStoreKey GetCurrentKey() => new("second", new byte[32]);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LocalStoreKey? GetKey(string keyId) => GetCurrentKey();
    }
}
