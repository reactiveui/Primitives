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
    /// <summary>The provider key material length.</summary>
    private const int ProviderKeyBytes = 32;

    /// <summary>The maximum retained cipher key count.</summary>
    private const int CipherCacheKeyCapacity = 8;

    /// <summary>The expected derivation count after filling, evicting and revisiting a key.</summary>
    private const long DerivationsAfterEviction = 10;

    /// <summary>The original and replacement key derivations.</summary>
    private const long DerivationsAfterReplacement = 2;

    /// <summary>The test plaintext.</summary>
    private const string PlaintextValue = "value";

    /// <summary>The key identifier used for cache tests.</summary>
    private const string CachedKeyId = "cached";

    /// <summary>Gets the UTF-8 test plaintext.</summary>
    private static ReadOnlySpan<byte> PlaintextBytes => "value"u8;

    /// <summary>Verifies malformed key identifiers are rejected before a provider is consulted.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task MalformedEnvelopeHeadersHaveNoKeyIdentifier()
    {
        var provider = new StaticLocalStoreKeyProvider(new LocalStoreKey("valid-key", new byte[ProviderKeyBytes]));
        var protection = SqliteRecordProtection.Create(provider);
        var envelope = protection.Protect(PlaintextBytes, static header => header);
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
        var originalKey = new LocalStoreKey("first", new byte[ProviderKeyBytes]);
        var original = SqliteRecordProtection.Create(new StaticLocalStoreKeyProvider(originalKey));
        var envelope = original.Protect(PlaintextBytes, static header => header);
        var mismatched = SqliteRecordProtection.Create(new MismatchedKeyProvider());

        await Assert.That(() => mismatched.Unprotect(envelope, static header => header))
            .ThrowsExactly<LocalStoreRecordAuthenticationException>();
    }

    /// <summary>Checks one derivation serves repeated encryption and decryption without reusing a nonce.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DerivedCipherIsReusedButEveryEncryptionGetsAFreshNonce()
    {
        using var protection = SqliteRecordProtection.Create(new StaticLocalStoreKeyProvider(new LocalStoreKey(CachedKeyId, new byte[ProviderKeyBytes])));
        var first = protection.Protect(PlaintextBytes, static header => header);
        var second = protection.Protect(PlaintextBytes, static header => header);
        await Assert.That(Convert.ToHexString(first)).IsNotEqualTo(Convert.ToHexString(second));
        await Assert.That(System.Text.Encoding.UTF8.GetString(protection.Unprotect(first, static header => header))).IsEqualTo(PlaintextValue);
        await Assert.That(System.Text.Encoding.UTF8.GetString(protection.Unprotect(second, static header => header))).IsEqualTo(PlaintextValue);
        await Assert.That(protection.DerivationCount).IsEqualTo(1L);
    }

    /// <summary>Checks cached ciphers do not bypass revocation or replacement of material under the same identifier.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CachedCipherHonorsRevocationAndSameIdentifierReplacement()
    {
        var provider = new MutableKeyProvider(new(CachedKeyId, new byte[ProviderKeyBytes]));
        using var protection = SqliteRecordProtection.Create(provider);
        var first = protection.Protect(PlaintextBytes, static header => header);
        provider.Available = false;
        await Assert.That(() => protection.Unprotect(first, static header => header))
            .ThrowsExactly<LocalStoreRecordAuthenticationException>();
        provider.Available = true;
        provider.Current = new(CachedKeyId, Enumerable.Repeat((byte)1, ProviderKeyBytes).ToArray());
        await Assert.That(() => protection.Unprotect(first, static header => header))
            .ThrowsExactly<LocalStoreRecordAuthenticationException>();
        var replacement = protection.Protect("new"u8, static header => header);
        await Assert.That(System.Text.Encoding.UTF8.GetString(protection.Unprotect(replacement, static header => header))).IsEqualTo("new");
        await Assert.That(protection.DerivationCount).IsEqualTo(DerivationsAfterReplacement);
        provider.Current = null!;
        await Assert.That(() => protection.Protect("new"u8, static header => header)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Checks the cipher cache is bounded and disposal forbids new cryptographic work.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CipherCacheEvictsOldKeysAndDisposalReleasesResources()
    {
        var original = new LocalStoreKey("key-0", new byte[ProviderKeyBytes]);
        var provider = new MutableKeyProvider(original);
        var protection = SqliteRecordProtection.Create(provider);
        try
        {
            _ = protection.Protect(PlaintextBytes, static header => header);
            for (var index = 1; index <= CipherCacheKeyCapacity; index++)
            {
                provider.Current = new($"key-{index}", new byte[ProviderKeyBytes]);
                _ = protection.Protect(PlaintextBytes, static header => header);
            }

            provider.Current = original;
            _ = protection.Protect(PlaintextBytes, static header => header);
            await Assert.That(protection.DerivationCount).IsEqualTo(DerivationsAfterEviction);
            protection.Dispose();
            protection.Dispose();
            await Assert.That(() => protection.Protect(PlaintextBytes, static header => header)).ThrowsExactly<ObjectDisposedException>();
            await Assert.That(() => protection.Unprotect([], static header => header)).ThrowsExactly<ObjectDisposedException>();
            await Assert.That(() => protection.GetCurrentKeyId()).ThrowsExactly<ObjectDisposedException>();
        }
        finally
        {
            protection.Dispose();
        }
    }

    /// <summary>A key provider whose material and availability can change after cache warming.</summary>
    /// <param name="current">The initial provider key.</param>
    private sealed class MutableKeyProvider(LocalStoreKey current) : ILocalStoreKeyProvider
    {
        /// <summary>Gets or sets the current provider key.</summary>
        internal LocalStoreKey Current { get; set; } = current;

        /// <summary>Gets or sets whether historical resolution is allowed.</summary>
        internal bool Available { get; set; } = true;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LocalStoreKey GetCurrentKey() => Current;

        /// <inheritdoc/>
        public LocalStoreKey? GetKey(string keyId) => Available && Current.KeyId == keyId ? Current : null;
    }

    /// <summary>Supplies a different key identity for every lookup.</summary>
    private sealed class MismatchedKeyProvider : ILocalStoreKeyProvider
    {
        /// <inheritdoc/>
        public LocalStoreKey GetCurrentKey() => new("second", new byte[ProviderKeyBytes]);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LocalStoreKey? GetKey(string keyId) => GetCurrentKey();
    }
}
