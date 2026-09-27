// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="LocalStoreKey"/>.</summary>
public sealed class LocalStoreKeyTests
{
    /// <summary>The smallest accepted key length in bytes.</summary>
    private const int MinimumKeyBytes = 32;

    /// <summary>A key identifier length one character over the limit.</summary>
    private const int TooLongKeyIdLength = 65;

    /// <summary>The key identifier used by tests.</summary>
    private const string KeyId = "device-key.v1";

    /// <summary>Verifies keys copy their material and never print it.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorCopiesMaterialAndToStringHidesIt()
    {
        var material = new byte[MinimumKeyBytes];
        Array.Fill(material, (byte)0x41);
        var key = new LocalStoreKey(KeyId, material);
        material[0] = 0;

        await Assert.That(key.KeyId).IsEqualTo(KeyId);
        await Assert.That(key.KeyMaterial.Length).IsEqualTo(MinimumKeyBytes);
        await Assert.That(key.KeyMaterial[0]).IsEqualTo((byte)0x41);
        await Assert.That(key.ToString()).IsEqualTo($"LocalStoreKey {KeyId}");
        await Assert.That(key.ToString()).DoesNotContain("AAAA");
    }

    /// <summary>Verifies keys shorter than 256 bits and malformed identifiers are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsShortKeysAndMalformedIdentifiers()
    {
        await Assert.That(static () => new LocalStoreKey(KeyId, new byte[MinimumKeyBytes - 1])).ThrowsExactly<ArgumentException>();
        await Assert.That(static () => new LocalStoreKey(string.Empty, new byte[MinimumKeyBytes])).ThrowsExactly<ArgumentException>();
        await Assert.That(static () => new LocalStoreKey("key id", new byte[MinimumKeyBytes])).ThrowsExactly<ArgumentException>();
        await Assert.That(static () => new LocalStoreKey(new string('k', TooLongKeyIdLength), new byte[MinimumKeyBytes])).ThrowsExactly<ArgumentException>();
        await Assert.That(static () => new LocalStoreKey(KeyId, null!)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies random keys are 256-bit and distinct.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CreateRandomReturnsDistinct256BitKeys()
    {
        var first = LocalStoreKey.CreateRandom(KeyId);
        var second = LocalStoreKey.CreateRandom(KeyId);

        await Assert.That(first.KeyMaterial.Length).IsEqualTo(MinimumKeyBytes);
        await Assert.That(first.KeyMaterial.SequenceEqual(second.KeyMaterial)).IsFalse();
    }
}
