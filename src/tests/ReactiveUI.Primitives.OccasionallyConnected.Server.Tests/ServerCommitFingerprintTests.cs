// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="ServerCommitFingerprint"/>.</summary>
public sealed class ServerCommitFingerprintTests
{
    /// <summary>The original first fingerprint byte.</summary>
    private const byte OriginalByte = 1;

    /// <summary>The changed caller-owned source byte.</summary>
    private const byte MutatedSourceByte = 2;

    /// <summary>The changed caller-owned copy byte.</summary>
    private const byte MutatedCopyByte = 3;

    /// <summary>Verifies fingerprints require the canonical SHA-256 byte length.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConstructorRejectsNonCanonicalLength() =>
        await Assert.That(static () => new ServerCommitFingerprint(Array.Empty<byte>())).ThrowsExactly<ArgumentException>();

    /// <summary>Verifies byte copies are owned and mismatches are detected.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ToArrayReturnsOwnedCopyAndMatchesComparesBytes()
    {
        var bytes = new byte[ServerCommitFingerprint.Length];
        bytes[0] = OriginalByte;
        var fingerprint = new ServerCommitFingerprint(bytes);
        bytes[0] = MutatedSourceByte;
        var copy = fingerprint.ToArray();
        copy[0] = MutatedCopyByte;

        await Assert.That(fingerprint.ToArray()[0] == OriginalByte).IsTrue();
        await Assert.That(fingerprint.Matches(new(fingerprint.ToArray()))).IsTrue();
        await Assert.That(fingerprint.Matches(new(bytes))).IsFalse();
    }
}
