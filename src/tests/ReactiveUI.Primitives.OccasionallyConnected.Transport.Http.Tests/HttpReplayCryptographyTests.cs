// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpReplayCryptography"/>.</summary>
public sealed class HttpReplayCryptographyTests
{
    /// <summary>The first byte value.</summary>
    private const byte FirstByte = 1;

    /// <summary>The second byte value.</summary>
    private const byte SecondByte = 2;

    /// <summary>The third byte value.</summary>
    private const byte ThirdByte = 3;

    /// <summary>The changed byte value.</summary>
    private const byte ChangedByte = 4;

    /// <summary>Verifies equal byte sequences are accepted.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task FixedTimeEqualsAcceptsEqualBytes()
    {
        byte[] left = [FirstByte, SecondByte, ThirdByte];
        byte[] right = [FirstByte, SecondByte, ThirdByte];

        await Assert.That(HttpReplayCryptography.FixedTimeEquals(left, right)).IsTrue();
    }

    /// <summary>Verifies one changed byte rejects otherwise equal-length input.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task FixedTimeEqualsRejectsDifferentByte()
    {
        byte[] left = [FirstByte, SecondByte, ThirdByte];
        byte[] right = [FirstByte, ChangedByte, ThirdByte];

        await Assert.That(HttpReplayCryptography.FixedTimeEquals(left, right)).IsFalse();
    }

    /// <summary>Verifies different lengths are rejected.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task FixedTimeEqualsRejectsDifferentLength()
    {
        byte[] left = [FirstByte, SecondByte, ThirdByte];
        byte[] right = [FirstByte, SecondByte];

        await Assert.That(HttpReplayCryptography.FixedTimeEquals(left, right)).IsFalse();
    }

    /// <summary>Verifies zeroing clears every byte.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ZeroMemoryClearsAllBytes()
    {
        byte[] buffer = [FirstByte, SecondByte, ThirdByte];

        HttpReplayCryptography.ZeroMemory(buffer);

        await Assert.That(Array.TrueForAll(buffer, static value => value == 0)).IsTrue();
    }
}
