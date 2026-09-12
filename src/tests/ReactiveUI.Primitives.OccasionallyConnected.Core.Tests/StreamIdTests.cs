// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="StreamId"/>.</summary>
public sealed class StreamIdTests
{
    /// <summary>Defines the UTF-8 byte limit for a stream identifier.</summary>
    private const int MaximumUtf8Bytes = 256;

    /// <summary>Defines the UTF-8 byte count for one U+00E9 character.</summary>
    private const int Utf8BytesPerEAcute = 2;

    /// <summary>Defines a representative valid stream identifier.</summary>
    private const string ValidStreamId = "sensor/temperature-v2_1.reading";

    /// <summary>Verifies that valid identifiers retain their value.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenValueIsValid_ThenPreservesItsValue()
    {
        var streamId = new StreamId(ValidStreamId);

        await Assert.That(streamId.Value).IsEqualTo(ValidStreamId);
        await Assert.That(streamId.ToString()).IsEqualTo(ValidStreamId);
    }

    /// <summary>Verifies that identifiers are normalized to Unicode NFC.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenValueUsesDecomposedUnicode_ThenNormalizesToNfc()
    {
        var streamId = new StreamId("cafe\u0301/temperature");

        await Assert.That(streamId.Value).IsEqualTo("caf\u00e9/temperature");
    }

    /// <summary>Verifies that supplementary-plane letters are valid.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenValueContainsSupplementaryLetter_ThenAcceptsIt()
    {
        var streamId = new StreamId("sensor/\U00010400");

        await Assert.That(streamId.Value).IsEqualTo("sensor/\U00010400");
    }

    /// <summary>Verifies that every permitted Unicode letter and digit category is accepted.</summary>
    /// <param name="character">The letter or digit in the identifier.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Arguments('A')]
    [Arguments('a')]
    [Arguments('\u01c5')]
    [Arguments('\u02b0')]
    [Arguments('\u4e00')]
    [Arguments('\u0661')]
    public async Task WhenValueContainsAnAllowedUnicodeCategory_ThenAcceptsIt(char character)
    {
        var streamId = new StreamId($"sensor/{character}");

        await Assert.That(streamId.Value).IsEqualTo($"sensor/{character}");
    }

    /// <summary>Verifies that identifiers outside the allowed grammar are rejected.</summary>
    /// <param name="value">The invalid stream identifier value.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Arguments(null)]
    [Arguments("")]
    [Arguments("/sensor")]
    [Arguments("sensor/")]
    [Arguments("sensor//temperature")]
    [Arguments("sensor/../temperature")]
    [Arguments("sensor/..temperature")]
    [Arguments("sensor/temperature?")]
    [Arguments("sensor/temperature\n")]
    public async Task WhenValueViolatesTheIdentifierGrammar_ThenThrowsArgumentException(string? value)
    {
        Action action = () => Create(value!);

        await Assert.That(action).Throws<ArgumentException>();
    }

    /// <summary>Verifies that an unpaired high surrogate is rejected.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenValueContainsAnUnpairedHighSurrogate_ThenThrowsArgumentException()
    {
        Action action = static () => Create("sensor/\ud800");

        await Assert.That(action).Throws<ArgumentException>();
    }

    /// <summary>Verifies that an unpaired low surrogate is rejected.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenValueContainsAnUnpairedLowSurrogate_ThenThrowsArgumentException()
    {
        Action action = static () => Create("sensor/\udc00");

        await Assert.That(action).Throws<ArgumentException>();
    }

    /// <summary>Verifies that a high surrogate not followed by a low surrogate is rejected.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenValueContainsAMismatchedSurrogatePair_ThenThrowsArgumentException()
    {
        Action action = static () => Create("sensor/\ud800a");

        await Assert.That(action).Throws<ArgumentException>();
    }

    /// <summary>Verifies that the UTF-8 boundary is inclusive.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenUtf8LengthIsAtTheLimit_ThenAcceptsIt()
    {
        var streamId = new StreamId(new string('a', MaximumUtf8Bytes));

        await Assert.That(streamId.Value).Length().IsEqualTo(MaximumUtf8Bytes);
    }

    /// <summary>Verifies that identifiers above the UTF-8 boundary are rejected.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenUtf8LengthExceedsTheLimit_ThenThrowsArgumentException()
    {
        Action action = static () => Create(new('a', MaximumUtf8Bytes + 1));

        await Assert.That(action).Throws<ArgumentException>();
    }

    /// <summary>Verifies that the UTF-8 byte boundary applies to multibyte characters.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenMultibyteUtf8LengthIsAtTheLimit_ThenAcceptsIt()
    {
        var streamId = new StreamId(new string('\u00e9', MaximumUtf8Bytes / Utf8BytesPerEAcute));

        await Assert.That(Encoding.UTF8.GetByteCount(streamId.Value)).IsEqualTo(MaximumUtf8Bytes);
    }

    /// <summary>Verifies that multibyte characters cannot exceed the UTF-8 byte boundary.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenMultibyteUtf8LengthExceedsTheLimit_ThenThrowsArgumentException()
    {
        Action action = static () => Create(new('\u00e9', (MaximumUtf8Bytes / Utf8BytesPerEAcute) + 1));

        await Assert.That(action).Throws<ArgumentException>();
    }

    /// <summary>Verifies that normalized identifier values compare equal.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenValuesNormalizeToTheSameValue_ThenTheyAreEqual()
    {
        var first = new StreamId("caf\u00e9");
        var second = new StreamId("cafe\u0301");

        await Assert.That(first).IsEqualTo(second);
        await Assert.That(first.GetHashCode()).IsEqualTo(second.GetHashCode());
    }

    /// <summary>Verifies that distinct identifier values do not compare equal.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenValuesDiffer_ThenTheyAreNotEqual()
    {
        var first = new StreamId("sensor/temperature");
        var second = new StreamId("sensor/humidity");

        await Assert.That(first).IsNotEqualTo(second);
    }

    /// <summary>Verifies default record struct equality.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDefaultValueIsCompared_ThenUsesRecordStructEquality()
    {
        StreamId first = default;
        StreamId second = default;

        await Assert.That(first).IsEqualTo(second);
        await Assert.That(first.ToString()).IsEqualTo(string.Empty);
    }

    /// <summary>Constructs a stream identifier for exception assertions.</summary>
    /// <param name="value">The stream identifier value to construct.</param>
    private static void Create(string value) => _ = new StreamId(value);
}
