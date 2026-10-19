// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reflection;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="StartPosition"/>.</summary>
public sealed class StartPositionTests
{
    /// <summary>The maximum permitted UTF-8 cursor length.</summary>
    private const int CursorUtf8ByteLimit = 4096;

    /// <summary>The number of UTF-8 bytes in an e acute character.</summary>
    private const int Utf8BytesPerEAcute = 2;

    /// <summary>The e acute character count that exactly consumes the cursor limit.</summary>
    private const int CursorLengthAtUtf8ByteLimit = CursorUtf8ByteLimit / Utf8BytesPerEAcute;

    /// <summary>The ASCII cursor length that exceeds the cursor limit.</summary>
    private const int CursorLengthAboveUtf8ByteLimit = CursorUtf8ByteLimit + 1;

    /// <summary>Verifies the latest singleton has no position payload.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LatestHasNoPositionPayload()
    {
        var position = StartPosition.Latest;

        await Assert.That(position.Kind).IsEqualTo(StartPositionKind.Latest);
        await Assert.That(position.Timestamp).IsNull();
        await Assert.That(position.Sequence).IsNull();
        await Assert.That(position.Cursor).IsNull();
        await Assert.That(StartPosition.Latest).IsSameReferenceAs(position);
    }

    /// <summary>Verifies a timestamp position retains only its timestamp.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromTimestampRetainsTimestampOnly()
    {
        DateTimeOffset timestamp = new(2026, 9, 11, 10, 30, 0, TimeSpan.FromHours(1));

        var position = StartPosition.FromTimestamp(timestamp);

        await Assert.That(position.Kind).IsEqualTo(StartPositionKind.FromTimestamp);
        await Assert.That(position.Timestamp).IsEqualTo(timestamp);
        await Assert.That(position.Sequence).IsNull();
        await Assert.That(position.Cursor).IsNull();
    }

    /// <summary>Verifies non-negative server sequences are retained without another payload.</summary>
    /// <param name="sequence">The server-assigned sequence.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments(0L)]
    [Arguments(long.MaxValue)]
    public async Task FromSequenceRetainsNonNegativeServerSequenceOnly(long sequence)
    {
        var position = StartPosition.FromSequence(sequence);

        await Assert.That(position.Kind).IsEqualTo(StartPositionKind.FromSequence);
        await Assert.That(position.Timestamp).IsNull();
        await Assert.That(position.Sequence).IsEqualTo(sequence);
        await Assert.That(position.Cursor).IsNull();
    }

    /// <summary>Verifies negative server sequences are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromSequenceRejectsNegativeServerSequence()
    {
        var action = static () => StartPosition.FromSequence(-1);

        await Assert.That(action).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies a cursor remains opaque and is not normalized or otherwise changed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromCursorRetainsOpaqueCursorWithoutNormalization()
    {
        const string Cursor = "cursor/cafe\u0301/\U0001F642";

        var position = StartPosition.FromCursor(Cursor);

        await Assert.That(position.Kind).IsEqualTo(StartPositionKind.FromCursor);
        await Assert.That(position.Timestamp).IsNull();
        await Assert.That(position.Sequence).IsNull();
        await Assert.That(position.Cursor).IsEqualTo(Cursor);
    }

    /// <summary>Verifies a cursor whose UTF-8 representation is exactly 4096 bytes is accepted.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromCursorAcceptsCursorAtUtf8ByteLimit()
    {
        var cursor = new string('\u00e9', CursorLengthAtUtf8ByteLimit);

        var position = StartPosition.FromCursor(cursor);

        await Assert.That(position.Cursor).IsEqualTo(cursor);
    }

    /// <summary>Verifies a cursor whose UTF-8 representation exceeds 4096 bytes is rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromCursorRejectsCursorAboveUtf8ByteLimit()
    {
        var action = static () => StartPosition.FromCursor(new('a', CursorLengthAboveUtf8ByteLimit));

        await Assert.That(action).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies null and empty cursors are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The expected factory is unavailable.</exception>
    [Test]
    public async Task FromCursorRejectsMissingCursor()
    {
        var factory = typeof(StartPosition).GetMethod(nameof(StartPosition.FromCursor), [typeof(string)])
            ?? throw new InvalidOperationException("The cursor factory is unavailable.");
        var emptyAction = static () => StartPosition.FromCursor(string.Empty);

        var exception = await Assert.That(() => factory.Invoke(null, [null])).ThrowsExactly<TargetInvocationException>();
        await Assert.That(exception?.InnerException).IsTypeOf<ArgumentNullException>();
        await Assert.That(emptyAction).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies malformed UTF-16 cursor data is rejected before it can be encoded as UTF-8.</summary>
    /// <param name="cursor">The malformed cursor.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments("\ud800")]
    [Arguments("\ud800x")]
    [Arguments("\udc00")]
    public async Task FromCursorRejectsMalformedUnicode(string cursor)
    {
        var action = () => StartPosition.FromCursor(cursor);

        await Assert.That(action).ThrowsExactly<ArgumentException>();
    }
}
