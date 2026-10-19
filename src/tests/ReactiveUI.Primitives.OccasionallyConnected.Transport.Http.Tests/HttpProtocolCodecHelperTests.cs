// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpProtocolCodecHelper"/>.</summary>
public sealed class HttpProtocolCodecHelperTests
{
    /// <summary>The expected JSON null byte count.</summary>
    private const int JsonNullByteCount = 4;

    /// <summary>The expected JSON string byte count for one ASCII character.</summary>
    private const int OneCharacterJsonStringByteCount = 8;

    /// <summary>Verifies optional JSON string size estimation treats null as a JSON null token.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task EstimateOptionalJsonStringBytesReturnsNullTokenSize()
    {
        var bytes = HttpProtocolCodecHelper.EstimateOptionalJsonStringBytes(null);

        await Assert.That(bytes).IsEqualTo(JsonNullByteCount);
    }

    /// <summary>Verifies optional JSON string size estimation delegates non-null strings to escaped string estimation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task EstimateOptionalJsonStringBytesReturnsEscapedStringSize()
    {
        var bytes = HttpProtocolCodecHelper.EstimateOptionalJsonStringBytes("x");

        await Assert.That(bytes).IsEqualTo(OneCharacterJsonStringByteCount);
    }
}
