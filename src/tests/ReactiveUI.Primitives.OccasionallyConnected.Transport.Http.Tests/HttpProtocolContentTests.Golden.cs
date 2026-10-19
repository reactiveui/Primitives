// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpProtocolContent"/> against the retained protocol-v1 media type fixture.</summary>
public sealed partial class HttpProtocolContentTests
{
    /// <summary>Verifies the protocol media type, including its major version parameter, matches the retained fixture.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GoldenMediaTypeMatchesProtocolContent() =>
        await Assert.That(HttpProtocolContent.MediaType).IsEqualTo(ProtocolGoldenFixtures.ReadText("media-type.txt"));
}
