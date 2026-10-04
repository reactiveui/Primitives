// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Net;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpTransportStatus"/> against the retained protocol-v1 error classification fixture.</summary>
public sealed partial class HttpTransportStatusTests
{
    /// <summary>The golden error classification fixture.</summary>
    private const string GoldenErrorClassificationFile = "error-status-classification.txt";

    /// <summary>Verifies every retained bodyless error response classifies to the retained failure kind.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GoldenErrorResponsesClassifyToRetainedFailureKinds()
    {
        var lines = ProtocolGoldenFixtures.ReadLines(GoldenErrorClassificationFile);

        await Assert.That(lines.Length).IsGreaterThan(0);
        foreach (var line in lines)
        {
            var separator = line.IndexOf(' ', StringComparison.Ordinal);
            var response = line[..separator];
            var expected = Enum.Parse<HttpTransportFailureKind>(line[(separator + 1)..]);
            using var message = CreateGoldenResponse(response);

            await Assert.That(HttpTransportStatus.Classify(message)).IsEqualTo(expected);
        }
    }

    /// <summary>Creates a bodyless response from one fixture response token.</summary>
    /// <param name="response">The status code and optional header in <c>status;name=value</c> form.</param>
    /// <returns>The response.</returns>
    private static HttpResponseMessage CreateGoldenResponse(string response)
    {
        var headerSeparator = response.IndexOf(';', StringComparison.Ordinal);
        var statusText = headerSeparator < 0 ? response : response[..headerSeparator];
        var message = new HttpResponseMessage((HttpStatusCode)int.Parse(statusText, NumberStyles.None, CultureInfo.InvariantCulture));
        if (headerSeparator < 0)
        {
            return message;
        }

        var header = response[(headerSeparator + 1)..];
        var equals = header.IndexOf('=', StringComparison.Ordinal);
        _ = message.Headers.TryAddWithoutValidation(header[..equals], header[(equals + 1)..]);
        return message;
    }
}
