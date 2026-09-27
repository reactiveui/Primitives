// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net.Http;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests protocol-v1 version negotiation of <see cref="HttpRemoteTransportCapabilities"/> against retained fixtures.</summary>
public sealed partial class HttpRemoteTransportCapabilitiesTests
{
    /// <summary>The golden connect request fixture.</summary>
    private const string GoldenConnectRequestFile = "connect-request.json";

    /// <summary>The golden connect response fixture.</summary>
    private const string GoldenConnectResponseFile = "connect-response.json";

    /// <summary>The HTTP client required by codec options.</summary>
    private static readonly HttpClient GoldenHttpClient = new();

    /// <summary>Verifies the golden request and golden response negotiate the server's lower shared minor.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GoldenConnectExchangeNegotiatesLowerSharedMinor()
    {
        var codec = CreateGoldenCodec();
        var request = codec.DeserializeConnectRequest(ProtocolGoldenFixtures.ReadBytes(GoldenConnectRequestFile));
        var response = codec.DeserializeConnectResponse(ProtocolGoldenFixtures.ReadBytes(GoldenConnectResponseFile));

        HttpRemoteTransportCapabilities.ValidateNegotiation(request, response, response.Features);

        await Assert.That(request.SupportedProtocolVersions.Maximum).IsGreaterThan(response.ProtocolVersion);
        await Assert.That(response.ProtocolVersion).IsEqualTo(request.SupportedProtocolVersions.Minimum);
    }

    /// <summary>Verifies every advertised v1 minor inside the golden request range negotiates.</summary>
    /// <param name="minor">The advertised server minor version.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    public async Task GoldenConnectRequestAcceptsMinorInsideRange(int minor)
    {
        var request = CreateGoldenCodec().DeserializeConnectRequest(ProtocolGoldenFixtures.ReadBytes(GoldenConnectRequestFile));
        var response = CreateGoldenResponse() with { ProtocolVersion = new(1, minor) };

        HttpRemoteTransportCapabilities.ValidateNegotiation(request, response, response.Features);

        await Assert.That(response.ProtocolVersion.Major).IsEqualTo(request.SupportedProtocolVersions.Maximum.Major);
    }

    /// <summary>Verifies a newer minor or a different major outside the golden request range fails negotiation.</summary>
    /// <param name="major">The advertised server major version.</param>
    /// <param name="minor">The advertised server minor version.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(1, 2)]
    [Arguments(2, 0)]
    [Arguments(0, 9)]
    public async Task GoldenConnectRequestRejectsVersionOutsideRange(int major, int minor)
    {
        var request = CreateGoldenCodec().DeserializeConnectRequest(ProtocolGoldenFixtures.ReadBytes(GoldenConnectRequestFile));
        var response = CreateGoldenResponse() with { ProtocolVersion = new(major, minor) };

        var exception = await CaptureHttpExceptionAsync(() => HttpRemoteTransportCapabilities.ValidateNegotiation(request, response, response.Features));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.SchemaIncompatible);
    }

    /// <summary>Decodes the golden connect response.</summary>
    /// <returns>The negotiated capabilities.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static NegotiatedCapabilities CreateGoldenResponse() =>
        CreateGoldenCodec().DeserializeConnectResponse(ProtocolGoldenFixtures.ReadBytes(GoldenConnectResponseFile));

    /// <summary>Creates a codec with default client limits.</summary>
    /// <returns>The codec.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpProtocolCodec CreateGoldenCodec() =>
        new(new HttpRemoteTransportOptions { HttpClient = GoldenHttpClient, BaseAddress = new("https://example.invalid/oc/") });
}
