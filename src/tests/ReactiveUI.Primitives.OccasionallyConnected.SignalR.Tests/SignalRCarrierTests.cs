// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;
using System.Text.Json;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR.Tests;

/// <summary>Checks the independent carrier bounds and strict generated schema.</summary>
public sealed class SignalRCarrierTests
{
    /// <summary>Checks malformed JSON and incomplete carrier bodies fail closed.</summary>
    /// <param name="json">The malformed body.</param>
    /// <returns>The test task.</returns>
    [Test]
    [Arguments("{")]
    [Arguments("{}")]
    [Arguments("null")]
    [Arguments("{\"body\":null,\"headers\":{}}")]
    public async Task MalformedCarrierIsRejected(string json)
    {
        var failure = Assert.ThrowsExactly<HttpRemoteTransportException>(
            () => SignalRCarrier.Decode(Encoding.UTF8.GetBytes(json)));
        await Assert.That(failure.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Checks input byte limits before parsing.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task EncodedCarrierLimitIsEnforcedBeforeDeserialization()
    {
        var failure = Assert.ThrowsExactly<HttpRemoteTransportException>(
            static () => SignalRCarrier.Decode(new byte[SignalRCarrier.MaximumBytes + 1]));
        await Assert.That(failure.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Checks body limits before serialization can allocate encoded output.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task BodyLimitIsEnforcedBeforeSerialization()
    {
        var failure = Assert.ThrowsExactly<HttpRemoteTransportException>(
            static () => SignalRCarrier.Encode(new("POST", "/push", 0, [], new byte[SignalRCarrier.MaximumBodyBytes + 1])));
        await Assert.That(failure.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Checks each invalid bounded-header shape before wire output.</summary>
    /// <param name="kind">The invalid header shape.</param>
    /// <returns>The test task.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    [Arguments(4)]
    [Arguments(5)]
    public async Task InvalidHeadersAreRejected(int kind)
    {
        const int HeaderCapacity = 32;
        const int HeaderValueCapacity = 8;
        const int HeaderCharacterCapacity = 32 * 1024;
        const int TooManyValues = 2;
        const int NullValue = 3;
        const int OversizedValue = 4;
        var headers = new Dictionary<string, string[]>();
        switch (kind)
        {
            case 0:
                {
                    headers.Add(string.Empty, []);
                    break;
                }

            case 1:
                {
                    headers.Add("key", null!);
                    break;
                }

            case TooManyValues:
                {
                    headers.Add("key", new string[HeaderValueCapacity + 1]);
                    break;
                }

            case NullValue:
                {
                    headers.Add("key", [null!]);
                    break;
                }

            case OversizedValue:
                {
                    headers.Add("key", [new('x', HeaderCharacterCapacity + 1)]);
                    break;
                }

            default:
                {
                    for (var index = 0; index <= HeaderCapacity; index++)
                    {
                        headers.Add(index.ToString(System.Globalization.CultureInfo.InvariantCulture), []);
                    }

                    break;
                }
        }

        _ = Assert.ThrowsExactly<HttpRemoteTransportException>(() => SignalRCarrier.Encode(new(null, null, 0, headers, [])));
        await Assert.That(headers).IsNotNull();
    }

    /// <summary>Checks the generated serializer's nullable carrier shapes before protocol admission.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task MissingBodyAndHeadersAreRejected()
    {
        var missingBody = new SignalRCarrierMessage(null, null, 0, [], null!);
        var missingHeaders = new SignalRCarrierMessage(null, null, 0, null!, []);
        _ = Assert.ThrowsExactly<HttpRemoteTransportException>(() => SignalRCarrier.Encode(missingBody));
        _ = Assert.ThrowsExactly<HttpRemoteTransportException>(() => SignalRCarrier.Encode(missingHeaders));
        await Assert.That(missingBody.Body).IsNull();
    }

    /// <summary>Checks content headers cannot be smuggled into a content-free pull request.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task ContentHeadersWithoutContentAreRejected()
    {
        using var request = new HttpRequestMessage();
        using var content = new ByteArrayContent([]);
        var headers = new Dictionary<string, string[]> { ["Content-Type"] = ["application/json"] };
        _ = Assert.ThrowsExactly<HttpRemoteTransportException>(() => SignalRCarrier.SetHeaders(headers, request.Headers, null));
        var invalid = new Dictionary<string, string[]> { ["not a header"] = ["value"] };
        _ = Assert.ThrowsExactly<HttpRemoteTransportException>(() => SignalRCarrier.SetHeaders(invalid, request.Headers, content));
        await Assert.That(request.Content).IsNull();
    }

    /// <summary>Checks generated serialization includes all nullable message fields and owned headers.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task GeneratedCarrierRoundTripsNullableFieldsAndMultipleHeaders()
    {
        const int HeaderValues = 2;
        var message = new SignalRCarrierMessage(null, null, 0, new() { ["Accept"] = ["one", "two"] }, []);
        var bytes = JsonSerializer.SerializeToUtf8Bytes(message, SignalRCarrierJsonContext.Default.SignalRCarrierMessage);
        var decoded = SignalRCarrier.Decode(bytes);
        await Assert.That(decoded.Method).IsNull();
        await Assert.That(decoded.PathAndQuery).IsNull();
        await Assert.That(decoded.Headers["Accept"]).Count().IsEqualTo(HeaderValues);
    }
}
