// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text.Json;
using Microsoft.AspNetCore.SignalR.Protocol;

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR.Tests;

/// <summary>Checks private carrier metadata can serialize the full admitted byte envelope.</summary>
public sealed class SignalRHubProtocolTests
{
    /// <summary>Checks the largest admitted carrier retains every byte under JSON framing.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task MaximumCarrierSerializesWithGeneratedMetadata()
    {
        var carrier = new byte[SignalRCarrier.MaximumBytes];
        Array.Fill(carrier, byte.MaxValue);
        var protocol = new SignalRHubProtocol();
        var message = protocol.GetMessageBytes(new InvocationMessage("Exchange", [carrier]));
        using var json = JsonDocument.Parse(message[..^1]);
        var decoded = json.RootElement.GetProperty("arguments")[0].GetBytesFromBase64();
        await Assert.That(decoded.AsSpan().SequenceEqual(carrier)).IsTrue();
        await Assert.That(protocol.Name).IsEqualTo(SignalRHubProtocol.ProtocolName);
    }
}
