// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Microsoft.Extensions.Options;

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR;

/// <summary>Composes SignalR JSON framing with private generated carrier metadata.</summary>
internal sealed class SignalRHubProtocol : IHubProtocol
{
    /// <summary>The name selected by both carrier peers during the SignalR handshake.</summary>
    internal const string ProtocolName = "reactiveui-occasionally-connected-json";

    /// <summary>The framework implementation of JSON message framing.</summary>
    private readonly JsonHubProtocol _json = new(Options.Create(new JsonHubProtocolOptions
    { PayloadSerializerOptions = new() { TypeInfoResolver = SignalRCarrierJsonContext.Default } }));

    /// <inheritdoc/>
    public string Name => ProtocolName;

    /// <inheritdoc/>
    public int Version => _json.Version;

    /// <inheritdoc/>
    public TransferFormat TransferFormat => _json.TransferFormat;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsVersionSupported(int version) => _json.IsVersionSupported(version);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryParseMessage(ref ReadOnlySequence<byte> input, IInvocationBinder binder, [NotNullWhen(true)] out HubMessage? message) =>
        _json.TryParseMessage(ref input, binder, out message);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteMessage(HubMessage message, IBufferWriter<byte> output) => _json.WriteMessage(message, output);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlyMemory<byte> GetMessageBytes(HubMessage message) => _json.GetMessageBytes(message);
}
