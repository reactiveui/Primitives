// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text.Json.Serialization;

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR;

/// <summary>Supplies generated metadata for the carrier and SignalR's byte-array argument.</summary>
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(SignalRCarrierMessage))]
[JsonSerializable(typeof(byte[]))]
internal sealed partial class SignalRCarrierJsonContext : JsonSerializerContext;
