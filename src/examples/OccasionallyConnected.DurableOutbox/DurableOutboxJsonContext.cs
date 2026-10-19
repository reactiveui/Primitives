// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text.Json.Serialization;

namespace OccasionallyConnected.DurableOutbox;

/// <summary>Source-generated JSON metadata for the sample's allowlisted payload contracts.</summary>
[JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(TemperatureReading))]
[JsonSerializable(typeof(TemperatureSnapshot))]
internal sealed partial class DurableOutboxJsonContext : JsonSerializerContext;
