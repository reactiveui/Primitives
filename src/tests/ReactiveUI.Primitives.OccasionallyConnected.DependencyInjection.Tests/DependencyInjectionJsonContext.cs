// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text.Json.Serialization;

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection.Tests;

/// <summary>Source-generated JSON metadata for dependency-injection tests.</summary>
[JsonSerializable(typeof(DependencyInjectionTestDoubles.CounterInput))]
[JsonSerializable(typeof(DependencyInjectionTestDoubles.CounterState))]
internal sealed partial class DependencyInjectionJsonContext : JsonSerializerContext;
