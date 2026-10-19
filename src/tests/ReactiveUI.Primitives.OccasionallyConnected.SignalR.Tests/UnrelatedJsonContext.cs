// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text.Json.Serialization;

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR.Tests;

/// <summary>The host's custom source-generated JSON metadata.</summary>
[JsonSerializable(typeof(UnrelatedMessage))]
internal sealed partial class UnrelatedJsonContext : JsonSerializerContext;
