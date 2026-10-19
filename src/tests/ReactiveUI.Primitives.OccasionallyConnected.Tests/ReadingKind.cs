// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text.Json.Serialization;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Identifies the reading kind.</summary>
[JsonConverter(typeof(JsonStringEnumConverter<ReadingKind>))]
internal enum ReadingKind
{
    /// <summary>A temperature reading.</summary>
    Temperature = 0,
}
