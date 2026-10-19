// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text.Json.Serialization;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem;

/// <summary>Provides source-generated JSON metadata for durable journal types.</summary>
[JsonSerializable(typeof(FileSystemLocalStoreAdapter.JournalRecord))]
[JsonSerializable(typeof(PayloadEnvelope))]
[JsonSerializable(typeof(StreamId))]
internal sealed partial class FileSystemJsonContext : JsonSerializerContext;
