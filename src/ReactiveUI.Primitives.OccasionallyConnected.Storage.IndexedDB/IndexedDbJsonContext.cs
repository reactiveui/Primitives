// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text.Json.Serialization;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB;

/// <summary>Provides source-generated JSON metadata for IndexedDB store documents.</summary>
[JsonSerializable(typeof(IndexedDbLocalStoreAdapter.StoreState))]
[JsonSerializable(typeof(LocalSnapshot))]
[JsonSerializable(typeof(PayloadEnvelope))]
[JsonSerializable(typeof(StreamId))]
internal sealed partial class IndexedDbJsonContext : JsonSerializerContext;
