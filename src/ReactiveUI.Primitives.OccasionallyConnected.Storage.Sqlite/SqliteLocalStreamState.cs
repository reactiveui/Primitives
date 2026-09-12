// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Stores durable local stream sequencing state.</summary>
/// <param name="NextClientSequence">The next client sequence.</param>
/// <param name="ServerCursor">The remote server cursor.</param>
internal readonly record struct SqliteLocalStreamState(long NextClientSequence, string? ServerCursor);
