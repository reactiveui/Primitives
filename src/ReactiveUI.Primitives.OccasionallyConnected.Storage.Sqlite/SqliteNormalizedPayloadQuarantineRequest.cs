// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>A normalized SQLite quarantine request with bounded non-null evidence.</summary>
/// <param name="Request">The normalized public request shape.</param>
/// <param name="Evidence">The normalized bounded evidence.</param>
internal readonly record struct SqliteNormalizedPayloadQuarantineRequest(
    LocalPayloadQuarantineRequest Request,
    LocalPayloadQuarantineEvidence Evidence);
