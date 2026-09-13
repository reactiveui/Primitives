// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes the outcome of one prepared upload attempt.</summary>
/// <param name="Barriers">The durable attempt barrier decisions.</param>
/// <param name="Sent">Whether the prepared handle was sent.</param>
/// <param name="Reconciled">Whether the response was passed to reconciliation.</param>
internal sealed record PreparedUploadAttemptResult(IReadOnlyList<AttemptBarrierResult> Barriers, bool Sent, bool Reconciled);
