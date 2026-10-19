// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Returns the status and atomic view from a commit attempt.</summary>
/// <param name="Status">The commit status.</param>
/// <param name="Snapshot">The atomic stream snapshot observed by the attempt.</param>
internal sealed record ServerCommitResult(ServerCommitStatus Status, ServerCommitSnapshot Snapshot);
