// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Stores the result of accepting a context stop operation.</summary>
/// <param name="StopTask">The shared stop completion task.</param>
/// <param name="Completion">The accepted stop completion source, or <see langword="null"/> when joining an existing stop.</param>
internal readonly record struct AcceptedStopDecision(Task StopTask, TaskCompletionSource<bool>? Completion);
