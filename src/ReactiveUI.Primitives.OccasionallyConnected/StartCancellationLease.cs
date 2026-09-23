// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Stores an atomically detached start generation and cancellation drain.</summary>
/// <param name="Generation">The detached generation.</param>
/// <param name="CancellationTask">The cancellation drain to await before disposal.</param>
internal readonly record struct StartCancellationLease(
    StartGeneration Generation,
    Task CancellationTask);
