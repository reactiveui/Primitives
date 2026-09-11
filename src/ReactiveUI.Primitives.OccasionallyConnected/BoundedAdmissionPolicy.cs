// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Chooses how a bounded queue handles overflow.</summary>
/// <typeparam name="T">The item value type.</typeparam>
/// <param name="snapshot">The immutable queue snapshot observed before invoking the policy.</param>
/// <param name="incoming">The incoming item.</param>
/// <returns>The deterministic overflow decision.</returns>
internal delegate BoundedAdmissionDecision BoundedAdmissionPolicy<T>(BoundedAdmissionSnapshot<T> snapshot, BoundedAdmissionItem<T> incoming);
