// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Provides a local-first stream whose local state and input values use the same type.</summary>
/// <typeparam name="T">The local state and input value type.</typeparam>
public interface IOccasionallyConnectedStream<T> : IOccasionallyConnectedStream<T, T>;
