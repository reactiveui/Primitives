// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Reports the committed result of a bounded queue admission attempt.</summary>
/// <typeparam name="T">The queued value type.</typeparam>
/// <param name="Kind">The result kind.</param>
/// <param name="Item">The incoming item.</param>
/// <param name="EvictedItems">The queued items evicted to admit the incoming item.</param>
internal readonly record struct BoundedAdmissionResult<T>(
    BoundedAdmissionResultKind Kind,
    BoundedAdmissionItem<T> Item,
    IReadOnlyList<BoundedAdmissionItem<T>> EvictedItems);
