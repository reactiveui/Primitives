// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a stable queue state observed by a custom admission policy.</summary>
/// <typeparam name="T">The queued value type.</typeparam>
/// <param name="Items">The queued items in FIFO order.</param>
/// <param name="Count">The admitted item count.</param>
/// <param name="Bytes">The admitted byte count.</param>
/// <param name="Capacity">The maximum admitted item count.</param>
/// <param name="CapacityBytes">The maximum admitted byte count.</param>
/// <param name="ReservedControlCapacity">The item count reserved for control traffic.</param>
/// <param name="ReservedControlBytes">The byte count reserved for control traffic.</param>
/// <param name="Version">The queue version used to revalidate custom decisions.</param>
internal readonly record struct BoundedAdmissionSnapshot<T>(
    IReadOnlyList<BoundedAdmissionItem<T>> Items,
    int Count,
    long Bytes,
    int Capacity,
    long CapacityBytes,
    int ReservedControlCapacity,
    long ReservedControlBytes,
    long Version);
