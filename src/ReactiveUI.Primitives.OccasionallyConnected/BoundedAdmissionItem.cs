// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Represents an item admitted to a bounded occasionally connected queue.</summary>
/// <typeparam name="T">The item value type.</typeparam>
/// <param name="Value">The queued value.</param>
/// <param name="SizeBytes">The estimated byte size.</param>
/// <param name="Durable">Whether the item is protected by durable delivery semantics.</param>
/// <param name="Control">Whether the item carries control traffic.</param>
internal readonly record struct BoundedAdmissionItem<T>(T Value, long SizeBytes, bool Durable, bool Control)
{
    /// <summary>Gets a value indicating whether the item may be dropped by lossy policies.</summary>
    internal bool IsDropEligible => !Durable && !Control;
}
