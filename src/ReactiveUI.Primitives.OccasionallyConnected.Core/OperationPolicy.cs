// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes the effective synchronization policy persisted with a local operation.</summary>
/// <param name="DeliveryGuarantee">The remote delivery guarantee required by the operation.</param>
/// <param name="Durability">The local durability requirement for the operation.</param>
/// <param name="Priority">The bounded relative priority used when leasing pending work.</param>
/// <param name="ConflictPolicy">The remote conflict handling policy.</param>
[System.Diagnostics.DebuggerDisplay("{DeliveryGuarantee,nq} {Durability,nq} {Priority,nq}")]
public sealed record OperationPolicy(
    DeliveryGuarantee DeliveryGuarantee,
    OperationDurability Durability,
    int Priority,
    ConflictPolicy ConflictPolicy)
{
    /// <summary>The default minimum operation priority.</summary>
    private const int DefaultMinimumPriority = -10;

    /// <summary>The default maximum operation priority.</summary>
    private const int DefaultMaximumPriority = 10;

    /// <summary>Gets the lowest valid operation priority.</summary>
    public static int MinimumPriority => DefaultMinimumPriority;

    /// <summary>Gets the highest valid operation priority.</summary>
    public static int MaximumPriority => DefaultMaximumPriority;

    /// <summary>Gets the default durable, at-least-once, merge policy.</summary>
    public static OperationPolicy Default { get; } = new(
        DeliveryGuarantee.AtLeastOnce,
        OperationDurability.Durable,
        Priority: 0,
        ConflictPolicy.Merge);

    /// <summary>Validates the policy before it is persisted with an operation.</summary>
    /// <exception cref="ArgumentException">The policy contains an undefined enum value or unsupported combination.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="Priority"/> is outside the valid range.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Validate() => Validate(DefaultMinimumPriority, DefaultMaximumPriority);

    /// <summary>Validates the policy before it is persisted with an operation.</summary>
    /// <param name="minimumPriority">The inclusive minimum accepted priority.</param>
    /// <param name="maximumPriority">The inclusive maximum accepted priority.</param>
    /// <exception cref="ArgumentException">The policy contains an undefined enum value or unsupported combination.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The priority range or value is outside the valid bounds.</exception>
    public void Validate(int minimumPriority, int maximumPriority)
    {
        ValidateDeliveryGuarantee();
        ValidateDurability();
        ValidateConflictPolicy();
        if (minimumPriority > maximumPriority)
        {
            throw new ArgumentOutOfRangeException(nameof(minimumPriority), minimumPriority, "Minimum priority must not exceed maximum priority.");
        }

        ValidateSupportedCombination();
        ValidatePriority(minimumPriority, maximumPriority);
    }

    /// <summary>Validates the delivery guarantee value.</summary>
    /// <exception cref="ArgumentException"><see cref="DeliveryGuarantee"/> is undefined.</exception>
    private void ValidateDeliveryGuarantee() =>
        _ = DeliveryGuarantee switch
        {
            DeliveryGuarantee.AtMostOnce or DeliveryGuarantee.AtLeastOnce or DeliveryGuarantee.ExactlyOnce => true,
            _ => throw new ArgumentException("Delivery guarantee must be a defined value.", nameof(DeliveryGuarantee)),
        };

    /// <summary>Validates the durability value.</summary>
    /// <exception cref="ArgumentException"><see cref="Durability"/> is undefined.</exception>
    private void ValidateDurability() =>
        _ = Durability switch
        {
            OperationDurability.Durable or OperationDurability.Volatile => true,
            _ => throw new ArgumentException("Durability must be a defined value.", nameof(Durability)),
        };

    /// <summary>Validates the conflict policy value.</summary>
    /// <exception cref="ArgumentException"><see cref="ConflictPolicy"/> is undefined.</exception>
    private void ValidateConflictPolicy() =>
        _ = ConflictPolicy switch
        {
            ConflictPolicy.Merge or ConflictPolicy.LastWriterWins or ConflictPolicy.Custom => true,
            _ => throw new ArgumentException("Conflict policy must be a defined value.", nameof(ConflictPolicy)),
        };

    /// <summary>Validates cross-property policy combinations.</summary>
    /// <exception cref="ArgumentException">The policy combination is unsupported.</exception>
    private void ValidateSupportedCombination() =>
        _ = (DeliveryGuarantee, Durability) switch
        {
            (DeliveryGuarantee.ExactlyOnce, not OperationDurability.Durable) =>
                throw new ArgumentException("Exactly-once operations must be durable.", nameof(DeliveryGuarantee)),
            _ => true,
        };

    /// <summary>Validates priority bounds.</summary>
    /// <param name="minimumPriority">The inclusive minimum accepted priority.</param>
    /// <param name="maximumPriority">The inclusive maximum accepted priority.</param>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="Priority"/> is outside the valid range.</exception>
    private void ValidatePriority(int minimumPriority, int maximumPriority) =>
        _ = Priority switch
        {
            var value when value < minimumPriority || value > maximumPriority =>
                throw new ArgumentOutOfRangeException(nameof(Priority), Priority, "Priority must be within the configured range."),
            _ => true,
        };
}
