// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures local admission and remote delivery for published operations.</summary>
[DebuggerDisplay("{StreamId.Value,nq}; Durable={Durable,nq}; Guarantee={DeliveryGuarantee,nq}")]
public sealed record RemotePublishOptions
{
    /// <summary>Gets the stream receiving published operations.</summary>
    public required StreamId StreamId { get; init; }

    /// <summary>Gets a value indicating whether accepted operations are retained durably before remote delivery.</summary>
    public bool Durable { get; init; } = true;

    /// <summary>Gets the scheduling priority, validated against the configured range (by default -10 through 10).</summary>
    public int Priority { get; init; }

    /// <summary>Gets the conflict policy requested for remote application.</summary>
    public ConflictPolicy ConflictPolicy { get; init; } = ConflictPolicy.Merge;

    /// <summary>Gets the requested delivery guarantee.</summary>
    public DeliveryGuarantee DeliveryGuarantee { get; init; } = DeliveryGuarantee.AtLeastOnce;

    /// <summary>Gets the bounded outbox admission strategy.</summary>
    public BufferStrategy AdmissionStrategy { get; init; } = BufferStrategy.Block;

    /// <summary>Gets the optional base version used for optimistic concurrency checks.</summary>
    public string? BaseVersion { get; init; }

    /// <summary>Validates this option record using structural rules only.</summary>
    /// <exception cref="InvalidOperationException">The option record contains an invalid value.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Validate() => Validate(supportsCustomPolicy: false);

    /// <summary>Validates this option record.</summary>
    /// <param name="supportsCustomPolicy">Whether custom admission or conflict policies have been registered and supported.</param>
    /// <exception cref="InvalidOperationException">The option record contains an invalid value.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Validate(bool supportsCustomPolicy) => Validate(
        supportsCustomPolicy,
        OccasionallyConnectedOptionsValidation.MinimumPriority,
        OccasionallyConnectedOptionsValidation.MaximumPriority);

    /// <summary>Validates this option record.</summary>
    /// <param name="supportsCustomPolicy">Whether custom admission or conflict policies have been registered and supported.</param>
    /// <param name="minimumPriority">The inclusive minimum accepted priority.</param>
    /// <param name="maximumPriority">The inclusive maximum accepted priority.</param>
    /// <exception cref="InvalidOperationException">The option record contains an invalid value.</exception>
    public void Validate(bool supportsCustomPolicy, int minimumPriority, int maximumPriority)
    {
        OccasionallyConnectedOptionsValidation.ValidateStreamId(StreamId, nameof(StreamId));
        OccasionallyConnectedOptionsValidation.ValidateDeliveryGuarantee(DeliveryGuarantee);
        OccasionallyConnectedOptionsValidation.ValidateBufferStrategy(AdmissionStrategy);
        ValidateConflictPolicy();
        OccasionallyConnectedOptionsValidation.ValidatePriorityRange(minimumPriority, maximumPriority);
        ValidatePriority(minimumPriority, maximumPriority);
        OccasionallyConnectedOptionsValidation.ValidateCustomPolicy(UsesCustomPolicy(), supportsCustomPolicy);
        ValidateDurableGuarantee();
        ValidateDurableAdmissionStrategy();
    }

    /// <summary>Validates the configured conflict policy enum value.</summary>
    /// <exception cref="InvalidOperationException"><see cref="ConflictPolicy"/> is undefined.</exception>
    private void ValidateConflictPolicy()
    {
        if (ConflictPolicy is ConflictPolicy.LastWriterWins or ConflictPolicy.Merge or ConflictPolicy.Custom)
        {
            return;
        }

        throw new InvalidOperationException("ConflictPolicy must be a defined value.");
    }

    /// <summary>Validates the configured scheduling priority.</summary>
    /// <param name="minimumPriority">The inclusive minimum accepted priority.</param>
    /// <param name="maximumPriority">The inclusive maximum accepted priority.</param>
    /// <exception cref="InvalidOperationException"><see cref="Priority"/> is outside the supported range.</exception>
    private void ValidatePriority(int minimumPriority, int maximumPriority)
    {
        if (Priority >= minimumPriority && Priority <= maximumPriority)
        {
            return;
        }

        throw new InvalidOperationException("Priority must be within the configured range.");
    }

    /// <summary>Determines whether any selected policy is custom.</summary>
    /// <returns><see langword="true"/> when a selected policy is custom; otherwise, <see langword="false"/>.</returns>
    private bool UsesCustomPolicy() => AdmissionStrategy == BufferStrategy.Custom || ConflictPolicy == ConflictPolicy.Custom;

    /// <summary>Validates exactly-once durability requirements.</summary>
    /// <exception cref="InvalidOperationException">Exactly-once publishing is requested without durable retention.</exception>
    private void ValidateDurableGuarantee()
    {
        if (Durable || DeliveryGuarantee != DeliveryGuarantee.ExactlyOnce)
        {
            return;
        }

        throw new InvalidOperationException("ExactlyOnce publishing requires Durable to be true.");
    }

    /// <summary>Validates that durable work cannot be silently dropped.</summary>
    /// <exception cref="InvalidOperationException">Durable publishing uses a dropping strategy.</exception>
    private void ValidateDurableAdmissionStrategy()
    {
        if (!Durable || AdmissionStrategy is not (BufferStrategy.DropOldest or BufferStrategy.DropNewest))
        {
            return;
        }

        throw new InvalidOperationException("Durable publishing cannot use a dropping admission strategy.");
    }
}
