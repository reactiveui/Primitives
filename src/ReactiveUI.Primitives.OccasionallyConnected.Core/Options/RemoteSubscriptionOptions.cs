// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures a logical remote stream subscription.</summary>
[DebuggerDisplay("{StreamId.Value,nq}; Subscription={SubscriptionId,nq}; Guarantee={DeliveryGuarantee,nq}")]
public sealed record RemoteSubscriptionOptions
{
    /// <summary>Defines the default remote subscription item capacity.</summary>
    private const int DefaultBufferCapacity = 1024;

    /// <summary>Defines the default remote subscription byte capacity.</summary>
    private const long DefaultBufferCapacityBytes = 16 * OccasionallyConnectedOptionsValidation.BytesPerMebibyte;

    /// <summary>Gets the stream consumed by the subscription.</summary>
    public required StreamId StreamId { get; init; }

    /// <summary>Gets the optional durable subscription identity to resume.</summary>
    public SubscriptionId? SubscriptionId { get; init; }

    /// <summary>Gets the initial remote start position used when no durable cursor has been recovered.</summary>
    public StartPosition StartPosition { get; init; } = StartPosition.Latest;

    /// <summary>Gets the requested delivery guarantee.</summary>
    public DeliveryGuarantee DeliveryGuarantee { get; init; } = DeliveryGuarantee.AtLeastOnce;

    /// <summary>Gets the remote event buffer admission strategy.</summary>
    public BufferStrategy BufferStrategy { get; init; } = BufferStrategy.Block;

    /// <summary>Gets the maximum number of remote events buffered for delivery.</summary>
    public int BufferCapacity { get; init; } = DefaultBufferCapacity;

    /// <summary>Gets the maximum estimated number of buffered remote event bytes.</summary>
    public long BufferCapacityBytes { get; init; } = DefaultBufferCapacityBytes;

    /// <summary>Validates this option record using structural rules only.</summary>
    /// <exception cref="InvalidOperationException">The option record contains an invalid value.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Validate() => Validate(supportsCustomPolicy: false);

    /// <summary>Validates this option record.</summary>
    /// <param name="supportsCustomPolicy">Whether a custom buffer policy has been registered and supported.</param>
    /// <exception cref="InvalidOperationException">The option record contains an invalid value.</exception>
    public void Validate(bool supportsCustomPolicy)
    {
        OccasionallyConnectedOptionsValidation.ValidateStreamId(StreamId, nameof(StreamId));
        ValidateSubscriptionId();
        ValidateStartPosition();
        OccasionallyConnectedOptionsValidation.ValidateDeliveryGuarantee(DeliveryGuarantee);
        OccasionallyConnectedOptionsValidation.ValidateBufferStrategy(BufferStrategy);
        OccasionallyConnectedOptionsValidation.ValidateCapacities(BufferCapacity, BufferCapacityBytes);
        OccasionallyConnectedOptionsValidation.ValidateCustomPolicy(BufferStrategy == BufferStrategy.Custom, supportsCustomPolicy);
    }

    /// <summary>Validates the optional durable subscription identity.</summary>
    /// <exception cref="InvalidOperationException"><see cref="SubscriptionId"/> contains an empty value.</exception>
    private void ValidateSubscriptionId()
    {
        if (SubscriptionId is not { Value: { } value } || value != Guid.Empty)
        {
            return;
        }

        throw new InvalidOperationException("SubscriptionId must be non-empty when supplied.");
    }

    /// <summary>Validates the initial start position reference.</summary>
    /// <exception cref="InvalidOperationException"><see cref="StartPosition"/> is missing.</exception>
    private void ValidateStartPosition()
    {
        if (StartPosition is not null)
        {
            return;
        }

        throw new InvalidOperationException("StartPosition must be provided.");
    }
}
