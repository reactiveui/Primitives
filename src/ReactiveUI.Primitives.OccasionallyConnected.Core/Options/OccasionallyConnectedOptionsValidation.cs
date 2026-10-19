// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Contains validation helpers for occasionally connected option records.</summary>
internal static class OccasionallyConnectedOptionsValidation
{
    /// <summary>The minimum accepted scheduling priority.</summary>
    internal const int MinimumPriority = -10;

    /// <summary>The maximum accepted scheduling priority.</summary>
    internal const int MaximumPriority = 10;

    /// <summary>The number of bytes in one mebibyte.</summary>
    internal const long BytesPerMebibyte = 1024 * 1024;

    /// <summary>Validates that a stream identifier was explicitly configured.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="optionName">The option property name.</param>
    /// <exception cref="InvalidOperationException"><paramref name="streamId"/> is the default value.</exception>
    internal static void ValidateStreamId(StreamId streamId, string optionName)
    {
        if (!string.IsNullOrEmpty(streamId.Value))
        {
            return;
        }

        throw new InvalidOperationException($"{optionName} must be a non-default stream identifier.");
    }

    /// <summary>Validates a stable wire contract identifier without normalizing it.</summary>
    /// <param name="contractId">The wire contract identifier.</param>
    /// <param name="optionName">The option property name.</param>
    /// <exception cref="InvalidOperationException"><paramref name="contractId"/> is null, empty, or whitespace.</exception>
    internal static void ValidateContractId(string contractId, string optionName)
    {
        if (!string.IsNullOrWhiteSpace(contractId))
        {
            return;
        }

        throw new InvalidOperationException($"{optionName} must be non-empty and non-whitespace.");
    }

    /// <summary>Validates that a version is positive.</summary>
    /// <param name="version">The configured version.</param>
    /// <param name="optionName">The option property name.</param>
    /// <exception cref="InvalidOperationException"><paramref name="version"/> is not positive.</exception>
    internal static void ValidatePositiveVersion(int version, string optionName)
    {
        if (version > 0)
        {
            return;
        }

        throw new InvalidOperationException($"{optionName} must be positive.");
    }

    /// <summary>Validates a bounded queue capacity pair.</summary>
    /// <param name="bufferCapacity">The maximum item count.</param>
    /// <param name="bufferCapacityBytes">The maximum byte count.</param>
    /// <exception cref="InvalidOperationException">Either capacity is not positive.</exception>
    internal static void ValidateCapacities(int bufferCapacity, long bufferCapacityBytes)
    {
        if (bufferCapacity > 0)
        {
            ValidateBufferCapacityBytes(bufferCapacityBytes);
            return;
        }

        throw new InvalidOperationException("BufferCapacity must be positive.");
    }

    /// <summary>Validates a bounded queue byte capacity.</summary>
    /// <param name="bufferCapacityBytes">The maximum byte count.</param>
    /// <exception cref="InvalidOperationException"><paramref name="bufferCapacityBytes"/> is not positive.</exception>
    internal static void ValidateBufferCapacityBytes(long bufferCapacityBytes)
    {
        if (bufferCapacityBytes > 0)
        {
            return;
        }

        throw new InvalidOperationException("BufferCapacityBytes must be positive.");
    }

    /// <summary>Validates a delivery guarantee enum value.</summary>
    /// <param name="deliveryGuarantee">The delivery guarantee.</param>
    /// <exception cref="InvalidOperationException"><paramref name="deliveryGuarantee"/> is undefined.</exception>
    internal static void ValidateDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee)
    {
        if (deliveryGuarantee is DeliveryGuarantee.AtMostOnce or DeliveryGuarantee.AtLeastOnce or DeliveryGuarantee.ExactlyOnce)
        {
            return;
        }

        throw new InvalidOperationException("DeliveryGuarantee must be a defined value.");
    }

    /// <summary>Validates a buffer strategy enum value.</summary>
    /// <param name="bufferStrategy">The buffer strategy.</param>
    /// <exception cref="InvalidOperationException"><paramref name="bufferStrategy"/> is undefined.</exception>
    internal static void ValidateBufferStrategy(BufferStrategy bufferStrategy)
    {
        if (bufferStrategy is BufferStrategy.DropOldest or BufferStrategy.DropNewest or BufferStrategy.Block or BufferStrategy.Reject or BufferStrategy.Custom)
        {
            return;
        }

        throw new InvalidOperationException("BufferStrategy must be a defined value.");
    }

    /// <summary>Validates a custom policy capability requirement.</summary>
    /// <param name="usesCustomPolicy">Whether the options select a custom policy.</param>
    /// <param name="supportsCustomPolicy">Whether the caller has registered and supported custom policy support.</param>
    /// <exception cref="InvalidOperationException">A custom policy was selected without capability support.</exception>
    internal static void ValidateCustomPolicy(bool usesCustomPolicy, bool supportsCustomPolicy)
    {
        if (!usesCustomPolicy || supportsCustomPolicy)
        {
            return;
        }

        throw new InvalidOperationException("Custom policies require explicit capability support.");
    }

    /// <summary>Validates configured priority bounds.</summary>
    /// <param name="minimumPriority">The inclusive minimum accepted priority.</param>
    /// <param name="maximumPriority">The inclusive maximum accepted priority.</param>
    /// <exception cref="InvalidOperationException"><paramref name="minimumPriority"/> is greater than <paramref name="maximumPriority"/>.</exception>
    internal static void ValidatePriorityRange(int minimumPriority, int maximumPriority)
    {
        if (minimumPriority <= maximumPriority)
        {
            return;
        }

        throw new InvalidOperationException("The minimum priority cannot be greater than the maximum priority.");
    }
}
