// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures the internal bounded admission queue.</summary>
/// <param name="Capacity">The maximum admitted item count.</param>
/// <param name="CapacityBytes">The maximum admitted byte count.</param>
/// <param name="MaximumBlockedProducers">The maximum number of producers allowed to wait.</param>
/// <param name="ReservedControlCapacity">The item count reserved for control traffic.</param>
/// <param name="ReservedControlBytes">The byte count reserved for control traffic.</param>
internal readonly record struct BoundedAdmissionQueueOptions(
    int Capacity,
    long CapacityBytes,
    int MaximumBlockedProducers,
    int ReservedControlCapacity = 0,
    long ReservedControlBytes = 0)
{
    /// <summary>Gets the count capacity available to non-control traffic.</summary>
    internal int DataCapacity => Capacity - ReservedControlCapacity;

    /// <summary>Gets the byte capacity available to non-control traffic.</summary>
    internal long DataCapacityBytes => CapacityBytes - ReservedControlBytes;

    /// <summary>Validates the queue options.</summary>
    /// <exception cref="InvalidOperationException">The option record contains invalid values.</exception>
    internal void Validate()
    {
        ValidateCapacity(Capacity, nameof(Capacity));
        ValidateCapacity(CapacityBytes, nameof(CapacityBytes));

        if (MaximumBlockedProducers <= 0)
        {
            throw new InvalidOperationException("MaximumBlockedProducers must be positive.");
        }

        ValidateControlReserve(ReservedControlCapacity, Capacity, nameof(ReservedControlCapacity));
        ValidateControlReserve(ReservedControlBytes, CapacityBytes, nameof(ReservedControlBytes));
    }

    /// <summary>Validates a control reserve value.</summary>
    /// <param name="reserved">The reserved amount.</param>
    /// <param name="capacity">The total capacity.</param>
    /// <param name="optionName">The option name.</param>
    /// <exception cref="InvalidOperationException">The reserve is outside the valid range.</exception>
    private static void ValidateControlReserve(long reserved, long capacity, string optionName)
    {
        if (reserved < 0)
        {
            throw new InvalidOperationException($"{optionName} cannot be negative.");
        }

        if (reserved < capacity)
        {
            return;
        }

        throw new InvalidOperationException($"{optionName} must leave capacity for data traffic.");
    }

    /// <summary>Validates a positive capacity value.</summary>
    /// <param name="capacity">The capacity value.</param>
    /// <param name="optionName">The option name.</param>
    /// <exception cref="InvalidOperationException">The capacity is not positive.</exception>
    private static void ValidateCapacity(long capacity, string optionName)
    {
        if (capacity > 0)
        {
            return;
        }

        throw new InvalidOperationException($"{optionName} must be positive.");
    }
}
