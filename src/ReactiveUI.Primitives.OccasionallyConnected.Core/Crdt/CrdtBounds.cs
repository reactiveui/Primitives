// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Crdt;

/// <summary>Defines finite CRDT collection, element, and payload limits.</summary>
[System.Diagnostics.DebuggerDisplay("Components = {MaximumCounterComponents}, EncodedBytes = {MaximumEncodedBytes}")]
public sealed record CrdtBounds
{
    /// <summary>The absolute maximum counter component count copied from caller-owned data.</summary>
    internal const int MaximumOwnedCounterComponents = 4096;

    /// <summary>The absolute maximum retained dot count copied from caller-owned data.</summary>
    internal const int MaximumOwnedRetainedDots = 16_384;

    /// <summary>The absolute maximum active element count copied from caller-owned data.</summary>
    internal const int MaximumOwnedElements = 4096;

    /// <summary>The absolute maximum element bytes copied from caller-owned data.</summary>
    internal const int MaximumOwnedElementBytes = 16 * Kibibyte;

    /// <summary>The absolute maximum register bytes copied from caller-owned data.</summary>
    internal const int MaximumOwnedRegisterBytes = Kibibyte * Kibibyte;

    /// <summary>The number of bytes in one kibibyte.</summary>
    private const int Kibibyte = 1024;

    /// <summary>Gets default production-oriented CRDT bounds.</summary>
    public static CrdtBounds Default { get; } = new();

    /// <summary>Gets the maximum number of counter components. The value may be reduced but cannot exceed the ownership ceiling.</summary>
    public int MaximumCounterComponents { get; init; } = MaximumOwnedCounterComponents;

    /// <summary>Gets the maximum number of OR-set dot bindings. The value may be reduced but cannot exceed the ownership ceiling.</summary>
    public int MaximumDotBindings { get; init; } = MaximumOwnedRetainedDots;

    /// <summary>Gets the maximum number of OR-set tombstones. The value may be reduced but cannot exceed the ownership ceiling.</summary>
    public int MaximumTombstones { get; init; } = MaximumOwnedRetainedDots;

    /// <summary>Gets the maximum number of active OR-set elements. The value may be reduced but cannot exceed the ownership ceiling.</summary>
    public int MaximumElements { get; init; } = MaximumOwnedElements;

    /// <summary>Gets the maximum bytes accepted for one element value. The value may be reduced but cannot exceed the ownership ceiling.</summary>
    public int MaximumElementBytes { get; init; } = MaximumOwnedElementBytes;

    /// <summary>Gets the maximum bytes accepted for one register value. The value may be reduced but cannot exceed the ownership ceiling.</summary>
    public int MaximumRegisterBytes { get; init; } = MaximumOwnedRegisterBytes;

    /// <summary>Gets the maximum encoded payload bytes.</summary>
    public int MaximumEncodedBytes { get; init; } = Kibibyte * Kibibyte;

    /// <summary>Gets the maximum UTF-8 bytes accepted for an authenticated client identifier.</summary>
    public int MaximumClientIdUtf8Bytes { get; init; } = 1024;

    /// <summary>Validates the configured limits.</summary>
    /// <exception cref="ArgumentOutOfRangeException">A configured limit is less than one or an ownership limit exceeds its absolute ceiling.</exception>
    public void Validate()
    {
        ValidateOwnershipLimit(MaximumCounterComponents, MaximumOwnedCounterComponents, nameof(MaximumCounterComponents));
        ValidateOwnershipLimit(MaximumDotBindings, MaximumOwnedRetainedDots, nameof(MaximumDotBindings));
        ValidateOwnershipLimit(MaximumTombstones, MaximumOwnedRetainedDots, nameof(MaximumTombstones));
        ValidateOwnershipLimit(MaximumElements, MaximumOwnedElements, nameof(MaximumElements));
        ValidateOwnershipLimit(MaximumElementBytes, MaximumOwnedElementBytes, nameof(MaximumElementBytes));
        ValidateOwnershipLimit(MaximumRegisterBytes, MaximumOwnedRegisterBytes, nameof(MaximumRegisterBytes));
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumEncodedBytes);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(MaximumClientIdUtf8Bytes);
    }

    /// <summary>Validates one configurable ownership limit.</summary>
    /// <param name="value">The configured value.</param>
    /// <param name="maximum">The immutable ownership ceiling.</param>
    /// <param name="parameterName">The configured property name.</param>
    /// <exception cref="ArgumentOutOfRangeException">The value is not positive or exceeds the immutable ownership ceiling.</exception>
    private static void ValidateOwnershipLimit(int value, int maximum, string parameterName)
    {
        if (value > 0 && value <= maximum)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(parameterName, value, "The CRDT ownership limit must be positive and cannot exceed its absolute ceiling.");
    }
}
