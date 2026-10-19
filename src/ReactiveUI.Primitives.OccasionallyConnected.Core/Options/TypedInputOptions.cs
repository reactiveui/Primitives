// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures the retained typed-input admission contract for a public stream facade.</summary>
/// <remarks>
/// The retained-input byte declaration is supplied by the caller before serialization. It is not the serialized payload limit.
/// </remarks>
[DebuggerDisplay("Count={BufferCapacity,nq}; Bytes={BufferCapacityBytes,nq}; RetainedInput={MaximumRetainedInputBytes,nq}")]
public sealed record TypedInputOptions
{
    /// <summary>Defines the default typed input queue item capacity.</summary>
    private const int DefaultBufferCapacity = 64;

    /// <summary>Defines the default typed input queue retained byte capacity.</summary>
    private const long DefaultBufferCapacityBytes = 4 * OccasionallyConnectedOptionsValidation.BytesPerMebibyte;

    /// <summary>Gets the maximum number of typed input work items retained by the stream facade.</summary>
    public int BufferCapacity { get; init; } = DefaultBufferCapacity;

    /// <summary>Gets the maximum retained byte count for typed input work retained by the stream facade.</summary>
    public long BufferCapacityBytes { get; init; } = DefaultBufferCapacityBytes;

    /// <summary>Gets the caller-declared retained byte charge for one typed input before serialization is available.</summary>
    public required long MaximumRetainedInputBytes { get; init; }

    /// <summary>Validates this option record using structural rules only.</summary>
    /// <exception cref="InvalidOperationException">The option record contains an invalid value.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Validate()
    {
        OccasionallyConnectedOptionsValidation.ValidateCapacities(BufferCapacity, BufferCapacityBytes);
        if (MaximumRetainedInputBytes <= 0)
        {
            throw new InvalidOperationException("MaximumRetainedInputBytes must be positive.");
        }

        if (MaximumRetainedInputBytes <= BufferCapacityBytes)
        {
            return;
        }

        throw new InvalidOperationException("MaximumRetainedInputBytes cannot exceed BufferCapacityBytes.");
    }
}
