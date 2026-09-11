// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures the synchronous observer input bridge admission queue.</summary>
[DebuggerDisplay("{BufferStrategy,nq}; Count={BufferCapacity,nq}; Bytes={BufferCapacityBytes,nq}")]
public sealed record ObserverInputOptions
{
    /// <summary>Defines the default observer bridge item capacity.</summary>
    private const int DefaultBufferCapacity = 256;

    /// <summary>Defines the default observer bridge byte capacity.</summary>
    private const long DefaultBufferCapacityBytes = 4 * OccasionallyConnectedOptionsValidation.BytesPerMebibyte;

    /// <summary>Gets the admission strategy used by the observer bridge queue.</summary>
    public BufferStrategy BufferStrategy { get; init; } = BufferStrategy.Reject;

    /// <summary>Gets the maximum number of items admitted to the observer bridge queue.</summary>
    public int BufferCapacity { get; init; } = DefaultBufferCapacity;

    /// <summary>Gets the maximum estimated number of bytes admitted to the observer bridge queue.</summary>
    public long BufferCapacityBytes { get; init; } = DefaultBufferCapacityBytes;

    /// <summary>Validates this option record using structural rules only.</summary>
    /// <exception cref="InvalidOperationException">The option record contains an invalid value.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Validate() => Validate(supportsCustomPolicy: false);

    /// <summary>Validates this option record.</summary>
    /// <param name="supportsCustomPolicy">Whether a custom admission policy has been registered and supported.</param>
    /// <exception cref="InvalidOperationException">The option record contains an invalid value.</exception>
    public void Validate(bool supportsCustomPolicy)
    {
        OccasionallyConnectedOptionsValidation.ValidateBufferStrategy(BufferStrategy);
        OccasionallyConnectedOptionsValidation.ValidateCapacities(BufferCapacity, BufferCapacityBytes);
        OccasionallyConnectedOptionsValidation.ValidateCustomPolicy(BufferStrategy == BufferStrategy.Custom, supportsCustomPolicy);

        if (BufferStrategy != BufferStrategy.Block)
        {
            return;
        }

        throw new InvalidOperationException("Observer input bridges cannot use Block because OnNext cannot perform asynchronous backpressure.");
    }
}
