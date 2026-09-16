// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Reads the current value for a <see cref="CurrentValueDelivery{T}"/>.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>Implement it on a struct wrapping the owner so the read needs no delegate.</remarks>
public interface ICurrentValueReader<out T>
{
    /// <summary>Reads the current value.</summary>
    /// <returns>The current value.</returns>
    T Read();
}
