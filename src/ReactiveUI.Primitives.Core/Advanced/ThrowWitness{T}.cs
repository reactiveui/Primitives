// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Observer that ignores values and completion and rethrows errors.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
public sealed class ThrowWitness<T> : IObserver<T>
{
    /// <summary>Gets the shared throw witness instance.</summary>
    public static readonly ThrowWitness<T> Instance = new();

    /// <summary>Initializes a new instance of the <see cref="ThrowWitness{T}"/> class.</summary>
    private ThrowWitness()
    {
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => error.Rethrow();

    /// <inheritdoc/>
    public void OnNext(T value)
    {
    }
}
