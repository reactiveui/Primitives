// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection;

/// <summary>Identifies a named occasionally connected stream with its state and input types.</summary>
/// <typeparam name="TState">The stream state type.</typeparam>
/// <typeparam name="TInput">The stream input type.</typeparam>
[DebuggerDisplay("{Name,nq}")]
public readonly struct OccasionallyConnectedStreamKey<TState, TInput> :
    IEquatable<OccasionallyConnectedStreamKey<TState, TInput>>
{
    /// <summary>Initializes a new instance of the <see cref="OccasionallyConnectedStreamKey{TState, TInput}"/> struct.</summary>
    /// <param name="name">The registered stream name.</param>
    public OccasionallyConnectedStreamKey(string name) => Name = name;

    /// <summary>Gets the registered stream name.</summary>
    public string Name { get; }

    /// <summary>Gets the stream state type.</summary>
    public Type StateType => typeof(TState);

    /// <summary>Gets the stream input type.</summary>
    public Type InputType => typeof(TInput);

    /// <summary>Compares two stream keys for equality.</summary>
    /// <param name="left">The left key.</param>
    /// <param name="right">The right key.</param>
    /// <returns>A value indicating whether the keys are equal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool operator ==(
        OccasionallyConnectedStreamKey<TState, TInput> left,
        OccasionallyConnectedStreamKey<TState, TInput> right) =>
        left.Equals(right);

    /// <summary>Compares two stream keys for inequality.</summary>
    /// <param name="left">The left key.</param>
    /// <param name="right">The right key.</param>
    /// <returns>A value indicating whether the keys are different.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool operator !=(
        OccasionallyConnectedStreamKey<TState, TInput> left,
        OccasionallyConnectedStreamKey<TState, TInput> right) =>
        !left.Equals(right);

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Equals(OccasionallyConnectedStreamKey<TState, TInput> other) =>
        string.Equals(Name, other.Name, StringComparison.Ordinal);

    /// <inheritdoc />
    public override bool Equals(object? obj) =>
        obj is OccasionallyConnectedStreamKey<TState, TInput> other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => Name is null ? 0 : StringComparer.Ordinal.GetHashCode(Name);
}
