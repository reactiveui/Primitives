// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions;

/// <summary>
/// Represents either a staleness indicator or a value update from an observable stream. Value-type shape;
/// the stale-detection operator emits these directly so per-emission allocations are zero. Note that
/// <c>default(Stale&lt;T&gt;)</c> represents a value update with the default <typeparamref name="T"/>; use
/// <c>new Stale&lt;T&gt;()</c> to construct a staleness signal.
/// </summary>
/// <typeparam name="T">The type of the update value.</typeparam>
public readonly record struct Stale<T> : IStale<T>
{
    /// <summary>Initializes a new instance of the <see cref="Stale{T}"/> struct representing a stale signal.</summary>
    public Stale()
    {
        IsStale = true;
        Update = default;
    }

    /// <summary>Initializes a new instance of the <see cref="Stale{T}"/> struct representing a value update.</summary>
    /// <param name="update">The update value.</param>
    public Stale(T? update)
    {
        IsStale = false;
        Update = update;
    }

    /// <inheritdoc/>
    public bool IsStale { get; }

    /// <inheritdoc/>
    public T? Update => IsStale ? throw new InvalidOperationException("Stale instance has no update.") : field;
}
