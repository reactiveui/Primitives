// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions;

/// <summary>
/// Represents either a heartbeat signal or a value update from an observable stream. Value-type shape; the
/// heartbeat operator emits these directly so per-emission allocations are zero. Note that
/// <c>default(Heartbeat&lt;T&gt;)</c> represents a value update with the default <typeparamref name="T"/>; use
/// <c>new Heartbeat&lt;T&gt;()</c> to construct a heartbeat tick.
/// </summary>
/// <typeparam name="T">The type of the update value.</typeparam>
public readonly record struct Heartbeat<T> : IHeartbeat<T>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="Heartbeat{T}"/> struct representing a heartbeat tick.
    /// </summary>
    public Heartbeat()
    {
        IsHeartbeat = true;
        Update = default;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Heartbeat{T}"/> struct representing a value update.
    /// </summary>
    /// <param name="update">The update value.</param>
    public Heartbeat(T? update)
    {
        IsHeartbeat = false;
        Update = update;
    }

    /// <inheritdoc/>
    public bool IsHeartbeat { get; }

    /// <inheritdoc/>
    public T? Update { get; }
}
