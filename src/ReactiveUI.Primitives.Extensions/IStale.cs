// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions;

/// <summary>
/// Indicator for connection that has become stale.
/// </summary>
/// <typeparam name="T">The type.</typeparam>
public interface IStale<out T>
{
    /// <summary>
    /// Gets a value indicating whether this instance is stale.
    /// </summary>
    /// <value><c>true</c> if this instance is stale; otherwise, <c>false</c>.</value>
    bool IsStale { get; }

    /// <summary>
    /// Gets the update.
    /// </summary>
    /// <value>The update.</value>
    T? Update { get; }
}
