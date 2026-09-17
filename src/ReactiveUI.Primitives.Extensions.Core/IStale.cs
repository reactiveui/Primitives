// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions;

/// <summary>Carries either a staleness signal or a value update from an observable sequence.</summary>
/// <typeparam name="T">The type of the update value.</typeparam>
public interface IStale<out T>
{
    /// <summary>Gets a value indicating whether this notification signals staleness rather than an update.</summary>
    bool IsStale { get; }

    /// <summary>Gets the update value; an implementation may reject the read while <see cref="IsStale"/> is <see langword="true"/>.</summary>
    T? Update { get; }
}
