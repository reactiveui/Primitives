// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions;

/// <summary>Carries either a heartbeat tick or a value update from an observable sequence.</summary>
/// <typeparam name="T">The type of the update value.</typeparam>
public interface IHeartbeat<out T>
{
    /// <summary>Gets a value indicating whether this notification is a heartbeat tick rather than an update.</summary>
    bool IsHeartbeat { get; }

    /// <summary>Gets the update value, which carries no meaning while <see cref="IsHeartbeat"/> is <see langword="true"/>.</summary>
    T? Update { get; }
}
