// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides configuration options for controlling the behavior of the TakeUntil operator.
/// </summary>
/// <remarks>This type allows customization of how the source sequence responds to failures in the 'other'
/// sequence when using TakeUntil. It is immutable and thread-safe.</remarks>
public sealed record TakeUntilOptions
{
    /// <summary>
    /// Gets a value indicating whether the source operation fails when another related operation fails.
    /// </summary>
    public bool SourceFailsWhenOtherFails { get; init; }

    /// <summary>
    /// Gets the default configuration options for the TakeUntil operation.
    /// </summary>
    /// <remarks>Use this property to obtain a standard set of options when no customization is required. The
    /// returned instance is immutable and can be shared safely across multiple operations.</remarks>
    public static TakeUntilOptions Default { get; } = new();
}
