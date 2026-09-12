// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides configuration options for controlling the behavior of the TakeUntil operator.</summary>
[System.Diagnostics.DebuggerDisplay("TakeUntilOptions: SourceFailsWhenOtherFails = {SourceFailsWhenOtherFails}")]
public sealed record TakeUntilOptions
{
    /// <summary>Gets the default configuration options for the TakeUntil operation.</summary>
    public static TakeUntilOptions Default { get; } = new();

    /// <summary>Gets a value indicating whether a failure of the other sequence also fails the result sequence.</summary>
    public bool SourceFailsWhenOtherFails { get; init; }
}
