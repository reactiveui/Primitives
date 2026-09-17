// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Represents the set of options used when creating a Signal, including publishing behavior and state management requirements.</summary>
[System.Diagnostics.DebuggerDisplay("SignalCreationOptions: PublishingOption = {PublishingOption}, IsStateless = {IsStateless}")]
public sealed record SignalCreationOptions
{
    /// <summary>Gets the default options: serial publishing, with completion state retained.</summary>
    public static SignalCreationOptions Default { get; } = new() { PublishingOption = PublishingOption.Serial, IsStateless = false };

    /// <summary>Gets the order notifications are delivered to observers in.</summary>
    public required PublishingOption PublishingOption { get; init; }

    /// <summary>Gets a value indicating whether the signal discards its completion state, so values can keep flowing after a terminal notification.</summary>
    public required bool IsStateless { get; init; }
}
