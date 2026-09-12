// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Represents configuration options for creating a replay-latest Signal, controlling publishing behavior and state retention.</summary>
[System.Diagnostics.DebuggerDisplay("ReplayLatestSignalCreationOptions: PublishingOption = {PublishingOption}, IsStateless = {IsStateless}")]
public sealed record ReplayLatestSignalCreationOptions
{
    /// <summary>Gets the default options: serial publishing, with completion state retained.</summary>
    public static ReplayLatestSignalCreationOptions Default { get; } = new() { PublishingOption = PublishingOption.Serial, IsStateless = false };

    /// <summary>Gets the order notifications are delivered to observers in.</summary>
    public required PublishingOption PublishingOption { get; init; }

    /// <summary>Gets a value indicating whether the signal discards its completion state, resetting to the initial value once the last observer leaves.</summary>
    public required bool IsStateless { get; init; }
}
