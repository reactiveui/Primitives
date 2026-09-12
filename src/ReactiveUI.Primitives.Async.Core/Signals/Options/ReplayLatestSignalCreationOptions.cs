// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Represents configuration options for creating a replay-latest Signal, controlling publishing behavior and state retention.</summary>
[System.Diagnostics.DebuggerDisplay("ReplayLatestSignalCreationOptions: PublishingOption = {PublishingOption}, IsStateless = {IsStateless}")]
public sealed record ReplayLatestSignalCreationOptions
{
    /// <summary>Gets the default configuration options for creating a ReplayLatestSignal instance.</summary>
    /// <remarks>Defaults to serial, stateful publishing.</remarks>
    public static ReplayLatestSignalCreationOptions Default { get; } = new() { PublishingOption = PublishingOption.Serial, IsStateless = false };

    /// <summary>Gets the publishing option to use when processing this item.</summary>
    public required PublishingOption PublishingOption { get; init; }

    /// <summary>Gets a value indicating whether the object is stateless.</summary>
    public required bool IsStateless { get; init; }
}
