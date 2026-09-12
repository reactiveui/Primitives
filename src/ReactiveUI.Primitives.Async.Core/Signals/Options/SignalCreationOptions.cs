// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Represents the set of options used when creating a Signal, including publishing behavior and state management requirements.</summary>
[System.Diagnostics.DebuggerDisplay("SignalCreationOptions: PublishingOption = {PublishingOption}, IsStateless = {IsStateless}")]
public sealed record SignalCreationOptions
{
    /// <summary>Gets the default configuration for Signal creation options.</summary>
    /// <remarks>Defaults to serial, stateful publishing.</remarks>
    public static SignalCreationOptions Default { get; } = new() { PublishingOption = PublishingOption.Serial, IsStateless = false };

    /// <summary>Gets the publishing option to use when processing the item.</summary>
    public required PublishingOption PublishingOption { get; init; }

    /// <summary>Gets a value indicating whether the object is stateless.</summary>
    public required bool IsStateless { get; init; }
}
