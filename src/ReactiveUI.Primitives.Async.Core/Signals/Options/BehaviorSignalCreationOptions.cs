// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Represents configuration options for creating a behavior Signal, including publishing behavior and statefulness.</summary>
[System.Diagnostics.DebuggerDisplay("BehaviorSignalCreationOptions: PublishingOption = {PublishingOption}, IsStateless = {IsStateless}")]
public sealed record BehaviorSignalCreationOptions
{
    /// <summary>Gets the default configuration options for creating a new BehaviorSignal instance.</summary>
    /// <remarks>Use this property to obtain a standard set of options when creating a BehaviorSignal, unless
    /// custom behavior is required.</remarks>
    public static BehaviorSignalCreationOptions Default { get; } = new() { PublishingOption = PublishingOption.Serial, IsStateless = false };

    /// <summary>Gets the publishing option to use when processing the item.</summary>
    public required PublishingOption PublishingOption { get; init; }

    /// <summary>Gets a value indicating whether the object is stateless.</summary>
    public required bool IsStateless { get; init; }
}
