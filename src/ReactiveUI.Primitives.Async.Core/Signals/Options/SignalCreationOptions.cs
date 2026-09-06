// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>
/// Represents the set of options used when creating a Signal, including publishing behavior and state management
/// requirements.
/// </summary>
/// <remarks>Use this type to configure how a Signal is created, specifying whether it should be stateless and
/// which publishing option to apply. The options provided affect the Signal's behavior and lifecycle. This record is
/// immutable and can be used to ensure consistent Signal creation across different parts of an application.</remarks>
[System.Diagnostics.DebuggerDisplay("SignalCreationOptions: PublishingOption = {PublishingOption}, IsStateless = {IsStateless}")]
public sealed record SignalCreationOptions
{
    /// <summary>Gets the default configuration for Signal creation options.</summary>
    /// <remarks>The default options use serial publishing and are not stateless. Use this property as a
    /// baseline when configuring new Signals if standard behavior is desired.</remarks>
    public static SignalCreationOptions Default { get; } = new() { PublishingOption = PublishingOption.Serial, IsStateless = false };

    /// <summary>Gets the publishing option to use when processing the item.</summary>
    public required PublishingOption PublishingOption { get; init; }

    /// <summary>Gets a value indicating whether the object is stateless.</summary>
    public required bool IsStateless { get; init; }
}
