// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Subjects;

/// <summary>
/// Represents the set of options used when creating a subject, including publishing behavior and state management
/// requirements.
/// </summary>
/// <remarks>Use this type to configure how a subject is created, specifying whether it should be stateless and
/// which publishing option to apply. The options provided affect the subject's behavior and lifecycle. This record is
/// immutable and can be used to ensure consistent subject creation across different parts of an application.</remarks>
public sealed record SubjectCreationOptions
{
    /// <summary>
    /// Gets the publishing option to use when processing the item.
    /// </summary>
    public required PublishingOption PublishingOption { get; init; }

    /// <summary>
    /// Gets a value indicating whether the object is stateless.
    /// </summary>
    public required bool IsStateless { get; init; }

    /// <summary>
    /// Gets the default configuration for subject creation options.
    /// </summary>
    /// <remarks>The default options use serial publishing and are not stateless. Use this property as a
    /// baseline when configuring new subjects if standard behavior is desired.</remarks>
    public static SubjectCreationOptions Default { get; } = new()
    {
        PublishingOption = PublishingOption.Serial, IsStateless = false
    };
}
