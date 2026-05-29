// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Subjects;

/// <summary>
/// Represents configuration options for creating a replay-latest subject, controlling publishing behavior and state
/// retention.
/// </summary>
/// <remarks>Use this type to specify how a replay-latest subject should handle message publishing and whether it
/// maintains state. The options provided affect how subscribers receive messages and whether the subject retains the
/// latest value. This type is immutable and can be used to configure subject creation in a thread-safe
/// manner.</remarks>
public sealed record ReplayLatestSubjectCreationOptions
{
    /// <summary>
    /// Gets the publishing option to use when processing this item.
    /// </summary>
    public required PublishingOption PublishingOption { get; init; }

    /// <summary>
    /// Gets a value indicating whether the object is stateless.
    /// </summary>
    public required bool IsStateless { get; init; }

    /// <summary>
    /// Gets the default configuration options for creating a ReplayLatestSubject instance.
    /// </summary>
    /// <remarks>The default options specify serial publishing and stateful behavior. Use this property to
    /// obtain a baseline configuration that can be modified as needed.</remarks>
    public static ReplayLatestSubjectCreationOptions Default { get; } = new()
    {
        PublishingOption = PublishingOption.Serial, IsStateless = false
    };
}
