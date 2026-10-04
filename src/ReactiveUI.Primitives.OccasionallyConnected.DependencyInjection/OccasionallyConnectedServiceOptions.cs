// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection;

/// <summary>Configures dependency-injection integration for one occasionally connected context.</summary>
[DebuggerDisplay("MaximumNamedStreams = {MaximumNamedStreams}, MaximumStreamNameLength = {MaximumStreamNameLength}")]
public sealed class OccasionallyConnectedServiceOptions
{
    /// <summary>Gets or sets the runtime context options captured when the singleton context is created.</summary>
    public OccasionallyConnectedOptions Options { get; set; } = OccasionallyConnectedOptions.Default;

    /// <summary>Gets or sets the maximum number of named streams that can be registered.</summary>
    public int MaximumNamedStreams { get; set; } = 128;

    /// <summary>Gets or sets the maximum length of a registered stream name.</summary>
    public int MaximumStreamNameLength { get; set; } = 128;
}
