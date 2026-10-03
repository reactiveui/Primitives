// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes an inclusive protocol version range.</summary>
/// <param name="Minimum">The minimum supported version.</param>
/// <param name="Maximum">The maximum supported version.</param>
[System.Diagnostics.DebuggerDisplay("{Minimum,nq}..{Maximum,nq}")]
public sealed record VersionRange(Version Minimum, Version Maximum);
