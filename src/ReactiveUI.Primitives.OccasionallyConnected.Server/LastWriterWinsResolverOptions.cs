// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Configures the built-in last-writer-wins resolver.</summary>
[System.Diagnostics.DebuggerDisplay("{VersionFactory,nq}")]
public sealed record LastWriterWinsResolverOptions
{
    /// <summary>Gets the version factory used for accepted writes.</summary>
    public required IServerConflictVersionFactory VersionFactory { get; init; }

    /// <summary>Validates the resolver options.</summary>
    /// <exception cref="ArgumentNullException">The version factory is missing.</exception>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public void Validate() => ArgumentExceptionHelper.ThrowIfNull(VersionFactory);
}
