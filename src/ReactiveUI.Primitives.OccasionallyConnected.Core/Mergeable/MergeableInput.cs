// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

/// <summary>Represents a local change or complete server state for a mergeable stream.</summary>
[System.Diagnostics.DebuggerDisplay("{Kind,nq}")]
public sealed record MergeableInput
{
    /// <summary>Initializes a new instance of the <see cref="MergeableInput"/> class.</summary>
    /// <param name="change">The local change case.</param>
    private MergeableInput(MergeableChange change)
    {
        ArgumentExceptionHelper.ThrowIfNull(change);
        Value = change;
    }

    /// <summary>Initializes a new instance of the <see cref="MergeableInput"/> class.</summary>
    /// <param name="state">The complete authoritative state case.</param>
    private MergeableInput(MergeableState state)
    {
        ArgumentExceptionHelper.ThrowIfNull(state);
        Value = state;
    }

    /// <summary>Gets the input kind derived from its single case.</summary>
    public MergeableInputKind Kind => Value is MergeableChange ? MergeableInputKind.Change : MergeableInputKind.AuthoritativeState;

    /// <summary>Gets the single case, either a <see cref="MergeableChange"/> or a <see cref="MergeableState"/>.</summary>
    public object Value { get; }

    /// <summary>Creates a local change input.</summary>
    /// <param name="change">The local change.</param>
    /// <returns>The mergeable input.</returns>
    public static MergeableInput ForChange(MergeableChange change) =>
        new(change);

    /// <summary>Creates a complete authoritative state input.</summary>
    /// <param name="state">The authoritative state.</param>
    /// <returns>The mergeable input.</returns>
    public static MergeableInput ForAuthoritativeState(MergeableState state) =>
        new(state);
}
