// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.OccasionallyConnected.Reactive.Mergeable;
#else
namespace ReactiveUI.Primitives.OccasionallyConnected.Mergeable;
#endif

/// <summary>Adapts built-in CRDT helpers to the local stream committer projection contract.</summary>
[System.Diagnostics.DebuggerDisplay("{AuthenticatedClientId,nq} {InitialState.Kind,nq}")]
public sealed class MergeableLocalProjection : ILocalProjection<MergeableState, MergeableInput>
{
    /// <summary>The CRDT bounds.</summary>
    private readonly MergeableBounds _bounds;

    /// <summary>Initializes a new instance of the <see cref="MergeableLocalProjection"/> class.</summary>
    /// <param name="authenticatedClientId">The authenticated client id.</param>
    /// <param name="initialState">The initial state.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <exception cref="ArgumentNullException"><paramref name="initialState"/> or <paramref name="bounds"/> is <see langword="null"/>.</exception>
    public MergeableLocalProjection(string authenticatedClientId, MergeableState initialState, MergeableBounds bounds)
    {
        ArgumentExceptionHelper.ThrowIfNull(authenticatedClientId);
        ArgumentExceptionHelper.ThrowIfNull(initialState);
        ArgumentExceptionHelper.ThrowIfNull(bounds);
        bounds.Validate();
        AuthenticatedClientId = authenticatedClientId;
        _bounds = bounds;
        InitialState = MergeableFunctions.ReplaceAuthoritativeState(initialState, bounds);
    }

    /// <summary>Initializes a new instance of the <see cref="MergeableLocalProjection"/> class.</summary>
    /// <param name="authenticatedClientId">The authenticated client id.</param>
    /// <param name="kind">The CRDT kind.</param>
    public MergeableLocalProjection(string authenticatedClientId, MergeableKind kind)
        : this(authenticatedClientId, MergeableFunctions.Empty(kind), MergeableBounds.Default)
    {
    }

    /// <summary>Gets the authenticated client id used to derive local OR-set dots.</summary>
    public string AuthenticatedClientId { get; }

    /// <inheritdoc/>
    public MergeableState InitialState { get; }

    /// <inheritdoc/>
    public MergeableState ApplyLocal(MergeableState state, MergeableInput input, SyncOperation operation)
    {
        ArgumentExceptionHelper.ThrowIfNull(operation);
        return MergeableFunctions.ApplyLocal(state, input, AuthenticatedClientId, operation.ClientSequence, _bounds);
    }

    /// <inheritdoc/>
    public MergeableState ApplyRemote(MergeableState state, MergeableInput input, RemoteEvent remoteEvent)
    {
        ArgumentExceptionHelper.ThrowIfNull(input);
        ArgumentExceptionHelper.ThrowIfNull(remoteEvent);
        if (input.Value is MergeableState authoritativeState)
        {
            if (authoritativeState.Kind != InitialState.Kind)
            {
                throw new InvalidOperationException("A CRDT remote snapshot cannot change the stream family.");
            }

            return MergeableFunctions.ReplaceAuthoritativeState(authoritativeState, _bounds);
        }

        throw new InvalidOperationException("A CRDT remote event must carry a complete authoritative state.");
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MergeableState Reconcile(MergeableState state, ConflictResolutionResult result) => state;
}
