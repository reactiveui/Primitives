// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Crdt;

/// <summary>Adapts built-in CRDT helpers to the local stream committer projection contract.</summary>
[System.Diagnostics.DebuggerDisplay("{AuthenticatedClientId,nq} {InitialState.Kind,nq}")]
public sealed class CrdtLocalProjection : ILocalProjection<CrdtState, CrdtInput>
{
    /// <summary>The CRDT bounds.</summary>
    private readonly CrdtBounds _bounds;

    /// <summary>Initializes a new instance of the <see cref="CrdtLocalProjection"/> class.</summary>
    /// <param name="authenticatedClientId">The authenticated client id.</param>
    /// <param name="initialState">The initial state.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <exception cref="ArgumentNullException"><paramref name="initialState"/> or <paramref name="bounds"/> is <see langword="null"/>.</exception>
    public CrdtLocalProjection(string authenticatedClientId, CrdtState initialState, CrdtBounds bounds)
    {
        ArgumentExceptionHelper.ThrowIfNull(authenticatedClientId);
        ArgumentExceptionHelper.ThrowIfNull(initialState);
        ArgumentExceptionHelper.ThrowIfNull(bounds);
        bounds.Validate();
        AuthenticatedClientId = authenticatedClientId;
        _bounds = bounds;
        InitialState = CrdtFunctions.ReplaceAuthoritativeState(initialState, bounds);
    }

    /// <summary>Initializes a new instance of the <see cref="CrdtLocalProjection"/> class.</summary>
    /// <param name="authenticatedClientId">The authenticated client id.</param>
    /// <param name="kind">The CRDT kind.</param>
    public CrdtLocalProjection(string authenticatedClientId, CrdtKind kind)
        : this(authenticatedClientId, CrdtFunctions.Empty(kind), CrdtBounds.Default)
    {
    }

    /// <summary>Gets the authenticated client id used to derive local OR-set dots.</summary>
    public string AuthenticatedClientId { get; }

    /// <inheritdoc/>
    public CrdtState InitialState { get; }

    /// <inheritdoc/>
    public CrdtState ApplyLocal(CrdtState state, CrdtInput input, SyncOperation operation)
    {
        ArgumentExceptionHelper.ThrowIfNull(operation);
        return CrdtFunctions.ApplyLocal(state, input, AuthenticatedClientId, operation.ClientSequence, _bounds);
    }

    /// <inheritdoc/>
    public CrdtState ApplyRemote(CrdtState state, CrdtInput input, RemoteEvent remoteEvent)
    {
        ArgumentExceptionHelper.ThrowIfNull(input);
        ArgumentExceptionHelper.ThrowIfNull(remoteEvent);
        if (input.Kind == CrdtInputKind.AuthoritativeState && input.State is not null)
        {
            if (input.State.Kind != InitialState.Kind)
            {
                throw new InvalidOperationException("A CRDT remote snapshot cannot change the stream family.");
            }

            return CrdtFunctions.ReplaceAuthoritativeState(input.State, _bounds);
        }

        throw new InvalidOperationException("A CRDT remote event must carry a complete authoritative state.");
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public CrdtState Reconcile(CrdtState state, ConflictResolutionResult result) => state;
}
