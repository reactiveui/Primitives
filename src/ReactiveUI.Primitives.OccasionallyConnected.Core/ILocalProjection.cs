// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Computes typed local state from validated inputs and conflict decisions.</summary>
/// <typeparam name="TState">The application state type.</typeparam>
/// <typeparam name="TInput">The application mutation type.</typeparam>
/// <remarks>
/// Implementations are deterministic synchronous reducers. They must not mutate the supplied state or input, perform
/// network I/O, or publish external side effects. A new state becomes observable only after its store transaction commits.
/// </remarks>
public interface ILocalProjection<TState, in TInput>
{
    /// <summary>Gets the initial state before any committed local or remote mutations.</summary>
    TState InitialState { get; }

    /// <summary>Computes the optimistic state for a local mutation.</summary>
    /// <param name="state">The previously committed state.</param>
    /// <param name="input">The validated typed mutation.</param>
    /// <param name="operation">The immutable outbox operation that will commit with the new state.</param>
    /// <returns>The new optimistic state.</returns>
    TState ApplyLocal(TState state, TInput input, SyncOperation operation);

    /// <summary>Computes state for a remote event after decoding, upcasting, and inbox filtering.</summary>
    /// <param name="state">The previously committed state.</param>
    /// <param name="input">The validated and upcast typed event payload.</param>
    /// <param name="remoteEvent">The immutable event metadata and encoded envelope.</param>
    /// <returns>The new state including the remote event.</returns>
    TState ApplyRemote(TState state, TInput input, RemoteEvent remoteEvent);

    /// <summary>Computes state after applying a canonical conflict decision.</summary>
    /// <param name="state">The state to reconcile.</param>
    /// <param name="result">The canonical conflict decision.</param>
    /// <returns>The reconciled state.</returns>
    TState Reconcile(TState state, ConflictResolutionResult result);
}
