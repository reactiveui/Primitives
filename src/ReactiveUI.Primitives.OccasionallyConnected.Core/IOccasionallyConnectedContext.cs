// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Coordinates synchronization and typed local-first stream lifetimes for one client context.</summary>
/// <remarks>
/// Repeated calls to <see cref="GetOrCreateStream{TState, TInput}(StreamDefinition{TState, TInput})"/> with the
/// same stream identifier and a compatible definition return the same stream instance. Implementations must reject an
/// incompatible definition before beginning network work.
/// </remarks>
public interface IOccasionallyConnectedContext : IAsyncDisposable
{
    /// <summary>Gets the synchronization engine owned by this context.</summary>
    ISyncEngine SyncEngine { get; }

    /// <summary>Gets synchronization lifecycle state changes for this context.</summary>
    IObservable<SyncState> SyncStates { get; }

    /// <summary>Gets or creates the typed stream represented by a definition.</summary>
    /// <typeparam name="TState">The local projection state type.</typeparam>
    /// <typeparam name="TInput">The local and remote input type.</typeparam>
    /// <param name="definition">The typed stream definition.</param>
    /// <returns>The existing compatible stream or a newly created stream.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="definition"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">A stream with the same identifier has an incompatible definition.</exception>
    IOccasionallyConnectedStream<TState, TInput> GetOrCreateStream<TState, TInput>(
        StreamDefinition<TState, TInput> definition);

    /// <summary>Starts synchronization and stream lifecycle work.</summary>
    /// <param name="cancellationToken">The token used to cancel startup.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask StartAsync(CancellationToken cancellationToken);

    /// <summary>Stops synchronization and stream lifecycle work.</summary>
    /// <param name="cancellationToken">The token used to cancel shutdown waiting.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask StopAsync(CancellationToken cancellationToken);
}
