// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Filters paired committed-state snapshots to synchronized states.</summary>
/// <typeparam name="TState">The state type.</typeparam>
/// <param name="source">The paired snapshot source.</param>
internal sealed class SynchronizedStateObservable<TState>(
    IObservable<OccasionallyConnectedCommittedStateQueueSnapshot<TState>> source) : IObservable<TState>
{
    /// <inheritdoc />
    public IDisposable Subscribe(IObserver<TState> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new SnapshotObserver(observer));
    }

    /// <summary>Forwards only snapshots with no pending work.</summary>
    /// <param name="observer">The downstream observer.</param>
    private sealed class SnapshotObserver(IObserver<TState> observer) : IObserver<OccasionallyConnectedCommittedStateQueueSnapshot<TState>>
    {
        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => observer.OnCompleted();

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => observer.OnError(error);

        /// <inheritdoc />
        public void OnNext(OccasionallyConnectedCommittedStateQueueSnapshot<TState> value)
        {
            if (value.Pending.OperationCount == 0)
            {
                observer.OnNext(value.State);
            }
        }
    }
}
