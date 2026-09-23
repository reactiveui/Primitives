// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Maps paired committed-state snapshots to pending summaries.</summary>
/// <typeparam name="TState">The state type.</typeparam>
/// <param name="source">The paired snapshot source.</param>
internal sealed class PendingSummaryObservable<TState>(
    IObservable<OccasionallyConnectedCommittedStateQueueSnapshot<TState>> source) : IObservable<PendingSyncSummary>
{
    /// <inheritdoc />
    public IDisposable Subscribe(IObserver<PendingSyncSummary> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new SnapshotObserver(observer));
    }

    /// <summary>Forwards changed pending summaries.</summary>
    /// <param name="observer">The downstream observer.</param>
    private sealed class SnapshotObserver(IObserver<PendingSyncSummary> observer) : IObserver<OccasionallyConnectedCommittedStateQueueSnapshot<TState>>
    {
        /// <summary>The last summary forwarded to this subscriber.</summary>
        private PendingSyncSummary? _last;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => observer.OnCompleted();

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => observer.OnError(error);

        /// <inheritdoc />
        public void OnNext(OccasionallyConnectedCommittedStateQueueSnapshot<TState> value)
        {
            var pending = value.Pending;
            if (pending == _last)
            {
                return;
            }

            _last = pending;
            observer.OnNext(pending);
        }
    }
}
