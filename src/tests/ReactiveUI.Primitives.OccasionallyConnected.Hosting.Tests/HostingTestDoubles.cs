// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Hosting.Tests;

/// <summary>Test doubles shared by hosting tests.</summary>
internal static class HostingTestDoubles
{
    /// <summary>The encoded bytes charged per pending operation.</summary>
    private const long BytesPerOperation = 10;

    /// <summary>Creates a synchronization state.</summary>
    /// <param name="status">The lifecycle status.</param>
    /// <param name="pendingOperations">The pending operation count.</param>
    /// <param name="reasonCode">The reason code.</param>
    /// <returns>The state.</returns>
    internal static SyncState CreateState(SyncLifecycleStatus status, int pendingOperations, string? reasonCode) =>
        new(status, NetworkAvailable: false, pendingOperations, pendingOperations * BytesPerOperation, DateTimeOffset.UnixEpoch, null, null, reasonCode);

    /// <summary>A context that records lifecycle calls and publishes states on demand.</summary>
    internal sealed class RecordingContext : IOccasionallyConnectedContext, IObservable<SyncState>
    {
        /// <summary>Protects the observer list.</summary>
        private readonly Lock _gate = new();

        /// <summary>The subscribed observers.</summary>
        private readonly List<IObserver<SyncState>> _observers = [];

        /// <summary>The latest published state, replayed to new subscribers like the real context.</summary>
        private SyncState? _latest;

        /// <inheritdoc/>
        public ISyncEngine SyncEngine => throw new NotSupportedException();

        /// <inheritdoc/>
        public IObservable<SyncState> SyncStates => this;

        /// <summary>Gets the number of start calls.</summary>
        internal int StartCalls { get; private set; }

        /// <summary>Gets the number of stop calls.</summary>
        internal int StopCalls { get; private set; }

        /// <summary>Gets the token passed to the last stop call.</summary>
        internal CancellationToken LastStopToken { get; private set; }

        /// <summary>Gets the number of active subscriptions.</summary>
        internal int SubscriberCount
        {
            get
            {
                lock (_gate)
                {
                    return _observers.Count;
                }
            }
        }

        /// <inheritdoc/>
        public IOccasionallyConnectedStream<TState, TInput> GetOrCreateStream<TState, TInput>(StreamDefinition<TState, TInput> definition) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask StartAsync(CancellationToken cancellationToken)
        {
            StartCalls++;
            return default;
        }

        /// <inheritdoc/>
        public ValueTask StopAsync(CancellationToken cancellationToken)
        {
            StopCalls++;
            LastStopToken = cancellationToken;
            return default;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => default;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<SyncState> observer)
        {
            SyncState? latest;
            lock (_gate)
            {
                _observers.Add(observer);
                latest = _latest;
            }

            if (latest is not null)
            {
                observer.OnNext(latest);
            }

            return new Subscription(this, observer);
        }

        /// <summary>Publishes a state to every subscriber.</summary>
        /// <param name="state">The state.</param>
        internal void Publish(SyncState state)
        {
            IObserver<SyncState>[] observers;
            lock (_gate)
            {
                _latest = state;
                observers = [.. _observers];
            }

            foreach (var observer in observers)
            {
                observer.OnNext(state);
            }
        }

        /// <summary>Removes an observer.</summary>
        /// <param name="observer">The observer.</param>
        private void Remove(IObserver<SyncState> observer)
        {
            lock (_gate)
            {
                _ = _observers.Remove(observer);
            }
        }

        /// <summary>Removes its observer when disposed.</summary>
        /// <param name="owner">The owning context.</param>
        /// <param name="observer">The observer.</param>
        private sealed class Subscription(RecordingContext owner, IObserver<SyncState> observer) : IDisposable
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Dispose() => owner.Remove(observer);
        }
    }
}
