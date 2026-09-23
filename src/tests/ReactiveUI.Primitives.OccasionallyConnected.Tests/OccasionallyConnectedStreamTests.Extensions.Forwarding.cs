// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedStream{TState,TInput}"/>.</summary>
/// <content>Paired snapshot projection and terminal forwarding.</content>
public sealed partial class OccasionallyConnectedStreamTests
{
    /// <summary>Verifies pending summaries suppress equal values while synchronized states remain independent.</summary>
    /// <returns>A task that completes when the assertions finish.</returns>
    [Test]
    public async Task PairedSnapshotProjectionsSuppressEqualPendingAndForwardErrors()
    {
        var source = new TerminalSnapshotSource();
        var pending = new RecordingObserver<PendingSyncSummary>();
        var states = new RecordingObserver<CounterState>();
        using var pendingSubscription = new PendingSummaryObservable<CounterState>(source).Subscribe(pending);
        using var stateSubscription = new SynchronizedStateObservable<CounterState>(source).Subscribe(states);
        var zero = new PendingSyncSummary(0, 0, null);

        source.Publish(new(new(FirstValue), zero));
        source.Publish(new(new(SecondValue), zero));
        await Assert.That(pending.Values).Count().IsEqualTo(1);
        await AssertSequenceAsync(states.Values.Select(static state => state.Sum).ToArray(), [FirstValue, SecondValue]);

        var error = new InvalidOperationException("Paired snapshot source failed.");
        source.Fail(error);
        await Assert.That(pending.Error).IsSameReferenceAs(error);
        await Assert.That(states.Error).IsSameReferenceAs(error);
    }

    /// <summary>Verifies both projections forward completion from their paired source.</summary>
    /// <returns>A task that completes when the assertions finish.</returns>
    [Test]
    public async Task PairedSnapshotProjectionsForwardCompletion()
    {
        var source = new TerminalSnapshotSource();
        var pending = new RecordingObserver<PendingSyncSummary>();
        var states = new RecordingObserver<CounterState>();
        using var pendingSubscription = new PendingSummaryObservable<CounterState>(source).Subscribe(pending);
        using var stateSubscription = new SynchronizedStateObservable<CounterState>(source).Subscribe(states);

        source.Complete();

        await Assert.That(pending.CompletedCount).IsEqualTo(1);
        await Assert.That(states.CompletedCount).IsEqualTo(1);
    }

    /// <summary>Manual paired snapshot source with terminal forwarding.</summary>
    private sealed class TerminalSnapshotSource : IObservable<OccasionallyConnectedCommittedStateQueueSnapshot<CounterState>>
    {
        /// <summary>The active subscriptions.</summary>
        private readonly List<IObserver<OccasionallyConnectedCommittedStateQueueSnapshot<CounterState>>> _observers = [];

        /// <inheritdoc />
        public IDisposable Subscribe(IObserver<OccasionallyConnectedCommittedStateQueueSnapshot<CounterState>> observer)
        {
            ArgumentNullException.ThrowIfNull(observer);
            _observers.Add(observer);
            return new Subscription(this, observer);
        }

        /// <summary>Publishes a paired snapshot.</summary>
        /// <param name="snapshot">The snapshot.</param>
        public void Publish(OccasionallyConnectedCommittedStateQueueSnapshot<CounterState> snapshot)
        {
            foreach (var observer in _observers.ToArray())
            {
                observer.OnNext(snapshot);
            }
        }

        /// <summary>Fails the source.</summary>
        /// <param name="error">The terminal error.</param>
        public void Fail(Exception error)
        {
            foreach (var observer in _observers.ToArray())
            {
                observer.OnError(error);
            }
        }

        /// <summary>Completes the source.</summary>
        public void Complete()
        {
            foreach (var observer in _observers.ToArray())
            {
                observer.OnCompleted();
            }
        }

        /// <summary>Removes one observer from the source.</summary>
        /// <param name="observer">The observer to remove.</param>
        private void Remove(IObserver<OccasionallyConnectedCommittedStateQueueSnapshot<CounterState>> observer) => _ = _observers.Remove(observer);

        /// <summary>Owns one source observer registration.</summary>
        /// <param name="owner">The source.</param>
        /// <param name="observer">The observer.</param>
        private sealed class Subscription(TerminalSnapshotSource owner, IObserver<OccasionallyConnectedCommittedStateQueueSnapshot<CounterState>> observer) : IDisposable
        {
            /// <inheritdoc />
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Dispose() => owner.Remove(observer);
        }
    }
}
