// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies a chained sequence of sources releases the subscription of each source as soon as it is done with it.</summary>
public sealed class ChainSubscriptionReleaseTests
{
    /// <summary>The sources chained when each ends during its own subscribe.</summary>
    private const int Sources = 5;

    /// <summary>The value of the source that stays running.</summary>
    private const int RunningValue = 1;

    /// <summary>The value of the source the chain never reaches.</summary>
    private const int NeverValue = 2;

    /// <summary>Verifies the subscription of a source that ended during its own subscribe is released before the chain ends.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscriptionOfASourceThatEndedWhileSubscribingIsReleased()
    {
        var sources = Enumerable.Range(0, Sources).Select(static value => new TrackedSource(value, completesAtOnce: true)).ToArray();
        List<int> values = [];

        using var subscription = Signal.Concat(sources.Select(static source => (IObservable<int>)source))
            .Subscribe(values.Add, static _ => { }, static () => { });

        await Assert.That(values.SequenceEqual(Enumerable.Range(0, Sources))).IsTrue();
        await Assert.That(Array.TrueForAll(sources, static source => source.Disposed)).IsTrue();
    }

    /// <summary>Verifies disposing a chain part way through releases the subscription of the source that is running.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposingTheChainReleasesTheRunningSource()
    {
        TrackedSource first = new(0, completesAtOnce: true);
        TrackedSource running = new(RunningValue, completesAtOnce: false);
        TrackedSource never = new(NeverValue, completesAtOnce: true);

        var subscription = Signal.Concat(first, running, never).Subscribe(static _ => { }, static _ => { }, static () => { });
        var runningWhileChained = !running.Disposed;
        subscription.Dispose();

        await Assert.That(runningWhileChained).IsTrue();
        await Assert.That(running.Disposed).IsTrue();
        await Assert.That(never.Subscribed).IsFalse();
    }

    /// <summary>A source that gives one value and either ends at once or stays running, and records whether its subscription was released.</summary>
    /// <param name="value">The value.</param>
    /// <param name="completesAtOnce">Whether the source ends during its own subscribe.</param>
    private sealed class TrackedSource(int value, bool completesAtOnce) : IObservable<int>
    {
        /// <summary>Gets a value indicating whether the source was subscribed to.</summary>
        public bool Subscribed { get; private set; }

        /// <summary>Gets a value indicating whether the subscription was released.</summary>
        public bool Disposed { get; private set; }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<int> observer)
        {
            Subscribed = true;
            observer.OnNext(value);
            if (completesAtOnce)
            {
                observer.OnCompleted();
            }

            return new Release(this);
        }

        /// <summary>The subscription, which marks the source released when disposed.</summary>
        /// <param name="owner">The source.</param>
        private sealed class Release(TrackedSource owner) : IDisposable
        {
            /// <inheritdoc/>
            public void Dispose() => owner.Disposed = true;
        }
    }
}
