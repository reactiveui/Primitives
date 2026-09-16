// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests bounded-concurrency blend admission, termination and delivery serialization.</summary>
public sealed class MaxConcurrentBlendCoordinatorTests
{
    /// <summary>The second value a test pushes.</summary>
    private const int Second = 2;

    /// <summary>The third value a test pushes.</summary>
    private const int Third = 3;

    /// <summary>The message of the error raised for a null inner source.</summary>
    private const string NullSourceMessage = "Blend source contained null.";

    /// <summary>
    /// An observer that marshals synchronously to another thread is not deadlocked when that thread pushes a value
    /// through a sibling source.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ObserverMarshallingToAnotherSourceThreadDoesNotDeadlock() =>
        MergeDeliveryAssertions.ObserverMarshallingToAnotherSourceThreadDoesNotDeadlock(Subscribe);

    /// <summary>A value pushed by the observer itself is delivered after the observer returns, not inside it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ValueRaisedByTheObserverIsDeliveredAfterItReturns() =>
        MergeDeliveryAssertions.ValueRaisedByTheObserverIsDeliveredAfterItReturns(Subscribe);

    /// <summary>Sources pushing from separate threads never overlap downstream, and each source's values arrive in order.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ConcurrentSourcesDeliverEveryValueInOrderWithoutOverlap() =>
        MergeDeliveryAssertions.ConcurrentSourcesDeliverEveryValueInOrderWithoutOverlap(Subscribe);

    /// <summary>An error raised while another thread delivers is delivered after the values queued before it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ErrorRaisedDuringDeliveryFollowsTheQueuedValues() =>
        MergeDeliveryAssertions.ErrorRaisedDuringDeliveryFollowsTheQueuedValues(Subscribe);

    /// <summary>A value pushed while a terminal notification waits for the delivering thread is dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ValuePushedWhileTheTerminalWaitsIsDropped() =>
        MergeDeliveryAssertions.ValuePushedWhileTheTerminalWaitsIsDropped(Subscribe);

    /// <summary>Sources that complete while they are being subscribed free their slot for the next source in turn.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SourcesCompletingWhileSubscribedAreAdmittedInTurn()
    {
        RecordingWitness<int> downstream = new();

        using var subscription = new MaxConcurrentBlendCoordinator<int>(downstream).Run(
            [EmitAndComplete(1), EmitAndComplete(Second), EmitAndComplete(Third)],
            1);

        await Assert.That(string.Join(",", downstream.Values)).IsEqualTo("1,2,3");
        await Assert.That(downstream.Completed).IsEqualTo(1);
    }

    /// <summary>Once the sources run out, completion waits for every source that is still active.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CompletionWaitsForEveryActiveSourceAfterEnumerationEnds()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        RecordingWitness<int> downstream = new();

        using var subscription = new MaxConcurrentBlendCoordinator<int>(downstream).Run(
            [
                new ScriptedObservable<int>(observer => left = observer),
                new ScriptedObservable<int>(observer => right = observer),
            ],
            Third);
        left!.OnNext(1);
        left.OnCompleted();
        await Assert.That(downstream.Completed).IsEqualTo(0);

        right!.OnCompleted();
        await Assert.That(downstream.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(downstream.Completed).IsEqualTo(1);
    }

    /// <summary>A failure reading the next source is delivered as the error and releases the enumerator.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task EnumerationFailureIsDeliveredAndReleasesTheEnumerator()
    {
        IObserver<int>? active = null;
        InvalidOperationException expected = new("enumerate");
        SourceSequence sources = new([new ScriptedObservable<int>(observer => active = observer)], expected);
        RecordingWitness<int> downstream = new();

        using var subscription = new MaxConcurrentBlendCoordinator<int>(downstream).Run(sources, Second);
        active!.OnNext(1);

        await Assert.That(downstream.Errors.Count).IsEqualTo(1);
        await Assert.That(downstream.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(downstream.Values.Count).IsEqualTo(0);
        await Assert.That(sources.DisposeCount).IsEqualTo(1);
    }

    /// <summary>A null source is delivered as an error and releases the enumerator.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task NullSourceIsDeliveredAsAnErrorAndReleasesTheEnumerator()
    {
        SourceSequence sources = new([null!], null);
        RecordingWitness<int> downstream = new();

        using var subscription = new MaxConcurrentBlendCoordinator<int>(downstream).Run(sources, 1);

        await Assert.That(downstream.Errors.Count).IsEqualTo(1);
        await Assert.That(downstream.Errors[0].Message).IsEqualTo(NullSourceMessage);
        await Assert.That(sources.DisposeCount).IsEqualTo(1);
    }

    /// <summary>An inner error releases the sibling subscriptions and the unread sources.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task InnerErrorReleasesTheActiveSubscriptionsAndTheEnumerator()
    {
        IObserver<int>? failing = null;
        RecordingDisposable siblingSubscription = new();
        SourceSequence sources = new(
            [
                new ScriptedObservable<int>(observer => failing = observer),
                new SubscriptionObservable(siblingSubscription),
                new ScriptedObservable<int>(static _ => { }),
            ],
            null);
        RecordingWitness<int> downstream = new();

        using var subscription = new MaxConcurrentBlendCoordinator<int>(downstream).Run(sources, Second);
        failing!.OnError(new InvalidOperationException("inner"));

        await Assert.That(downstream.Errors.Count).IsEqualTo(1);
        await Assert.That(siblingSubscription.DisposeCount).IsEqualTo(1);
        await Assert.That(sources.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Subscribes a coordinator that admits every source at once.</summary>
    /// <param name="sources">The sources to blend.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IDisposable Subscribe(IObservable<int>[] sources, IObserver<int> observer) =>
        new MaxConcurrentBlendCoordinator<int>(observer).Run(sources, sources.Length);

    /// <summary>Creates a source that emits one value and completes while it is being subscribed.</summary>
    /// <param name="value">The value to emit.</param>
    /// <returns>The source.</returns>
    private static ScriptedObservable<int> EmitAndComplete(int value) =>
        new(observer =>
        {
            observer.OnNext(value);
            observer.OnCompleted();
        });

    /// <summary>A source that returns a supplied subscription and never notifies.</summary>
    /// <param name="subscription">The subscription returned to the subscriber.</param>
    private sealed class SubscriptionObservable(IDisposable subscription) : IObservable<int>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Subscribe(IObserver<int> observer) => subscription;
    }

    /// <summary>A single-use sequence of sources that ends or fails after its sources, and counts its disposals.</summary>
    /// <param name="sources">The sources to yield.</param>
    /// <param name="failure">The error to throw once the sources run out, or <see langword="null"/> to end.</param>
    private sealed class SourceSequence(IObservable<int>[] sources, Exception? failure) : IEnumerable<IObservable<int>>, IEnumerator<IObservable<int>>
    {
        /// <summary>The index of the current source.</summary>
        private int _index = -1;

        /// <inheritdoc/>
        public IObservable<int> Current => sources[_index];

        /// <summary>Gets the number of times the enumerator was disposed.</summary>
        internal int DisposeCount { get; private set; }

        /// <inheritdoc/>
        object IEnumerator.Current => Current;

        /// <inheritdoc/>
        public IEnumerator<IObservable<int>> GetEnumerator() => this;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <inheritdoc/>
        public bool MoveNext()
        {
            if (_index + 1 < sources.Length)
            {
                _index++;
                return true;
            }

            return failure is null ? false : throw failure;
        }

        /// <inheritdoc/>
        public void Reset() => _index = -1;

        /// <inheritdoc/>
        public void Dispose() => DisposeCount++;
    }
}
