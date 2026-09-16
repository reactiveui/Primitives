// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Advanced;

namespace ReactiveUI.Primitives.Async.Tests.Internals;

/// <summary>Tests for <see cref="SyncLatestCoordinatorExtensions"/> and <see cref="SyncLatestSlot"/>, which wire sources into a coordinator's slots.</summary>
public class SyncLatestCoordinatorExtensionsTests
{
    /// <summary>The number of sources the test coordinator combines.</summary>
    private const int SourceCount = 2;

    /// <summary>The value pushed through a source.</summary>
    private const int SampleValue = 7;

    /// <summary>Verifies that a slot's witness records its source's values and reports completion through its own bit.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSlotWitnessObservesValueAndCompletion_ThenCoordinatorRecordsAndCounts()
    {
        CountingCoordinator coordinator = new(SourceCount);
        var recorded = 0;
        var witness = SyncLatestSlot.CreateWitness(coordinator, 1, (int value) => recorded = value);

        await witness.OnNextAsync(SampleValue, CancellationToken.None);
        await witness.OnCompletedAsync(Result.Success);

        await Assert.That(recorded).IsEqualTo(SampleValue);
        await Assert.That(coordinator.Emits).IsEqualTo(1);
        await Assert.That(coordinator.Lifecycle.HasDisposed).IsFalse();

        await coordinator.DisposeAsync();
    }

    /// <summary>Verifies that subscribing a source to its slot records what the source emits.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeToSlotAsync_ThenSourceValuesReachTheSlot()
    {
        CountingCoordinator coordinator = new(SourceCount);
        var recorded = 0;

        await using var subscription = await SignalAsync.Return(SampleValue)
            .SubscribeToSlotAsync(coordinator, 0, value => recorded = value, CancellationToken.None);

        await Assert.That(recorded).IsEqualTo(SampleValue);
        await Assert.That(subscription).IsNotNull();

        await coordinator.DisposeAsync();
    }

    /// <summary>Verifies that subscribing every source fills each lifecycle slot in index order.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeSourcesAsync_ThenEverySlotIsFilled()
    {
        CountingCoordinator coordinator = new(SourceCount);

        await ((ISyncLatestCoordinator<int>)coordinator).SubscribeSourcesAsync(CancellationToken.None);

        await Assert.That(coordinator.SubscribedIndices.SequenceEqual([0, 1])).IsTrue();
        await Assert.That(coordinator.Lifecycle.Subscriptions[0]).IsNotNull();
        await Assert.That(coordinator.Lifecycle.Subscriptions[1]).IsNotNull();

        await coordinator.DisposeAsync();
    }

    /// <summary>Verifies that a null coordinator is rejected.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCoordinatorIsNull_ThenArgumentNullExceptionIsThrown()
    {
        ISyncLatestCoordinator<int> missing = null!;

        _ = Assert.Throws<ArgumentNullException>(
            () => SyncLatestSlot.CreateWitness(missing, 0, static (int _) => { }));
        await Assert.That(async () => await missing.SubscribeSourcesAsync(CancellationToken.None))
            .Throws<ArgumentNullException>();
    }

    /// <summary>Verifies that a null value recorder is rejected.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRecordValueIsNull_ThenArgumentNullExceptionIsThrown()
    {
        CountingCoordinator coordinator = new(SourceCount);

        _ = Assert.Throws<ArgumentNullException>(
            () => SyncLatestSlot.CreateWitness<int, int>(coordinator, 0, null!));

        await coordinator.DisposeAsync();
    }

    /// <summary>Verifies that a null source is rejected.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourceIsNull_ThenArgumentNullExceptionIsThrown()
    {
        CountingCoordinator coordinator = new(SourceCount);
        IObservableAsync<int> missing = null!;

        await Assert.That(
                async () => await missing.SubscribeToSlotAsync(coordinator, 0, static _ => { }, CancellationToken.None))
            .Throws<ArgumentNullException>();

        await coordinator.DisposeAsync();
    }

    /// <summary>Verifies that an index outside the coordinator's slots is rejected.</summary>
    /// <param name="sourceIndex">The out-of-range index under test.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [Arguments(-1)]
    [Arguments(SourceCount)]
    public async Task WhenSourceIndexIsOutOfRange_ThenArgumentOutOfRangeExceptionIsThrown(int sourceIndex)
    {
        CountingCoordinator coordinator = new(SourceCount);

        _ = Assert.Throws<ArgumentOutOfRangeException>(
            () => SyncLatestSlot.CreateWitness(coordinator, sourceIndex, static (int _) => { }));

        await coordinator.DisposeAsync();
    }

    /// <summary>A coordinator that counts emits and the indices it was asked to subscribe.</summary>
    /// <param name="sourceCount">The number of combined sources.</param>
    private sealed class CountingCoordinator(int sourceCount) : ISyncLatestCoordinator<int>
    {
        /// <inheritdoc/>
        public SyncLatestLifecycle<int> Lifecycle { get; } = new(new DiscardingObserver(), sourceCount);

        /// <summary>Gets the number of projections requested.</summary>
        public int Emits { get; private set; }

        /// <summary>Gets the indices passed to <see cref="SubscribeAtAsync"/> in order.</summary>
        public List<int> SubscribedIndices { get; } = [];

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => Lifecycle.DisposeAsync();

        /// <inheritdoc/>
        public ValueTask EmitLatestAsync()
        {
            Emits++;
            return default;
        }

        /// <inheritdoc/>
        public ValueTask<IAsyncDisposable> SubscribeAtAsync(int index, CancellationToken cancellationToken)
        {
            SubscribedIndices.Add(index);
            return SignalAsync.Return(index)
                .SubscribeToSlotAsync(this, index, static _ => { }, cancellationToken);
        }
    }

    /// <summary>An observer that drops everything it receives.</summary>
    private sealed class DiscardingObserver : IObserverAsync<int>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnNextAsync(int value, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnCompletedAsync(Result result) => default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => default;
    }
}
