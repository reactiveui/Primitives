// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Verifies ownership and admission of caller-supplied inbox candidates.</summary>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>Verifies a candidate is captured once before inbox evaluation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task InboxLookupReadsEachCandidateOnce()
    {
        await using var store = await CreateInitializedStoreAsync();
        var candidates = new ChangingEventIdList(1, false);
        var result = await store.GetUnappliedEventIdsAsync(Stream, candidates, CancellationToken.None);
        await Assert.That(result.Count).IsEqualTo(1);
        await Assert.That(result[0]).IsEqualTo(candidates.FirstId);
        await Assert.That(candidates.ReadCount).IsEqualTo(1);
    }

    /// <summary>Verifies a list exceeding finite query bounds is rejected before its indexer runs.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task InboxLookupRejectsOversizedCandidatesBeforeReading()
    {
        await using var store = await CreateInitializedStoreAsync();
        var candidates = new ChangingEventIdList(int.MaxValue, true);
        Func<Task> lookup = async () => _ = await store.GetUnappliedEventIdsAsync(Stream, candidates, CancellationToken.None);
        await Assert.That(lookup).ThrowsExactly<QueueCapacityExceededException>();
        await Assert.That(candidates.ReadCount).IsEqualTo(0);
    }

    /// <summary>Verifies capture does not hold the store gate and concurrent query capacity is finite.</summary>
    /// <param name="recordCapacity">The transient and retained record limit.</param>
    /// <param name="byteCapacity">The transient and retained byte limit.</param>
    /// <returns>The asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The assertion did not return its expected exception.</exception>
    [Test]
    [Arguments(1, 4096L)]
    [Arguments(100, 64L)]
    public async Task InboxLookupBoundsConcurrentCaptureOutsideGate(int recordCapacity, long byteCapacity)
    {
        const int TimeoutSeconds = 5;
        await using var store = new InMemoryLocalStoreAdapter(recordCapacity, byteCapacity);
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        Action blockCapture = () =>
        {
            entered.Set();
            release.Wait();
        };
        var candidates = new ChangingEventIdList(1, false) { OnRead = blockCapture };
        var lookup = Task.Run(async () => await store.GetUnappliedEventIdsAsync(Stream, candidates, CancellationToken.None));
        try
        {
            await Assert.That(entered.Wait(TimeSpan.FromSeconds(TimeoutSeconds))).IsTrue();
            var status = await Task.Run(async () => await store.GetOperationStatusAsync(OperationId.New(), CancellationToken.None))
                .WaitAsync(TimeSpan.FromSeconds(TimeoutSeconds));
            await Assert.That(status).IsNull();
            var rejected = new ChangingEventIdList(1, true);
            Func<Task> overflow = async () => _ = await store.GetUnappliedEventIdsAsync(Stream, rejected, CancellationToken.None);
            var exception = await Assert.That(overflow).ThrowsExactly<QueueCapacityExceededException>() ?? throw new InvalidOperationException("No capacity exception.");
            await Assert.That(exception.CanFitWhenEmpty).IsTrue();
            await Assert.That(rejected.ReadCount).IsEqualTo(0);
        }
        finally
        {
            release.Set();
            _ = await lookup;
        }

        var next = await store.GetUnappliedEventIdsAsync(Stream, [Guid.NewGuid()], CancellationToken.None);
        await Assert.That(next.Count).IsEqualTo(1);
    }

    /// <summary>Verifies cancellation and caller exceptions both reclaim transient capacity.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task InboxLookupFailureReleasesCaptureReservation()
    {
        const long ByteCapacity = 128;
        await using var store = new InMemoryLocalStoreAdapter(1, ByteCapacity);
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        using var cancellation = new CancellationTokenSource();
        var candidates = new ChangingEventIdList(1, false) { OnRead = cancellation.Cancel };
        Func<Task> canceled = async () => _ = await store.GetUnappliedEventIdsAsync(Stream, candidates, cancellation.Token);
        await Assert.That(canceled).Throws<OperationCanceledException>();
        Func<Task> failed = async () => _ = await store.GetUnappliedEventIdsAsync(Stream, new ChangingEventIdList(1, true), CancellationToken.None);
        await Assert.That(failed).ThrowsExactly<InvalidOperationException>();
        var result = await store.GetUnappliedEventIdsAsync(Stream, [], CancellationToken.None);
        await Assert.That(result.Count).IsEqualTo(0);
        result = await store.GetUnappliedEventIdsAsync(Stream, [Guid.NewGuid()], CancellationToken.None);
        await Assert.That(result.Count).IsEqualTo(1);
    }

    /// <summary>Verifies byte limits and invalid counts reject before input capture.</summary>
    /// <returns>The asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The assertion did not return its expected exception.</exception>
    [Test]
    public async Task InboxLookupRejectsInvalidCountAndExcessiveBytes()
    {
        const long ByteCapacity = 64;
        const int RecordCapacity = 100;
        const int CandidateCount = 3;
        await using var store = new InMemoryLocalStoreAdapter(RecordCapacity, ByteCapacity);
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        Func<Task> invalid = async () => _ = await store.GetUnappliedEventIdsAsync(Stream, new ChangingEventIdList(-1, true), CancellationToken.None);
        await Assert.That(invalid).ThrowsExactly<ArgumentOutOfRangeException>();
        var candidates = new ChangingEventIdList(CandidateCount, true);
        Func<Task> overflow = async () => _ = await store.GetUnappliedEventIdsAsync(Stream, candidates, CancellationToken.None);
        var exception = await Assert.That(overflow).ThrowsExactly<QueueCapacityExceededException>() ?? throw new InvalidOperationException("No capacity exception.");
        await Assert.That(exception.CanFitWhenEmpty).IsFalse();
        await Assert.That(candidates.ReadCount).IsEqualTo(0);
    }

    /// <summary>A caller list that exposes repeated reads and pre-admission indexing.</summary>
    /// <param name="count">The advertised number of candidates.</param>
    /// <param name="failOnRead">Whether indexing must fail.</param>
    private sealed class ChangingEventIdList(int count, bool failOnRead) : IReadOnlyList<Guid>
    {
        /// <summary>Gets the first candidate supplied by this caller.</summary>
        public Guid FirstId { get; } = Guid.NewGuid();

        /// <summary>Gets the number of candidate reads.</summary>
        public int ReadCount { get; private set; }

        /// <summary>Gets the caller callback invoked by the indexer.</summary>
        public Action? OnRead { get; init; }

        /// <inheritdoc/>
        public int Count => count;

        /// <inheritdoc/>
        /// <exception cref="InvalidOperationException">The caller prohibits indexing.</exception>
        public Guid this[int index]
        {
            get
            {
                ReadCount++;
                OnRead?.Invoke();
                if (failOnRead)
                {
                    throw new InvalidOperationException("The caller list was indexed before admission.");
                }

                return ReadCount == 1 ? FirstId : Guid.NewGuid();
            }
        }

        /// <inheritdoc/>
        public IEnumerator<Guid> GetEnumerator()
        {
            for (var index = 0; index < Count; index++)
            {
                yield return this[index];
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
