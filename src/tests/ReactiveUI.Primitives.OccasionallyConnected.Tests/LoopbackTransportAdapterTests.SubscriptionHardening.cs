// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="LoopbackTransportAdapter"/>.</summary>
/// <content>Subscription hardening tests for <see cref="LoopbackTransportAdapter"/>.</content>
public sealed partial class LoopbackTransportAdapterTests
{
    /// <summary>Verifies a lost acknowledgement can redeliver the current batch without breaking later cursor continuity.</summary>
    /// <param name="resuming">Whether the subscription starts from the previously applied cursor.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SubscribeAllowsCurrentCursorRedeliveryBeforeNextBatch(bool resuming)
    {
        var first = CreateReceiveBatch(CreateRemoteEvent());
        var next = new RemoteEventBatch(Guid.NewGuid(), first.StreamId, first.NextCursor, "cursor-after-redelivery", []);
        var hub = new RecordingHub { SubscribeHandler = (_, _, _) => YieldBatches(first, first, next) };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var request = CreateSubscribeRequest() with { Cursor = resuming ? first.NextCursor : null };
        await using var enumerator = session.SubscribeAsync(request, CancellationToken.None).GetAsyncEnumerator();

        await Assert.That(await enumerator.MoveNextAsync()).IsTrue();
        await Assert.That(enumerator.Current).IsSameReferenceAs(first);
        await Assert.That(await enumerator.MoveNextAsync()).IsTrue();
        await Assert.That(enumerator.Current).IsSameReferenceAs(first);
        await Assert.That(await enumerator.MoveNextAsync()).IsTrue();
        await Assert.That(enumerator.Current).IsSameReferenceAs(next);
        await Assert.That(await enumerator.MoveNextAsync()).IsFalse();
    }

    /// <summary>Verifies the adapter validates and exposes the same upstream current batch instance.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeReadsUpstreamCurrentOnceBeforeExposure()
    {
        var validated = CreateReceiveBatch(CreateRemoteEvent());
        var unvalidated = new RemoteEventBatch(Guid.NewGuid(), new("sensor/humidity"), null, "cursor-swapped", []);
        var source = new ChangingCurrentEnumerable(validated, unvalidated);
        var hub = new RecordingHub { SubscribeHandler = (_, _, _) => source };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var enumerator = session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None).GetAsyncEnumerator();

        await Assert.That(await enumerator.MoveNextAsync()).IsTrue();
        await Assert.That(enumerator.Current).IsSameReferenceAs(validated);
        await enumerator.DisposeAsync();
    }

    /// <summary>Verifies a null upstream subscription sequence is rejected and releases capacity.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeRejectsNullHubSequenceAndReleasesCapacity()
    {
        var state = new NullThenValidSubscribeState();
        var hub = new RecordingHub { SubscribeHandler = (_, _, _) => state.Subscribe() };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub) with { MaximumConcurrentSubscriptions = 1 });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        var exception = Assert.ThrowsExactly<InvalidOperationException>(() => session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None).GetAsyncEnumerator());
        await Assert.That(exception.Message).Contains("no subscription sequence");

        var replacement = session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None).GetAsyncEnumerator();
        await Assert.That(await replacement.MoveNextAsync()).IsFalse();
        await replacement.DisposeAsync();
    }

    /// <summary>Creates a null sequence by reading a default holder value.</summary>
    /// <returns>The null sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IAsyncEnumerable<RemoteEventBatch> CreateNullSubscribeSequence() =>
        default(NullSubscribeSequenceBox).Sequence;

    /// <summary>Holds a subscription sequence behind a defaultable value.</summary>
    private readonly struct NullSubscribeSequenceBox
    {
        /// <summary>Gets the subscription sequence value.</summary>
        public IAsyncEnumerable<RemoteEventBatch> Sequence { get; init; }
    }

    /// <summary>Returns a null sequence once and a valid empty sequence afterwards.</summary>
    private sealed class NullThenValidSubscribeState
    {
        /// <summary>Whether the next subscription should return null.</summary>
        private bool _returnNull = true;

        /// <summary>Returns the next subscription sequence.</summary>
        /// <returns>The next subscription sequence.</returns>
        public IAsyncEnumerable<RemoteEventBatch> Subscribe()
        {
            if (!_returnNull)
            {
                return YieldBatches();
            }

            _returnNull = false;
            return CreateNullSubscribeSequence();
        }
    }

    /// <summary>Provides an upstream sequence whose current batch changes between reads.</summary>
    /// <param name="first">The first current batch.</param>
    /// <param name="second">The second current batch.</param>
    private sealed class ChangingCurrentEnumerable(RemoteEventBatch first, RemoteEventBatch second) : IAsyncEnumerable<RemoteEventBatch>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IAsyncEnumerator<RemoteEventBatch> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
            new ChangingCurrentEnumerator(first, second);
    }

    /// <summary>Changes the current batch after the first read.</summary>
    /// <param name="first">The first current batch.</param>
    /// <param name="second">The second current batch.</param>
    private sealed class ChangingCurrentEnumerator(RemoteEventBatch first, RemoteEventBatch second) : IAsyncEnumerator<RemoteEventBatch>
    {
        /// <summary>The number of move calls.</summary>
        private int _moves;

        /// <summary>The number of current reads.</summary>
        private int _currentReads;

        /// <inheritdoc/>
        public RemoteEventBatch Current => Interlocked.Increment(ref _currentReads) == 1 ? first : second;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> MoveNextAsync() => ValueTask.FromResult(Interlocked.Increment(ref _moves) == 1);
    }
}
