// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="RemoteSnapshotRecoveryRequest"/>.</summary>
public sealed class RemoteSnapshotRecoveryRequestTests
{
    /// <summary>The first operation sequence used by test fixtures.</summary>
    private const int FirstSequence = 1;

    /// <summary>The second operation sequence used by test fixtures.</summary>
    private const int SecondSequence = 2;

    /// <summary>The response byte limit used by request fixtures.</summary>
    private const int MaximumResponseBytes = 1024;

    /// <summary>The stream identifier used by test fixtures.</summary>
    private static readonly StreamId Stream = new("orders/live");

    /// <summary>Verifies pending operations are defensively copied.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PendingOperationsAreCopied()
    {
        var operations = new List<SyncOperation> { CreateOperation(FirstSequence) };
        var request = CreateRequest(operations);
        operations.Add(CreateOperation(SecondSequence));

        await Assert.That(request.PendingOperations).Count().IsEqualTo(1);
        await Assert.That(((ICollection<SyncOperation>)request.PendingOperations).IsReadOnly).IsTrue();
    }

    /// <summary>Verifies replay operations are defensively copied.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReplayOperationsAreCopied()
    {
        var operations = new List<SyncOperation> { CreateOperation(FirstSequence) };
        var request = CreateRequest([]) with { ReplayOperations = operations };
        operations.Add(CreateOperation(SecondSequence));

        await Assert.That(request.ReplayOperations).Count().IsEqualTo(1);
        await Assert.That(((ICollection<SyncOperation>)request.ReplayOperations).IsReadOnly).IsTrue();
    }

    /// <summary>Verifies callers that omit replay operations keep the legacy empty-role shape.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReplayOperationsDefaultsToEmpty()
    {
        var request = CreateRequest([]);

        await Assert.That(request.ReplayOperations).IsEmpty();
        await Assert.That(((ICollection<SyncOperation>)request.ReplayOperations).IsReadOnly).IsTrue();
    }

    /// <summary>Verifies replay operations reject runtime null collections.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReplayOperationsRejectsNullCollection()
    {
        var create = static () => CreateRequest([]) with
        {
            ReplayOperations = NullReference<IReadOnlyList<SyncOperation>>(),
        };

        await Assert.That(create).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies the fixed ownership ceiling is checked before indexing caller collections.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PendingOperationsRejectsOversizedCollectionBeforeIndexing()
    {
        var list = new OversizedOperationList();

        await Assert.That(() => CreateRequest(list)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(list.IndexerUsed).IsFalse();
    }

    /// <summary>Verifies the fixed replay ownership ceiling is checked before indexing caller collections.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReplayOperationsRejectsOversizedCollectionBeforeIndexing()
    {
        var list = new OversizedOperationList();

        await Assert.That(() => CreateRequest([]) with { ReplayOperations = list }).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(list.IndexerUsed).IsFalse();
    }

    /// <summary>Creates a recovery request fixture with the supplied pending operations.</summary>
    /// <param name="pending">The pending operations.</param>
    /// <returns>The request fixture.</returns>
    private static RemoteSnapshotRecoveryRequest CreateRequest(IReadOnlyList<SyncOperation> pending) => new()
    {
        StreamId = Stream,
        SubscriptionId = SubscriptionId.New(),
        ExpiredCursor = null,
        ClientStateContractId = "order-state",
        ClientStateSchemaVersion = 1,
        SnapshotFormatVersion = 1,
        PendingOperations = pending,
        MaximumResponseBytes = MaximumResponseBytes,
    };

    /// <summary>Creates a pending operation fixture.</summary>
    /// <param name="sequence">The client sequence.</param>
    /// <returns>The operation fixture.</returns>
    private static SyncOperation CreateOperation(long sequence) => new()
    {
        OperationId = OperationId.New(),
        StreamId = Stream,
        ClientSequence = sequence,
        TimestampUtc = DateTimeOffset.UnixEpoch,
        Type = SyncOperationType.Custom,
        Payload = new("order-command", 1, "application/json", ReadOnlyMemory<byte>.Empty, "sha256-empty"),
        Policy = OperationPolicy.Default,
        Metadata = new Dictionary<string, string>(),
    };

    /// <summary>Creates a typed null reference for runtime-null contract regression tests.</summary>
    /// <typeparam name="T">The reference type.</typeparam>
    /// <returns>A null reference typed as <typeparamref name="T"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T NullReference<T>()
        where T : class
    {
        object? value = null;
        return Unsafe.As<object?, T>(ref value);
    }

    /// <summary>A caller collection that fails if the copy helper indexes before checking the count.</summary>
    private sealed class OversizedOperationList : IReadOnlyList<SyncOperation>
    {
        /// <summary>The fixed owned-copy ceiling plus one.</summary>
        private const int OversizedCount = 4097;

        /// <summary>Gets a value indicating whether enumeration or indexing was attempted.</summary>
        public bool IndexerUsed { get; private set; }

        /// <summary>Gets the oversized item count.</summary>
        public int Count => OversizedCount;

        /// <summary>Gets the item at the supplied index and records indexer access.</summary>
        /// <param name="index">The requested index.</param>
        /// <returns>The operation fixture.</returns>
        public SyncOperation this[int index]
        {
            get
            {
                IndexerUsed = true;
                return CreateOperation(index + FirstSequence);
            }
        }

        /// <summary>Gets an enumerator and records enumeration access.</summary>
        /// <returns>The empty enumerator.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<SyncOperation> GetEnumerator()
        {
            IndexerUsed = true;
            return Array.Empty<SyncOperation>().AsEnumerable().GetEnumerator();
        }

        /// <summary>Gets an enumerator and records enumeration access.</summary>
        /// <returns>The empty enumerator.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
