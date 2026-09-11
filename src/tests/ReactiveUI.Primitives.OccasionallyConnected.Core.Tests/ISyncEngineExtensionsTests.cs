// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="ISyncEngineExtensions"/>.</summary>
public sealed class ISyncEngineExtensionsTests
{
    /// <summary>The operation sequence.</summary>
    private const int ClientSequence = 1;

    /// <summary>The expected recorded token count.</summary>
    private const int TokenCount = 4;

    /// <summary>Verifies engine convenience overloads forward their arguments and cancellation token exactly once.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConvenienceOverloadsForwardOperationAndTokensExactlyOnce()
    {
        var engine = new Engine();
        var operation = new SyncOperation
        {
            OperationId = OperationId.New(),
            StreamId = new("stream"),
            ClientSequence = ClientSequence,
            TimestampUtc = DateTimeOffset.UnixEpoch,
            Type = SyncOperationType.Append,
            Payload = new("contract", ClientSequence, "json", ReadOnlyMemory<byte>.Empty, "hash"),
            Policy = OperationPolicy.Default,
            Metadata = new Dictionary<string, string>(),
        };
        var receipt = await engine.EnqueueOperationAsync(operation);
        await engine.StartAsync();
        await engine.TriggerSyncAsync();
        await engine.StopAsync();
        await Assert.That(receipt.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(engine.Operation).IsSameReferenceAs(operation);
        await Assert.That(engine.EnqueueCalls).IsEqualTo(1);
        await Assert.That(engine.StartCalls).IsEqualTo(1);
        await Assert.That(engine.TriggerCalls).IsEqualTo(1);
        await Assert.That(engine.StopCalls).IsEqualTo(1);
        await Assert.That(engine.Tokens).Count().IsEqualTo(TokenCount);
        foreach (var token in engine.Tokens)
        {
            await Assert.That(token).IsEqualTo(CancellationToken.None);
        }
    }

    /// <summary>Verifies the start overload propagates engine failures.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StartAsyncPropagatesEngineFailure()
    {
        var error = new InvalidOperationException("failure");
        var engine = new Engine { Error = error };
        Func<Task> action = async () => await engine.StartAsync();
        var thrown = await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        await Assert.That(thrown).IsSameReferenceAs(error);
        await Assert.That(engine.StartCalls).IsEqualTo(1);
    }

    /// <summary>Records synchronization engine calls.</summary>
    private sealed class Engine : ISyncEngine
    {
        /// <summary>Gets the recorded cancellation tokens.</summary>
        public List<CancellationToken> Tokens { get; } = [];

        /// <summary>Gets the enqueue call count.</summary>
        public int EnqueueCalls { get; private set; }

        /// <summary>Gets the start call count.</summary>
        public int StartCalls { get; private set; }

        /// <summary>Gets the stop call count.</summary>
        public int StopCalls { get; private set; }

        /// <summary>Gets the trigger call count.</summary>
        public int TriggerCalls { get; private set; }

        /// <summary>Gets the enqueued operation.</summary>
        public SyncOperation? Operation { get; private set; }

        /// <summary>Gets the exception to throw from start.</summary>
        public Exception? Error { get; init; }

        /// <inheritdoc />
        public IObservable<SyncState> SyncStates => throw new NotSupportedException();

        /// <inheritdoc />
        public IObservable<SyncOperationStatus> OperationStates => throw new NotSupportedException();

        /// <inheritdoc />
        public IObservable<OccasionallyConnectedFault> Faults => throw new NotSupportedException();

        /// <inheritdoc />
        public ValueTask<PublishReceipt> EnqueueOperationAsync(
            SyncOperation operation,
            CancellationToken cancellationToken)
        {
            EnqueueCalls++;
            Operation = operation;
            Tokens.Add(cancellationToken);
            return new(
                new PublishReceipt(
                    operation.OperationId,
                    operation.ClientSequence,
                    SyncOperationState.SavedLocally,
                    DateTimeOffset.UnixEpoch));
        }

        /// <inheritdoc />
        public ValueTask StartAsync(CancellationToken cancellationToken)
        {
            StartCalls++;
            Tokens.Add(cancellationToken);
            return Error is null ? ValueTask.CompletedTask : ValueTask.FromException(Error);
        }

        /// <inheritdoc />
        public ValueTask StopAsync(CancellationToken cancellationToken)
        {
            StopCalls++;
            Tokens.Add(cancellationToken);
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc />
        public ValueTask TriggerSyncAsync(CancellationToken cancellationToken)
        {
            TriggerCalls++;
            Tokens.Add(cancellationToken);
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
