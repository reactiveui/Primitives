// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reflection;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="SyncBatch"/>.</summary>
public sealed class SyncBatchTests
{
    /// <summary>Verifies operations are copied from caller-owned collections.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorCopiesOperations()
    {
        var operations = new List<SyncOperation> { CreateOperation() };
        var batch = new SyncBatch(Guid.NewGuid(), operations);
        operations.Add(CreateOperation());
        await Assert.That(batch.Operations).Count().IsEqualTo(1);
    }

    /// <summary>Verifies null operations are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The expected public constructor is unavailable.</exception>
    [Test]
    public async Task ConstructorRejectsNullOperations()
    {
        var constructor = typeof(SyncBatch).GetConstructor([typeof(Guid), typeof(IReadOnlyList<SyncOperation>)])
            ?? throw new InvalidOperationException("The synchronization batch constructor is unavailable.");
        var exception = await Assert.That(() => constructor.Invoke([Guid.NewGuid(), null])).ThrowsExactly<TargetInvocationException>();

        await Assert.That(exception?.InnerException).IsTypeOf<ArgumentNullException>();
        await Assert.That((exception?.InnerException as ArgumentNullException)?.ParamName).IsEqualTo("source");
    }

    /// <summary>Creates a representative synchronization operation.</summary>
    /// <returns>A synchronization operation.</returns>
    private static SyncOperation CreateOperation() => new()
    {
        OperationId = OperationId.New(),
        StreamId = new("sensor/temperature"),
        ClientSequence = 1,
        TimestampUtc = DateTimeOffset.UnixEpoch,
        Type = SyncOperationType.Append,
        Payload = new("reading", 1, "application/json", ReadOnlyMemory<byte>.Empty, "sha256-empty"),
        Policy = OperationPolicy.Default,
        Metadata = new Dictionary<string, string>(),
    };
}
