// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Exact logical byte accounting tests for <see cref="SnapshotRecoveryValidator"/>.</summary>
public sealed partial class SnapshotRecoveryValidatorTests
{
    /// <summary>The exact logical bytes for a non-recovered result with no reason and empty dispositions.</summary>
    private const int NonRecoveredEmptyDispositionsHeaderBytes = 8;

    /// <summary>One byte below the exact non-recovered empty-dispositions result size.</summary>
    private const int BelowNonRecoveredEmptyDispositionsHeaderBytes = 7;

    /// <summary>Verifies a request accepts an exact logical size that includes the pending-operation array header.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateAcceptsRequestAtExactLogicalSize()
    {
        var initialRequest = CreateRequest([]);
        var exactLogicalBytes = GetRequestLogicalBytes(initialRequest);
        var request = initialRequest with { MaximumResponseBytes = exactLogicalBytes };
        var limits = new SnapshotRecoveryLimits { MaximumLogicalBytes = exactLogicalBytes };

        SnapshotRecoveryValidator.Validate(request, limits);

        await Assert.That(exactLogicalBytes).IsEqualTo(GetRequestHeaderLogicalBytes(request) + Int32LogicalBytes);
    }

    /// <summary>Verifies request logical size rejects one byte below the pending-operation array header size.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsRequestBelowExactLogicalSize()
    {
        var initialRequest = CreateRequest([]);
        var exactLogicalBytes = GetRequestLogicalBytes(initialRequest);
        var limits = new SnapshotRecoveryLimits { MaximumLogicalBytes = exactLogicalBytes - FirstSequence };
        var request = initialRequest with { MaximumResponseBytes = limits.MaximumLogicalBytes };

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, limits))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies non-recovered response accounting accepts the exact status plus empty-dispositions header size.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateAcceptsNonRecoveredResultAtExactLogicalSize()
    {
        var result = new RemoteSnapshotRecoveryResult { Status = RemoteSnapshotRecoveryStatus.RetentionExpired };
        var exactLogicalBytes = GetNonRecoveredResultLogicalBytes(result);
        var request = CreateRequest([]) with { MaximumResponseBytes = exactLogicalBytes };

        SnapshotRecoveryValidator.Validate(request, result, new());

        await Assert.That(exactLogicalBytes).IsEqualTo(Int32LogicalBytes + Int32LogicalBytes);
    }

    /// <summary>Verifies non-recovered response accounting rejects one byte below the status plus empty-dispositions header size.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsNonRecoveredResultBelowExactLogicalSize()
    {
        var result = new RemoteSnapshotRecoveryResult { Status = RemoteSnapshotRecoveryStatus.RetentionExpired };
        var request = CreateRequest([]) with { MaximumResponseBytes = GetNonRecoveredResultLogicalBytes(result) - FirstSequence };

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, result, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies non-recovered response accounting accepts a literal status plus empty-dispositions header size.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateAcceptsNonRecoveredResultAtLiteralEmptyDispositionHeaderSize()
    {
        var result = new RemoteSnapshotRecoveryResult { Status = RemoteSnapshotRecoveryStatus.RetentionExpired };
        var request = CreateRequest([]) with { MaximumResponseBytes = NonRecoveredEmptyDispositionsHeaderBytes };

        SnapshotRecoveryValidator.Validate(request, result, new());

        await Assert.That(request.MaximumResponseBytes).IsEqualTo(NonRecoveredEmptyDispositionsHeaderBytes);
    }

    /// <summary>Verifies non-recovered response accounting rejects one byte below status plus empty-dispositions header size.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsNonRecoveredResultBelowLiteralEmptyDispositionHeaderSize()
    {
        var result = new RemoteSnapshotRecoveryResult { Status = RemoteSnapshotRecoveryStatus.RetentionExpired };
        var request = CreateRequest([]) with { MaximumResponseBytes = BelowNonRecoveredEmptyDispositionsHeaderBytes };

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, result, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies local mutation accounting accepts the exact size including the local mutation header.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateAcceptsLocalMutationAtExactLogicalSize()
    {
        var request = CreateRequest([]);
        var mutation = CreateMutation(request, CreateCheckpoint(request), []);
        var recovered = CreateRecoveredStream(request.SubscriptionId, []);
        var exactLogicalBytes = GetLocalMutationLogicalBytes(mutation);
        var limits = new SnapshotRecoveryLimits { MaximumLogicalBytes = exactLogicalBytes };

        SnapshotRecoveryValidator.Validate(mutation, recovered, limits);

        await Assert.That(exactLogicalBytes).IsEqualTo(GetLocalMutationHeaderLogicalBytes(mutation) + GetLocalMutationBodyLogicalBytes(mutation));
    }

    /// <summary>Verifies local mutation accounting rejects one byte below the local mutation header-inclusive size.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsLocalMutationBelowExactLogicalSize()
    {
        var request = CreateRequest([]);
        var mutation = CreateMutation(request, CreateCheckpoint(request), []);
        var recovered = CreateRecoveredStream(request.SubscriptionId, []);
        var limits = new SnapshotRecoveryLimits { MaximumLogicalBytes = GetLocalMutationLogicalBytes(mutation) - FirstSequence };

        await Assert.That(() => SnapshotRecoveryValidator.Validate(mutation, recovered, limits))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Gets the correct request logical byte count for fixtures without pending operations.</summary>
    /// <param name="request">The request fixture.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetRequestLogicalBytes(RemoteSnapshotRecoveryRequest request) =>
        GetRequestHeaderLogicalBytes(request) + Int32LogicalBytes;

    /// <summary>Gets the request header logical byte count.</summary>
    /// <param name="request">The request fixture.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetRequestHeaderLogicalBytes(RemoteSnapshotRecoveryRequest request) =>
        Utf8Bytes(request.StreamId.Value)
        + GuidLogicalBytes
        + OptionalUtf8Bytes(request.ExpiredCursor)
        + Utf8Bytes(request.ClientStateContractId)
        + Int32LogicalBytes
        + Int32LogicalBytes
        + Int64LogicalBytes;

    /// <summary>Gets the correct non-recovered response logical byte count.</summary>
    /// <param name="result">The non-recovered result fixture.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetNonRecoveredResultLogicalBytes(RemoteSnapshotRecoveryResult result) =>
        Int32LogicalBytes + OptionalUtf8Bytes(result.ReasonCode) + Int32LogicalBytes;

    /// <summary>Gets the correct local mutation logical byte count for fixtures without pending operations.</summary>
    /// <param name="mutation">The local mutation fixture.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetLocalMutationLogicalBytes(LocalSnapshotRecoveryMutation mutation) =>
        GetLocalMutationHeaderLogicalBytes(mutation) + GetLocalMutationBodyLogicalBytes(mutation);

    /// <summary>Gets the local mutation header logical byte count.</summary>
    /// <param name="mutation">The local mutation fixture.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetLocalMutationHeaderLogicalBytes(LocalSnapshotRecoveryMutation mutation) =>
        Utf8Bytes(mutation.StreamId.Value)
        + GuidLogicalBytes
        + Int64LogicalBytes
        + OptionalUtf8Bytes(mutation.ExpectedPreviousCursor)
        + Int32LogicalBytes;

    /// <summary>Gets the current local mutation body logical byte count for fixtures without pending operations.</summary>
    /// <param name="mutation">The local mutation fixture.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetLocalMutationBodyLogicalBytes(LocalSnapshotRecoveryMutation mutation) =>
        GetCheckpointLogicalBytes(mutation.Checkpoint)
        + GetPayloadLogicalBytes(mutation.OptimisticState)
        + Int32LogicalBytes;

    /// <summary>Gets the checkpoint logical byte count.</summary>
    /// <param name="checkpoint">The checkpoint fixture.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetCheckpointLogicalBytes(RemoteSnapshotCheckpoint checkpoint) =>
        Utf8Bytes(checkpoint.StreamId.Value)
        + GuidLogicalBytes
        + OptionalUtf8Bytes(checkpoint.FrontierCursor)
        + Utf8Bytes(checkpoint.ServerVersion)
        + Int32LogicalBytes
        + DateTimeOffsetLogicalBytes
        + GetPayloadLogicalBytes(checkpoint.ClientState);

    /// <summary>Gets the payload logical byte count.</summary>
    /// <param name="payload">The payload fixture.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetPayloadLogicalBytes(PayloadEnvelope payload) =>
        Int32LogicalBytes
        + Int64LogicalBytes
        + Utf8Bytes(payload.ContractId)
        + Utf8Bytes(payload.ContentType)
        + Utf8Bytes(payload.PayloadHash)
        + payload.PayloadLength;

    /// <summary>Gets an optional protocol string's UTF-8 byte count.</summary>
    /// <param name="value">The optional string.</param>
    /// <returns>The UTF-8 byte count, or zero for null.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int OptionalUtf8Bytes(string? value) =>
        value is null ? 0 : Utf8Bytes(value);

    /// <summary>Gets a required protocol string's UTF-8 byte count.</summary>
    /// <param name="value">The string.</param>
    /// <returns>The UTF-8 byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Utf8Bytes(string value) =>
        System.Text.Encoding.UTF8.GetByteCount(value);
}
