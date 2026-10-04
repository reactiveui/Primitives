// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text.Json;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem;

/// <summary>Provides stateless helpers for the filesystem journal.</summary>
internal static class FileSystemJournalHelpers
{
    /// <summary>Determines whether a terminal operation is safe to remove.</summary>
    /// <param name="state">The full state used to check leases and snapshot inclusion.</param>
    /// <param name="operation">The operation considered for removal.</param>
    /// <param name="cutoffUtc">The retention cutoff.</param>
    /// <returns><see langword="true"/> when the operation meets all compaction rules.</returns>
    internal static bool CanCompact(
        FileSystemLocalStoreAdapter.StoreState state,
        FileSystemLocalStoreAdapter.OperationState operation,
        DateTimeOffset cutoffUtc)
    {
        if (!operation.Terminal || operation.Status.ChangedAtUtc >= cutoffUtc)
        {
            return false;
        }

        foreach (var lease in state.Leases.Values)
        {
            if (lease.OperationIds.Contains(operation.Operation.OperationId.Value))
            {
                return false;
            }
        }

        return operation.Status.State is SyncOperationState.Rejected or SyncOperationState.DeadLettered
            || (operation.Status.State == SyncOperationState.Synchronized
                && state.IncludedOperations.Contains(operation.Operation.OperationId.Value));
    }

    /// <summary>Computes the journal record checksum.</summary>
    /// <param name="value">The serialized record.</param>
    /// <returns>The checksum bytes.</returns>
    internal static byte[] ComputeHash(byte[] value)
    {
#if NET5_0_OR_GREATER
        return SHA256.HashData(value);
#else
        using var algorithm = SHA256.Create();
        return algorithm.ComputeHash(value);
#endif
    }

    /// <summary>Flushes buffered journal data to the durable device.</summary>
    /// <param name="stream">The journal stream to flush.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void FlushToDisk(FileStream stream) => stream.Flush(flushToDisk: true);

    /// <summary>Writes a complete buffer using the asynchronous file APIs available on the target framework.</summary>
    /// <param name="stream">The destination journal stream.</param>
    /// <param name="buffer">The bytes to write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when the bytes have been written.</returns>
    internal static ValueTask WriteAsync(FileStream stream, byte[] buffer, CancellationToken cancellationToken)
    {
#if NETCOREAPP3_0_OR_GREATER
        return stream.WriteAsync(buffer.AsMemory(), cancellationToken);
#else
        return new(stream.WriteAsync(buffer, 0, buffer.Length, cancellationToken));
#endif
    }

    /// <summary>Disposes a file stream asynchronously when the target framework supports it.</summary>
    /// <param name="stream">The file stream to dispose.</param>
    /// <returns>A task that completes when the stream has been disposed.</returns>
    internal static ValueTask DisposeAsync(FileStream stream)
    {
#if NETCOREAPP3_0_OR_GREATER
        return stream.DisposeAsync();
#else
        stream.Dispose();
        return default;
#endif
    }

    /// <summary>Compares checksums without data-dependent early exit.</summary>
    /// <param name="left">The computed checksum.</param>
    /// <param name="right">The stored checksum.</param>
    /// <returns><see langword="true"/> when both byte sequences match.</returns>
    internal static bool FixedTimeEquals(byte[] left, byte[] right)
    {
        if (left.Length != right.Length)
        {
            return false;
        }

        var difference = 0;
        for (var index = 0; index < left.Length; index++)
        {
            difference |= left[index] ^ right[index];
        }

        return difference == 0;
    }

    /// <summary>Gets the durable state for a stream.</summary>
    /// <param name="state">The store state.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The stream state.</returns>
    /// <exception cref="InvalidOperationException">The stream has no durable subscription identity.</exception>
    internal static FileSystemLocalStoreAdapter.StreamState GetStream(
        FileSystemLocalStoreAdapter.StoreState state,
        StreamId streamId) =>
        state.Streams.TryGetValue(streamId.Value, out var stream)
            ? stream
            : throw new InvalidOperationException("The stream has no durable subscription identity.");

    /// <summary>Finds a durable operation or throws when it is missing.</summary>
    /// <param name="state">The store state.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The matching operation state.</returns>
    /// <exception cref="InvalidOperationException">The operation is not present in the durable store.</exception>
    internal static FileSystemLocalStoreAdapter.OperationState FindOperation(
        FileSystemLocalStoreAdapter.StoreState state,
        OperationId operationId) =>
        FindOperationOrNull(state, operationId)
            ?? throw new InvalidOperationException("The operation is not present in the durable store.");

    /// <summary>Finds an operation across the stored streams.</summary>
    /// <param name="state">The store state.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The matching operation, or <see langword="null"/> when it is absent.</returns>
    internal static FileSystemLocalStoreAdapter.OperationState? FindOperationOrNull(
        FileSystemLocalStoreAdapter.StoreState state,
        OperationId operationId)
    {
        foreach (var stream in state.Streams.Values)
        {
            foreach (var operation in stream.Operations.Values)
            {
                if (operation.Operation.OperationId == operationId)
                {
                    return operation;
                }
            }
        }

        return null;
    }

    /// <summary>Validates that a server result matches the complete leased batch.</summary>
    /// <param name="state">The store state.</param>
    /// <param name="lease">The active lease.</param>
    /// <param name="result">The server result.</param>
    /// <exception cref="InvalidOperationException">The server result does not match the leased operation batch.</exception>
    internal static void ValidateResult(
        FileSystemLocalStoreAdapter.StoreState state,
        FileSystemLocalStoreAdapter.LeaseState lease,
        RemoteSyncResult result)
    {
        if (result.Operations.Count != lease.OperationIds.Count)
        {
            throw new InvalidOperationException("The remote result does not exactly match the leased operation batch.");
        }

        var seenIds = new HashSet<Guid>();
        foreach (var decision in result.Operations)
        {
            var operationId = decision.OperationId.Value;
            if (!seenIds.Add(operationId)
                || !lease.OperationIds.Contains(operationId)
                || FindOperationOrNull(state, decision.OperationId) is null)
            {
                throw new InvalidOperationException("The remote result does not exactly match the leased operation batch.");
            }
        }
    }

    /// <summary>Replays complete, checksummed journal records and truncates only an incomplete tail.</summary>
    /// <param name="journal">The journal stream.</param>
    /// <param name="context">The generated JSON context.</param>
    /// <returns>The recovered store state.</returns>
    /// <exception cref="InvalidDataException">A complete journal record has an invalid header, checksum, or payload.</exception>
    /// <exception cref="IOException">A journal read or recovery truncation fails.</exception>
    internal static FileSystemLocalStoreAdapter.StoreState ReadJournal(FileStream journal, FileSystemJsonContext context)
    {
        _ = journal.Seek(0, SeekOrigin.Begin);
        var state = new FileSystemLocalStoreAdapter.StoreState();
        var position = 0L;
        var header = new byte[FileSystemLocalStoreAdapter.JournalHeaderBytes];
        while (ReadUpTo(journal, header) == header.Length)
        {
            var payloadLength = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(0, sizeof(int)));
            var checksumLength = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(sizeof(int), sizeof(int)));
            if (payloadLength is <= 0 or > FileSystemLocalStoreAdapter.MaximumRecordBytes
                || checksumLength != FileSystemLocalStoreAdapter.JournalChecksumBytes)
            {
                throw new InvalidDataException("The filesystem journal contains an invalid record header.");
            }

            var payload = new byte[payloadLength];
            var checksum = new byte[checksumLength];
            if (ReadUpTo(journal, payload) != payload.Length || ReadUpTo(journal, checksum) != checksum.Length)
            {
                break;
            }

            if (!FixedTimeEquals(ComputeHash(payload), checksum))
            {
                throw new InvalidDataException("The filesystem journal record checksum is invalid.");
            }

            state = DecodeRecord(payload, context, state, position != 0);
            position = journal.Position;
        }

        if (journal.Length != position)
        {
            journal.SetLength(position);
            journal.Flush(flushToDisk: true);
        }

        return state;
    }

    /// <summary>Decodes and validates one complete checksummed transaction.</summary>
    /// <param name="payload">The serialized record.</param>
    /// <param name="context">The generated JSON metadata.</param>
    /// <param name="state">The committed prefix.</param>
    /// <param name="hasSnapshot">Whether the prefix contains an initial snapshot.</param>
    /// <returns>The updated recovered state.</returns>
    /// <exception cref="InvalidDataException">The record has invalid contents.</exception>
    private static FileSystemLocalStoreAdapter.StoreState DecodeRecord(
        byte[] payload,
        FileSystemJsonContext context,
        FileSystemLocalStoreAdapter.StoreState state,
        bool hasSnapshot)
    {
        try
        {
            var record = JsonSerializer.Deserialize(payload, context.JournalRecord)
                ?? throw new InvalidDataException("The filesystem journal contains an invalid record.");
            FileSystemJournalDelta.Validate(record, hasSnapshot);
            ValidateRecordIdentity(state, record);
            if (record.State is { } snapshot)
            {
                FileSystemJournalDelta.ValidateSnapshotReplay(state, snapshot);
                return snapshot;
            }

            FileSystemJournalDelta.Apply(state, record.Delta!);
            return state;
        }
        catch (Exception error) when (error is JsonException or ArgumentException or InvalidOperationException or KeyNotFoundException or FormatException)
        {
            throw new InvalidDataException("The filesystem journal contains an invalid record.", error);
        }
    }

    /// <summary>Ensures later records cannot change an already bound store or client identity.</summary>
    /// <param name="state">The committed prefix.</param>
    /// <param name="record">The next validated record.</param>
    /// <exception cref="InvalidDataException">The next record changes a durable identity.</exception>
    private static void ValidateRecordIdentity(
        FileSystemLocalStoreAdapter.StoreState state,
        FileSystemLocalStoreAdapter.JournalRecord record)
    {
        var clientId = record.State is { } snapshot ? snapshot.ClientId : record.Delta!.ClientId;
        if (state.ClientId is not null && !string.Equals(state.ClientId, clientId, StringComparison.Ordinal))
        {
            throw new InvalidDataException("The filesystem journal changes the bound client identity.");
        }

        if (record.State is { } next && state.StoreIdentity is not null
            && !string.Equals(state.StoreIdentity, next.StoreIdentity, StringComparison.Ordinal))
        {
            throw new InvalidDataException("The filesystem journal changes the bound store identity.");
        }
    }

    /// <summary>Reads into a buffer until it is full or the stream reaches its end.</summary>
    /// <param name="stream">The source stream.</param>
    /// <param name="buffer">The destination buffer.</param>
    /// <returns>The number of bytes read.</returns>
    private static int ReadUpTo(Stream stream, byte[] buffer)
    {
        var totalRead = 0;
        while (totalRead < buffer.Length)
        {
            var read = stream.Read(buffer, totalRead, buffer.Length - totalRead);
            if (read == 0)
            {
                break;
            }

            totalRead += read;
        }

        return totalRead;
    }
}
