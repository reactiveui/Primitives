// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem;

/// <summary>Stores local commits in a checksummed, append-only journal.</summary>
[System.Diagnostics.DebuggerDisplay("FileSystemLocalStoreAdapter: {Capabilities}")]
public sealed partial class FileSystemLocalStoreAdapter : ILocalStoreAdapter
{
    /// <summary>The maximum serialized journal record size.</summary>
    internal const int MaximumRecordBytes = 64 * 1024 * 1024;

    /// <summary>The journal record header size in bytes.</summary>
    internal const int JournalHeaderBytes = sizeof(int) * 2;

    /// <summary>The SHA-256 checksum size in bytes.</summary>
    internal const int JournalChecksumBytes = 32;

    /// <summary>The current on-disk journal schema version.</summary>
    private const int CurrentFormatVersion = 1;

    /// <summary>The stream buffer size in bytes.</summary>
    private const int StreamBufferBytes = 4096;

    /// <summary>The serializer options used to create the generated JSON context.</summary>
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.General)
    {
        Converters = { new StreamIdConverter(), new PayloadEnvelopeConverter(), new LocalSnapshotConverter() },
    };

    /// <summary>The source-generated metadata used for journal serialization.</summary>
    private static readonly FileSystemJsonContext JsonContext = new(JsonOptions);

    /// <summary>The configured directory containing durable store files.</summary>
    private readonly string _directory;

    /// <summary>The append-only journal path.</summary>
    private readonly string _journalPath;

    /// <summary>The process ownership lock path.</summary>
    private readonly string _ownerPath;

    /// <summary>The optional journal checkpoint used by the friend test assembly.</summary>
    private readonly Action<FileSystemJournalCheckpoint>? _journalCheckpoint;

    /// <summary>Provides the current time for durable operation metadata and lease checks.</summary>
    private readonly TimeProvider _timeProvider;

    /// <summary>Serializes access to the journal and in-memory state.</summary>
    private readonly SemaphoreSlim _gate = new(1, 1);

    /// <summary>Completes when the adapter has finished disposing.</summary>
    private readonly TaskCompletionSource<bool> _disposeCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The open durable journal.</summary>
    private FileStream? _journal;

    /// <summary>The open process ownership lock.</summary>
    private FileStream? _owner;

    /// <summary>The latest state recovered from or appended to the journal.</summary>
    private StoreState _state = new();

    /// <summary>Whether initialization has completed successfully.</summary>
    private int _initialized;

    /// <summary>Whether this adapter has been disposed.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="FileSystemLocalStoreAdapter"/> class.</summary>
    /// <param name="directory">The configured store directory.</param>
    public FileSystemLocalStoreAdapter(string directory)
        : this(directory, TimeProvider.System, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="FileSystemLocalStoreAdapter"/> class with a time provider.</summary>
    /// <param name="directory">The configured store directory.</param>
    /// <param name="timeProvider">The time provider used for operation timestamps and lease expiry.</param>
    public FileSystemLocalStoreAdapter(string directory, TimeProvider timeProvider)
        : this(directory, timeProvider, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="FileSystemLocalStoreAdapter"/> class with an optional journal checkpoint callback.</summary>
    /// <param name="directory">The configured store directory.</param>
    /// <param name="journalCheckpoint">The optional journal checkpoint callback.</param>
    internal FileSystemLocalStoreAdapter(
        string directory,
        Action<FileSystemJournalCheckpoint>? journalCheckpoint)
        : this(directory, TimeProvider.System, journalCheckpoint)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="FileSystemLocalStoreAdapter"/> class with a time provider and optional journal checkpoint callback.</summary>
    /// <param name="directory">The configured store directory.</param>
    /// <param name="timeProvider">The time provider used for operation timestamps and lease expiry.</param>
    /// <param name="journalCheckpoint">The optional journal checkpoint callback.</param>
    /// <exception cref="ArgumentException"><paramref name="directory"/> is null, empty, or whitespace.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="timeProvider"/> is null.</exception>
    internal FileSystemLocalStoreAdapter(
        string directory,
        TimeProvider timeProvider,
        Action<FileSystemJournalCheckpoint>? journalCheckpoint)
    {
#if NET5_0_OR_GREATER
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);
#else
        ArgumentExceptionHelper.ThrowIfNullOrWhiteSpace(directory);
#endif
        ArgumentExceptionHelper.ThrowIfNull(timeProvider);

        _directory = Path.GetFullPath(directory);
        _journalPath = Path.Combine(_directory, "journal.log");
        _ownerPath = Path.Combine(_directory, "owner.lock");
        _timeProvider = timeProvider;
        _journalCheckpoint = journalCheckpoint;
    }

    /// <inheritdoc/>
    public LocalStoreCapabilities Capabilities =>
        LocalStoreCapabilities.AtomicLocalCommit
        | LocalStoreCapabilities.AtomicRemoteApply
        | LocalStoreCapabilities.DurableInbox
        | LocalStoreCapabilities.LeasedOutbox
        | LocalStoreCapabilities.DurableLocalCommit
        | LocalStoreCapabilities.ClientIdentityBinding;

    /// <inheritdoc/>
    public async ValueTask InitializeAsync(LocalStoreInitialization initialization, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(initialization);

        if (initialization.RequireAuthenticatedEncryptionAtRest)
        {
            throw new NotSupportedException("Authenticated encryption at rest is not implemented by the filesystem adapter.");
        }

        if (initialization.RequiredSchemaVersion > CurrentFormatVersion)
        {
            throw new InvalidOperationException(
                $"The filesystem store supports schema version {CurrentFormatVersion}, not {initialization.RequiredSchemaVersion}.");
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            if (Volatile.Read(ref _initialized) != 0)
            {
                return;
            }

            await InitializeStoreUnderLockAsync(initialization, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <summary>Opens, recovers, binds, and initializes the store while the gate is held.</summary>
    /// <param name="initialization">The requested store and client identities.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when the store is ready.</returns>
    private async ValueTask InitializeStoreUnderLockAsync(
        LocalStoreInitialization initialization,
        CancellationToken cancellationToken)
    {
        _ = Directory.CreateDirectory(_directory);
        try
        {
            _owner = new(_ownerPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            _journal = new(
                _journalPath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.Read,
                StreamBufferBytes,
                FileOptions.SequentialScan);
            _state = FileSystemJournalHelpers.ReadJournal(_journal, JsonContext);
            ValidateStoreIdentity(initialization);
            _state.StoreIdentity = initialization.StoreIdentity;
            _state.ClientId = initialization.ClientId;
            if (initialization.Outbox is not null)
            {
                _state.Outbox = initialization.Outbox;
            }

            await PersistAsync(cancellationToken).ConfigureAwait(false);
            _ = Interlocked.Exchange(ref _initialized, 1);
        }
        catch
        {
            if (_journal is not null)
            {
                await FileSystemJournalHelpers.DisposeAsync(_journal).ConfigureAwait(false);
            }

            _journal = null;
            if (_owner is not null)
            {
                await FileSystemJournalHelpers.DisposeAsync(_owner).ConfigureAwait(false);
            }

            _owner = null;
            throw;
        }
    }

    /// <summary>Ensures recovered identities match the requested initialization.</summary>
    /// <param name="initialization">The requested store and client identities.</param>
    /// <exception cref="InvalidOperationException">The store is already bound to different identities.</exception>
    private void ValidateStoreIdentity(LocalStoreInitialization initialization)
    {
        if (_state.StoreIdentity is not null
            && !string.Equals(_state.StoreIdentity, initialization.StoreIdentity, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The filesystem store is bound to another store identity.");
        }

        if (_state.ClientId is not null
            && !string.Equals(_state.ClientId, initialization.ClientId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The filesystem store is bound to another client identity.");
        }
    }

    /// <summary>Serializes, appends, and flushes one complete journal record.</summary>
    /// <param name="record">The record to append.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when the record is durably flushed.</returns>
    /// <exception cref="AggregateException">Rollback also fails after the append fails.</exception>
    /// <exception cref="InvalidOperationException">The serialized record exceeds the configured size limit.</exception>
    /// <exception cref="IOException">A journal read or write fails.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before its durable commit.</exception>
    private async ValueTask AppendAsync(JournalRecord record, CancellationToken cancellationToken)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(record, JsonContext.JournalRecord);
        if (payload.Length > MaximumRecordBytes)
        {
            throw new InvalidOperationException("The durable journal record exceeds the configured size limit.");
        }

        var checksum = FileSystemJournalHelpers.ComputeHash(payload);
        var header = new byte[JournalHeaderBytes];
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(0, sizeof(int)), payload.Length);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(sizeof(int), sizeof(int)), checksum.Length);
        var journal = _journal!;
        var recordStart = journal.Seek(0, SeekOrigin.End);
        try
        {
            await FileSystemJournalHelpers.WriteAsync(journal, header, cancellationToken).ConfigureAwait(false);
            _journalCheckpoint?.Invoke(FileSystemJournalCheckpoint.AfterAppendHeader);
            await FileSystemJournalHelpers.WriteAsync(journal, payload, cancellationToken).ConfigureAwait(false);
            await FileSystemJournalHelpers.WriteAsync(journal, checksum, cancellationToken).ConfigureAwait(false);
            await journal.FlushAsync(cancellationToken).ConfigureAwait(false);
            FileSystemJournalHelpers.FlushToDisk(journal);
        }
        catch (Exception appendError)
        {
            try
            {
                journal.SetLength(recordStart);
                _ = journal.Seek(recordStart, SeekOrigin.Begin);
                FileSystemJournalHelpers.FlushToDisk(journal);
            }
            catch (Exception rollbackError)
            {
                Volatile.Write(ref _initialized, 0);
                await FileSystemJournalHelpers.DisposeAsync(journal).ConfigureAwait(false);
                _journal = null;
                throw new AggregateException("The journal append failed and its partial record could not be rolled back.", appendError, rollbackError);
            }

            throw;
        }
    }

    /// <summary>Opens the journal for serialized reads and writes.</summary>
    /// <returns>The open journal stream.</returns>
    private FileStream OpenJournal() =>
        new(_journalPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, StreamBufferBytes, FileOptions.SequentialScan);

    /// <summary>Atomically replaces the journal with a compacted state record.</summary>
    /// <param name="next">The state to persist.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The number of journal bytes reclaimed.</returns>
    /// <exception cref="InvalidOperationException">The compacted journal record exceeds the configured size limit.</exception>
    /// <exception cref="IOException">A journal read, write, or replacement fails.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before journal replacement.</exception>
    private async ValueTask<long> RewriteJournalAsync(StoreState next, CancellationToken cancellationToken)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(new(next), JsonContext.JournalRecord);
        if (payload.Length > MaximumRecordBytes)
        {
            throw new InvalidOperationException("The compacted journal record exceeds the configured size limit.");
        }

        var checksum = FileSystemJournalHelpers.ComputeHash(payload);
        var header = new byte[JournalHeaderBytes];
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(0, sizeof(int)), payload.Length);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(sizeof(int), sizeof(int)), checksum.Length);
        var temporaryPath = $"{_journalPath}.{Guid.NewGuid():N}.compact";
        try
        {
            var temporary = new FileStream(
                temporaryPath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                StreamBufferBytes,
                FileOptions.Asynchronous | FileOptions.WriteThrough);
            try
            {
                await FileSystemJournalHelpers.WriteAsync(temporary, header, cancellationToken).ConfigureAwait(false);
                await FileSystemJournalHelpers.WriteAsync(temporary, payload, cancellationToken).ConfigureAwait(false);
                await FileSystemJournalHelpers.WriteAsync(temporary, checksum, cancellationToken).ConfigureAwait(false);
                await temporary.FlushAsync(cancellationToken).ConfigureAwait(false);
                FileSystemJournalHelpers.FlushToDisk(temporary);
            }
            finally
            {
                await FileSystemJournalHelpers.DisposeAsync(temporary).ConfigureAwait(false);
            }

            cancellationToken.ThrowIfCancellationRequested();
            _journalCheckpoint?.Invoke(FileSystemJournalCheckpoint.BeforeCompactionJournalReplace);
            var oldLength = _journal!.Length;
            await FileSystemJournalHelpers.DisposeAsync(_journal).ConfigureAwait(false);
            _journal = null;
            try
            {
                File.Replace(temporaryPath, _journalPath, null);
            }
            catch
            {
                _journal = OpenJournal();
                throw;
            }

            _journalCheckpoint?.Invoke(FileSystemJournalCheckpoint.AfterCompactionJournalReplace);
            _state = next;
            _journal = OpenJournal();
            return Math.Max(0, oldLength - _journal.Length);
        }
        finally
        {
            if (File.Exists(temporaryPath))
            {
                File.Delete(temporaryPath);
            }
        }
    }

    /// <summary>Appends the current state to the journal.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when the record is durably flushed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ValueTask PersistAsync(CancellationToken cancellationToken) => AppendAsync(new(_state), cancellationToken);

    /// <summary>Appends a new state and publishes it after the durable write completes.</summary>
    /// <param name="next">The state to persist.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when the new state is durable.</returns>
    private async ValueTask PersistStateAsync(StoreState next, CancellationToken cancellationToken)
    {
        await AppendAsync(new(next), cancellationToken).ConfigureAwait(false);
        _state = next;
    }

    /// <summary>Gets an active lease or throws when it is missing or expired.</summary>
    /// <param name="state">The store state.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <returns>The matching lease state.</returns>
    /// <exception cref="InvalidOperationException">The lease is missing or expired.</exception>
    private LeaseState GetLease(StoreState state, Guid leaseId)
    {
        if (!state.Leases.TryGetValue(leaseId, out var lease) || lease.ExpiresAtUtc <= _timeProvider.GetUtcNow())
        {
            throw new InvalidOperationException("The lease is missing or expired.");
        }

        return lease;
    }

    /// <summary>Creates the durable snapshot represented by a mutation.</summary>
    /// <param name="mutation">The snapshot mutation.</param>
    /// <param name="cursor">The server cursor to associate with the snapshot.</param>
    /// <param name="revision">The committed revision.</param>
    /// <returns>The resulting snapshot.</returns>
    private LocalSnapshot CreateSnapshot(SnapshotMutation mutation, string? cursor, long revision) =>
        new(mutation.StreamId, mutation.FormatVersion, cursor, mutation.State, revision, _timeProvider.GetUtcNow()) { AuthoritativeState = mutation.AuthoritativeState, };

    /// <summary>Checks that this adapter is open and initialized.</summary>
    /// <exception cref="InvalidOperationException">The adapter has not been initialized.</exception>
    /// <exception cref="ObjectDisposedException">The adapter has been disposed.</exception>
    private void EnsureInitialized()
    {
        ThrowIfDisposed();
        if (Volatile.Read(ref _initialized) == 0 || _journal is null)
        {
            throw new InvalidOperationException("The filesystem store must be initialized before use.");
        }
    }

    /// <summary>Throws when this adapter has been disposed.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposed() => ObjectDisposedExceptionHelper.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    /// <summary>Contains the complete durable state represented by one journal record.</summary>
    internal sealed class StoreState
    {
        /// <summary>Initializes a new instance of the <see cref="StoreState"/> class.</summary>
        public StoreState()
        {
        }

        /// <summary>Initializes a new instance of the <see cref="StoreState"/> class by copying an existing state.</summary>
        /// <param name="other">The source state.</param>
        public StoreState(StoreState other)
        {
            StoreIdentity = other.StoreIdentity;
            ClientId = other.ClientId;
            Outbox = other.Outbox;
            foreach (var pair in other.Streams)
            {
                Streams.Add(pair.Key, new(pair.Value));
            }

            foreach (var pair in other.Leases)
            {
                Leases.Add(pair.Key, new LeaseState { LeaseId = pair.Value.LeaseId, ExpiresAtUtc = pair.Value.ExpiresAtUtc, OperationIds = new(pair.Value.OperationIds), });
            }

            Inbox = new(other.Inbox);
            IncludedOperations = new(other.IncludedOperations);
        }

        /// <summary>Gets or sets the identity of the store instance.</summary>
        public string? StoreIdentity { get; set; }

        /// <summary>Gets or sets the bound client identity.</summary>
        public string? ClientId { get; set; }

        /// <summary>Gets or sets the configured outbox limits.</summary>
        public OutboxOptions? Outbox { get; set; }

        /// <summary>Gets durable state indexed by stream identifier.</summary>
        public Dictionary<string, StreamState> Streams { get; init; } = [];

        /// <summary>Gets active outbox leases indexed by lease identifier.</summary>
        public Dictionary<Guid, LeaseState> Leases { get; init; } = [];

        /// <summary>Gets durably applied remote event identities.</summary>
        public HashSet<string> Inbox { get; init; } = [];

        /// <summary>Gets local operations already included in authoritative snapshots.</summary>
        public HashSet<Guid> IncludedOperations { get; init; } = [];
    }

    /// <summary>Contains durable state associated with one logical stream.</summary>
    internal sealed class StreamState
    {
        /// <summary>Initializes a new instance of the <see cref="StreamState"/> class.</summary>
        public StreamState()
        {
        }

        /// <summary>Initializes a new instance of the <see cref="StreamState"/> class by copying an existing state.</summary>
        /// <param name="other">The source stream state.</param>
        public StreamState(StreamState other)
        {
            NextSequence = other.NextSequence;
            Snapshot = other.Snapshot;
            Cursor = other.Cursor;
            SubscriptionId = other.SubscriptionId;
            foreach (var pair in other.Operations)
            {
                Operations.Add(pair.Key, new(pair.Value));
            }

            DeadLetters = new(other.DeadLetters);
        }

        /// <summary>Gets or sets the persisted logical subscription identity.</summary>
        public SubscriptionId? SubscriptionId { get; set; }

        /// <summary>Gets or sets the next client sequence assigned to this stream.</summary>
        public long NextSequence { get; set; }

        /// <summary>Gets or sets the latest locally committed snapshot.</summary>
        public LocalSnapshot? Snapshot { get; set; }

        /// <summary>Gets or sets the latest durably applied remote cursor.</summary>
        public string? Cursor { get; set; }

        /// <summary>Gets operations retained in the durable outbox.</summary>
        public Dictionary<Guid, OperationState> Operations { get; init; } = [];

        /// <summary>Gets terminal operations retained in the dead-letter store.</summary>
        public List<DeadLetterRecord> DeadLetters { get; init; } = [];
    }

    /// <summary>Tracks local status and lease metadata for one operation.</summary>
    internal sealed class OperationState
    {
        /// <summary>Initializes a new instance of the <see cref="OperationState"/> class.</summary>
        public OperationState()
        {
        }

        /// <summary>Initializes a new instance of the <see cref="OperationState"/> class by copying an existing state.</summary>
        /// <param name="other">The source operation state.</param>
        public OperationState(OperationState other)
        {
            Operation = other.Operation;
            Status = other.Status;
            RetryState = other.RetryState;
            LeaseId = other.LeaseId;
            LeaseExpiry = other.LeaseExpiry;
            Terminal = other.Terminal;
        }

        /// <summary>Gets or sets the immutable operation payload and metadata.</summary>
        public SyncOperation Operation { get; set; } = null!;

        /// <summary>Gets or sets the latest local operation status.</summary>
        public SyncOperationStatus Status { get; set; } = null!;

        /// <summary>Gets or sets the persisted retry schedule, when one exists.</summary>
        public RetryState? RetryState { get; set; }

        /// <summary>Gets or sets the active lease identifier.</summary>
        public Guid? LeaseId { get; set; }

        /// <summary>Gets or sets the active lease expiry time.</summary>
        public DateTimeOffset? LeaseExpiry { get; set; }

        /// <summary>Gets or sets whether this operation reached a terminal state.</summary>
        public bool Terminal { get; set; }
    }

    /// <summary>Records durable ownership and expiry for a leased operation batch.</summary>
    internal sealed class LeaseState
    {
        /// <summary>Gets or sets the lease identifier.</summary>
        public Guid LeaseId { get; set; }

        /// <summary>Gets or sets the lease expiry time.</summary>
        public DateTimeOffset ExpiresAtUtc { get; set; }

        /// <summary>Gets or sets the operation identifiers owned by the lease.</summary>
        public List<Guid> OperationIds { get; init; } = [];
    }

    /// <summary>Serializes payload envelopes using the stable journal wire shape.</summary>
    private sealed class PayloadEnvelopeConverter : JsonConverter<PayloadEnvelope>
    {
        public override PayloadEnvelope Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            using var document = JsonDocument.ParseValue(ref reader);
            var root = document.RootElement;
            return new(
                root.GetProperty("ContractId").GetString()!,
                root.GetProperty("SchemaVersion").GetInt32(),
                root.GetProperty("ContentType").GetString()!,
                Convert.FromBase64String(root.GetProperty("Payload").GetString()!),
                root.GetProperty("PayloadHash").GetString()!);
        }

        public override void Write(Utf8JsonWriter writer, PayloadEnvelope value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteString("ContractId", value.ContractId);
            writer.WriteNumber("SchemaVersion", value.SchemaVersion);
            writer.WriteString("ContentType", value.ContentType);
            writer.WriteString("Payload", Convert.ToBase64String(value.Payload.ToArray()));
            writer.WriteString("PayloadHash", value.PayloadHash);
            writer.WriteEndObject();
        }
    }

    /// <summary>Preserves the object-shaped stream identifier stored by the journal.</summary>
    private sealed class StreamIdConverter : JsonConverter<StreamId>
    {
        /// <inheritdoc/>
        public override StreamId Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            using var document = JsonDocument.ParseValue(ref reader);
            return new(document.RootElement.GetProperty(nameof(StreamId.Value)).GetString()!);
        }

        /// <inheritdoc/>
        public override void Write(Utf8JsonWriter writer, StreamId value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteString(nameof(StreamId.Value), value.Value);
            writer.WriteEndObject();
        }
    }

    /// <summary>Serializes snapshots while preserving their public value shape.</summary>
    private sealed class LocalSnapshotConverter : JsonConverter<LocalSnapshot>
    {
        public override LocalSnapshot Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            using var document = JsonDocument.ParseValue(ref reader);
            var root = document.RootElement;
            var snapshot = new LocalSnapshot(
                new StreamId(root.GetProperty(nameof(StreamId)).GetProperty("Value").GetString()!),
                root.GetProperty("FormatVersion").GetInt32(),
                root.GetProperty("ServerCursor").GetString(),
                JsonSerializer.Deserialize(root.GetProperty("State"), JsonContext.PayloadEnvelope)!,
                root.GetProperty("Revision").GetInt64(),
                root.GetProperty("SavedAtUtc").GetDateTimeOffset());
            if (root.TryGetProperty("AuthoritativeState", out var authoritative) && authoritative.ValueKind != JsonValueKind.Null)
            {
                snapshot = snapshot with
                {
                    AuthoritativeState = JsonSerializer.Deserialize(authoritative, JsonContext.PayloadEnvelope)
                };
            }

            return snapshot;
        }

        public override void Write(Utf8JsonWriter writer, LocalSnapshot value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(StreamId));
            writer.WriteStartObject();
            writer.WriteString("Value", value.StreamId.Value);
            writer.WriteEndObject();
            writer.WriteNumber("FormatVersion", value.FormatVersion);
            writer.WriteString("ServerCursor", value.ServerCursor);
            writer.WritePropertyName("State");
            JsonSerializer.Serialize(writer, value.State, JsonContext.PayloadEnvelope);
            writer.WriteNumber("Revision", value.Revision);
            writer.WriteString("SavedAtUtc", value.SavedAtUtc);
            writer.WritePropertyName("AuthoritativeState");
            if (value.AuthoritativeState is null)
            {
                writer.WriteNullValue();
            }
            else
            {
                JsonSerializer.Serialize(writer, value.AuthoritativeState, JsonContext.PayloadEnvelope);
            }

            writer.WriteEndObject();
        }
    }

    /// <summary>Represents one complete journal state record.</summary>
    /// <param name="State">The persisted store state.</param>
    internal sealed record JournalRecord(StoreState State);
}
