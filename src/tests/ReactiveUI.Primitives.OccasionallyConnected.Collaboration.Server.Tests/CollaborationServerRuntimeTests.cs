// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests;

/// <summary>Tests for <see cref="CollaborationServerRuntime"/> lifetime behavior.</summary>
public sealed class CollaborationServerRuntimeTests
{
    /// <summary>The bearer token accepted by the development credential store.</summary>
    private const string Token = "token-a";

    /// <summary>The tenant identifier configured on the trusted development credential.</summary>
    private const string Tenant = "tenant-a";

    /// <summary>The client identifier configured on the trusted development credential.</summary>
    private const string Client = "client-a";

    /// <summary>The invalid request limit used by validation ordering tests.</summary>
    private const int InvalidLimit = 0;

    /// <summary>The request byte limit used by payload coherence tests.</summary>
    private const int PayloadCoherenceRequestBytes = 1024;

    /// <summary>The payload byte limit that exceeds the request budget in coherence tests.</summary>
    private const int PayloadAboveRequestBytes = 2048;

    /// <summary>The invalid SQLite file content used by startup rollback tests.</summary>
    private const string CorruptSqliteContent = "not a sqlite database";

    /// <summary>Verifies cancellation is observed before the SQLite journal directory is created.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateAsyncHonorsCancellationBeforeDatabaseSideEffects()
    {
        using var lease = new DatabaseLease();
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        var startup = CollaborationServerRuntime.CreateAsync(CreateOptions(lease.Path), cancellation.Token).AsTask();
        var exception = await Assert.ThrowsExactlyAsync<TaskCanceledException>(() => startup);

        await Assert.That(startup.IsCanceled).IsTrue();
        await Assert.That(exception?.CancellationToken).IsEqualTo(cancellation.Token);
        await Assert.That(Directory.Exists(lease.Directory)).IsFalse();
    }

    /// <summary>Verifies a successfully created runtime disposes the composed endpoint and hub without deleting the journal.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeAsyncReleasesRuntimeResourcesAndPreservesJournalFile()
    {
        using var lease = new DatabaseLease(createDirectory: true);
        var runtime = await CollaborationServerRuntime.CreateAsync(CreateOptions(lease.Path), CancellationToken.None).ConfigureAwait(false);

        await runtime.DisposeAsync().ConfigureAwait(false);

        await Assert.That(File.Exists(lease.Path)).IsTrue();
    }

    /// <summary>Verifies runtime creation exposes the endpoint capabilities negotiated by clients.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateAsyncExposesNegotiatedEndpointCapabilities()
    {
        using var lease = new DatabaseLease(createDirectory: true);
        await using var runtime = await CollaborationServerRuntime.CreateAsync(CreateOptions(lease.Path), CancellationToken.None).ConfigureAwait(false);

        var features = runtime.Capabilities.Features;

        await Assert.That((features & RemoteTransportCapabilities.BatchPush) == RemoteTransportCapabilities.BatchPush).IsTrue();
        await Assert.That((features & RemoteTransportCapabilities.ReceiveAcknowledgements) == RemoteTransportCapabilities.ReceiveAcknowledgements).IsTrue();
        await Assert.That(
            (features & RemoteTransportCapabilities.AtomicApplyAndAcknowledge) == RemoteTransportCapabilities.AtomicApplyAndAcknowledge).IsTrue();
    }

    /// <summary>Verifies invalid options are rejected before the runtime creates the SQLite directory.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateAsyncRejectsInvalidOptionsBeforeDatabaseSideEffects()
    {
        using var lease = new DatabaseLease();
        var options = CreateOptions(lease.Path) with { MaximumRequestBytes = InvalidLimit };

        _ = await Assert.ThrowsExactlyAsync<ArgumentOutOfRangeException>(() =>
            CollaborationServerRuntime.CreateAsync(options, CancellationToken.None).AsTask());
        await Assert.That(Directory.Exists(lease.Directory)).IsFalse();
    }

    /// <summary>Verifies incoherent payload limits are rejected before opening the SQLite journal.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateAsyncRejectsPayloadLimitAboveRequestLimitBeforeDatabaseSideEffects()
    {
        using var lease = new DatabaseLease();
        var options = CreateOptions(lease.Path) with
        {
            MaximumRequestBytes = PayloadCoherenceRequestBytes,
            MaximumPayloadBytes = PayloadAboveRequestBytes,
        };

        _ = await Assert.ThrowsExactlyAsync<ArgumentOutOfRangeException>(() =>
            CollaborationServerRuntime.CreateAsync(options, CancellationToken.None).AsTask());
        await Assert.That(Directory.Exists(lease.Directory)).IsFalse();
    }

    /// <summary>Verifies startup rollback releases a corrupt SQLite file after the journal constructor fails.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateAsyncReleasesCorruptSqliteFileAfterStartupFailure()
    {
        using var lease = new DatabaseLease(createDirectory: true);
        await File.WriteAllTextAsync(lease.Path, CorruptSqliteContent).ConfigureAwait(false);

        var exception = await Assert.ThrowsExactlyAsync<SqliteException>(() =>
            CollaborationServerRuntime.CreateAsync(CreateOptions(lease.Path), CancellationToken.None).AsTask());

        await Assert.That(exception).IsNotNull();
        await Assert.That(File.Exists(lease.Path)).IsTrue();
        await using var stream = OpenExclusiveFile(lease.Path);
        await Assert.That(stream.Length).IsGreaterThan(0);
    }

    /// <summary>Opens a database file exclusively to prove failed startup left no live owner behind.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The exclusive file stream.</returns>
    private static FileStream OpenExclusiveFile(string path) =>
        new(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);

    /// <summary>Creates valid runtime options for a database path.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <returns>The configured options.</returns>
    private static CollaborationServerOptions CreateOptions(string databasePath) =>
        new() { ListenUri = new(CollaborationServerOptions.DefaultUrl), DatabasePath = databasePath, Credentials = [new(Token, Tenant, Client)] };

    /// <summary>Owns a temporary SQLite database path for runtime tests.</summary>
    private sealed class DatabaseLease : IDisposable
    {
        /// <summary>Initializes a new instance of the <see cref="DatabaseLease"/> class.</summary>
        /// <param name="createDirectory">Whether to create the directory immediately.</param>
        internal DatabaseLease(bool createDirectory = false)
        {
            Directory = OwnedTempDirectory.CreatePath("rxui-oc-runtime-");
            if (createDirectory)
            {
                _ = System.IO.Directory.CreateDirectory(Directory);
            }

            Path = System.IO.Path.Combine(Directory, "journal.db");
        }

        /// <summary>Gets the temporary directory containing the database file.</summary>
        internal string Directory { get; }

        /// <summary>Gets the leased database file path.</summary>
        internal string Path { get; }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
            if (!System.IO.Directory.Exists(Directory))
            {
                return;
            }

            System.IO.Directory.Delete(Directory, recursive: true);
        }
    }
}
