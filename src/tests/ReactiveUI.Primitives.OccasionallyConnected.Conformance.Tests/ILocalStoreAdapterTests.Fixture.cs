// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Extensions.Time.Testing;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.BliteDb;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem;
#if NET10_0_OR_GREATER
using ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB.Tests;
#endif
using ReactiveUI.Primitives.OccasionallyConnected.Storage.LiteDb;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Testing;

namespace ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests;

/// <content>Composed provider fixtures for public storage conformance.</content>
public sealed partial class ILocalStoreAdapterTests
{
    /// <summary>The filesystem provider selector.</summary>
    private const int FileSystemProvider = 2;

    /// <summary>The LiteDB provider selector.</summary>
    private const int LiteDbProvider = 3;

    /// <summary>The BLite provider selector.</summary>
    private const int BliteDbProvider = 4;

#if NET10_0_OR_GREATER
    /// <summary>The browser interop provider selector.</summary>
    private const int IndexedDbProvider = 5;
#endif

    /// <summary>The encrypted SQLite provider selector.</summary>
    private const int EncryptedSqliteProvider = 6;

    /// <summary>The AES-256 fixture key length.</summary>
    private const int EncryptionKeyBytes = 32;

    /// <summary>The bounded in-memory fixture record capacity.</summary>
    private const int MemoryRecords = 128;

    /// <summary>The bounded in-memory fixture encoded byte capacity.</summary>
    private const int MemoryBytes = 64 * 1024;

    /// <summary>Owns a fresh native directory or an IndexedDB interop contract fixture.</summary>
    private sealed class StoreFixture : IAsyncDisposable
    {
        /// <summary>The bounded wait for terminated process handles to close.</summary>
        private const int CleanupAttempts = 20;

        /// <summary>The interval between Windows sharing-violation retries.</summary>
        private static readonly TimeSpan CleanupPoll = TimeSpan.FromMilliseconds(50);

        /// <summary>The selected provider.</summary>
        private readonly int _provider;

        /// <summary>The owned physical directory.</summary>
        private readonly DirectoryInfo _directory;

        /// <summary>Whether this fixture deletes its temporary directory.</summary>
        private readonly bool _ownsDirectory;

#if NET10_0_OR_GREATER
        /// <summary>The browser interop contract backend.</summary>
        private readonly FakeJsRuntime _runtime = new();
#endif

        /// <summary>Initializes a new instance of the <see cref="StoreFixture"/> class.</summary>
        /// <param name="provider">The provider selector.</param>
        internal StoreFixture(int provider)
            : this(provider, PhysicalTempDirectory.Create("oc-store-conformance-"), true)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="StoreFixture"/> class at an existing location.</summary>
        /// <param name="provider">The provider selector.</param>
        /// <param name="directory">The durable directory.</param>
        /// <param name="ownsDirectory">Whether disposal removes the directory.</param>
        internal StoreFixture(int provider, DirectoryInfo directory, bool ownsDirectory)
        {
            _provider = provider;
            _directory = directory;
            _ownsDirectory = ownsDirectory;
        }

        /// <summary>Gets the fixture's physical directory.</summary>
        internal string DirectoryPath => _directory.FullName;

        /// <summary>Gets the shared clock retained when the store reopens.</summary>
        internal FakeTimeProvider Clock { get; } = new(DateTimeOffset.UnixEpoch);

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            if (!_ownsDirectory)
            {
                return;
            }

            for (var attempt = 0; attempt <= CleanupAttempts; attempt++)
            {
                try
                {
                    _directory.Delete(recursive: true);
                    return;
                }
                catch (IOException error) when (
                    OperatingSystem.IsWindows()
                    && error.HResult is unchecked((int)0x80070020) or unchecked((int)0x80070021)
                    && attempt < CleanupAttempts)
                {
                    await Task.Delay(CleanupPoll);
                }
            }
        }

#if NET10_0_OR_GREATER
        /// <summary>Replaces the document returned by the browser interop fixture.</summary>
        /// <param name="json">The corrupted persisted document.</param>
        internal void CorruptBrowserDocument(string json) => _runtime.Module.LoadedJsonOverride = json;
#endif

        /// <summary>Opens and initializes a new instance at the same durable location.</summary>
        /// <returns>The initialized store owned by the caller.</returns>
        /// <exception cref="ArgumentOutOfRangeException">The provider selector is unknown.</exception>
        internal async ValueTask<ILocalStoreAdapter> OpenAsync()
        {
            var database = Path.Combine(_directory.FullName, "store.db");
            ILocalStoreAdapter store = _provider switch
            {
                0 => new InMemoryLocalStoreAdapter(Clock, MemoryRecords, MemoryBytes, new RetentionOptions()),
                1 => new SqliteLocalStoreAdapter(database, new() { TimeProvider = Clock }),
                EncryptedSqliteProvider => new SqliteLocalStoreAdapter(
                    database,
                    new() { TimeProvider = Clock, KeyProvider = new StaticLocalStoreKeyProvider(new("conformance", new byte[EncryptionKeyBytes])) }),
                FileSystemProvider => new FileSystemLocalStoreAdapter(_directory.FullName, Clock),
                LiteDbProvider => new LiteDbLocalStoreAdapter(database, Clock),
                BliteDbProvider => new BliteDbLocalStoreAdapter(database, Clock),
#if NET10_0_OR_GREATER
                IndexedDbProvider => new IndexedDbLocalStoreAdapter(_runtime, Clock),
#endif
                _ => throw new ArgumentOutOfRangeException(nameof(_provider), _provider, "Unknown conformance provider."),
            };
            try
            {
                await store.InitializeAsync(
                    new(LocalStoreConformance.Identity, 1, false) { ClientId = LocalStoreConformance.Client },
                    CancellationToken.None);
                return store;
            }
            catch
            {
                await store.DisposeAsync();
                throw;
            }
        }
    }
}
