// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Maui.Storage;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile;

/// <summary>Loads a stable client identity and retained encryption keys from real host secure storage.</summary>
/// <remarks>
/// One secure-storage entry contains identity and the complete key ring. Access is serialized within this process for
/// the same service instance and entry name. The host must provide atomic replacement of one entry and exclusive
/// cross-process ownership. No plaintext fallback is used. All old keys remain available after rotation.
/// </remarks>
[DebuggerDisplay("Mobile secure identity and key provider")]
public sealed class MobileSecureState : ILocalStoreKeyProvider
{
    /// <summary>Shares in-process entry locks and immutable snapshots without retaining the secure storage service.</summary>
    private static readonly ConditionalWeakTable<ISecureStorage, ConcurrentDictionary<string, MobileSecureEntry>> Entries = new();

    /// <summary>The borrowed platform secure storage service.</summary>
    private readonly ISecureStorage _storage;

    /// <summary>The secure storage entry name.</summary>
    private readonly string _storageKey;

    /// <summary>The process-shared entry state.</summary>
    private readonly MobileSecureEntry _entry;

    /// <summary>Initializes a new instance of the <see cref="MobileSecureState"/> class.</summary>
    /// <param name="storage">The secure storage service.</param>
    /// <param name="storageKey">The entry name.</param>
    /// <param name="entry">The process-shared state.</param>
    private MobileSecureState(ISecureStorage storage, string storageKey, MobileSecureEntry entry)
    {
        _storage = storage;
        _storageKey = storageKey;
        _entry = entry;
    }

    /// <summary>Gets the stable client identity without credentials or key material.</summary>
    public ClientIdentity Identity => new(_entry.Read().ClientId);

    /// <summary>Loads secure state, optionally provisioning it for a new database.</summary>
    /// <param name="storage">The platform's secure storage implementation.</param>
    /// <param name="storageKey">An app-specific secure storage entry name.</param>
    /// <param name="allowCreate">Whether a missing entry may be created. Use false for existing databases.</param>
    /// <param name="cancellationToken">Cancels waiting or requests cancellation before a write.</param>
    /// <returns>The loaded in-memory key provider.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="storage"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="storageKey"/> is blank.</exception>
    /// <exception cref="InvalidOperationException">Required state is missing or malformed.</exception>
    /// <exception cref="OperationCanceledException">Provisioning admission or waiting was cancelled.</exception>
    /// <remarks>
    /// Essentials writes cannot be cancelled. An accepted write is awaited and published before cancellation is reported.
    /// Secure storage exceptions propagate. Missing or corrupt state never silently changes an existing identity.
    /// </remarks>
    public static async ValueTask<MobileSecureState> OpenAsync(
        ISecureStorage storage,
        string storageKey,
        bool allowCreate,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentException.ThrowIfNullOrWhiteSpace(storageKey);
        var entries = Entries.GetValue(storage, static _ => new(StringComparer.Ordinal));
        var entry = entries.GetOrAdd(storageKey, static _ => new());
        await entry.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var encoded = await storage.GetAsync(storageKey).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            var snapshot = encoded is null
                ? CreateSnapshot(allowCreate)
                : MobileSecureSnapshot.Parse(encoded);
            entry.Validate(snapshot);
            if (encoded is null)
            {
                await storage.SetAsync(storageKey, snapshot.Encode()).ConfigureAwait(false);
            }

            entry.Publish(snapshot);
            cancellationToken.ThrowIfCancellationRequested();
            return new(storage, storageKey, entry);
        }
        finally
        {
            _ = entry.Gate.Release();
        }
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public LocalStoreKey GetCurrentKey() => _entry.Read().Current;

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException"><paramref name="keyId"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="keyId"/> is invalid.</exception>
    public LocalStoreKey? GetKey(string keyId)
    {
        LocalStoreKey.ValidateKeyId(keyId);
        return _entry.Read().Keys.TryGetValue(keyId, out var key) ? key : null;
    }

    /// <summary>Persists a new random current key while retaining every older key for recovery.</summary>
    /// <param name="cancellationToken">Cancels waiting or requests cancellation before the write.</param>
    /// <returns>The new key identifier, never key material.</returns>
    /// <exception cref="InvalidOperationException">Secure state is missing, replaced, malformed, or already holds 32 keys.</exception>
    /// <exception cref="OperationCanceledException">Rotation admission or waiting was cancelled.</exception>
    /// <remarks>
    /// After this succeeds, call the SQLite adapter's RotateEncryptionKeyAsync to rewrite old records.
    /// Keys are never removed automatically. At most 32 keys are retained. A cancelled write may still have committed.
    /// </remarks>
    public async ValueTask<string> RotateAsync(CancellationToken cancellationToken)
    {
        await _entry.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var encoded = await _storage.GetAsync(_storageKey).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            var snapshot = MobileSecureSnapshot.Parse(
                encoded ?? throw new InvalidOperationException("Secure state is missing; rotation cannot replace it."));
            _entry.Validate(snapshot);
            var rotated = snapshot.Rotate();
            await _storage.SetAsync(_storageKey, rotated.Encode()).ConfigureAwait(false);
            _entry.Publish(rotated);
            cancellationToken.ThrowIfCancellationRequested();
            return rotated.Current.KeyId;
        }
        finally
        {
            _ = _entry.Gate.Release();
        }
    }

    /// <summary>Provisions a fresh identity for a database with no previous durable sequence state.</summary>
    /// <param name="storage">The secure storage service.</param>
    /// <param name="storageKey">The secure entry name.</param>
    /// <param name="cancellationToken">The admission cancellation token.</param>
    /// <returns>The new installation's key provider.</returns>
    internal static async ValueTask<MobileSecureState> CreateInstallationAsync(
        ISecureStorage storage,
        string storageKey,
        CancellationToken cancellationToken)
    {
        var entries = Entries.GetValue(storage, static _ => new(StringComparer.Ordinal));
        var previous = entries.GetOrAdd(storageKey, static _ => new());
        await previous.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            var snapshot = CreateSnapshot(true);
            await storage.SetAsync(storageKey, snapshot.Encode()).ConfigureAwait(false);
            var entry = new MobileSecureEntry();
            entry.Publish(snapshot);
            entries[storageKey] = entry;
            return new(storage, storageKey, entry);
        }
        finally
        {
            _ = previous.Gate.Release();
        }
    }

    /// <summary>Creates state only when the host allows first-time provisioning.</summary>
    /// <param name="allowCreate">Whether creation is allowed.</param>
    /// <returns>A new secure snapshot.</returns>
    /// <exception cref="InvalidOperationException">Creation was not allowed.</exception>
    private static MobileSecureSnapshot CreateSnapshot(bool allowCreate)
    {
        if (!allowCreate)
        {
            throw new InvalidOperationException(
                "Secure state is missing for an existing database. Restore its original secure identity and complete key ring "
                + "from a trusted backup. If the keys are lost, preserve or quarantine the database and explicitly re-enroll "
                + "with a new database and identity; pending encrypted operations cannot be recovered.");
        }

        var key = LocalStoreKey.CreateRandom(Guid.NewGuid().ToString("N"));
        return new(Guid.NewGuid().ToString("N"), key, new(StringComparer.Ordinal) { [key.KeyId] = key });
    }
}
