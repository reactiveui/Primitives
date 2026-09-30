// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Security.Cryptography;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile;

/// <summary>Serializes one secure entry and shares its current immutable snapshot.</summary>
internal sealed class MobileSecureEntry : IDisposable
{
    /// <summary>The published secure state.</summary>
    private MobileSecureSnapshot? _snapshot;

    /// <summary>Gets the asynchronous entry gate.</summary>
    internal SemaphoreSlim Gate { get; } = new(1, 1);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => Gate.Dispose();

    /// <summary>Reads the published snapshot.</summary>
    /// <returns>The current secure state.</returns>
    /// <exception cref="InvalidOperationException">No secure snapshot has been loaded.</exception>
    internal MobileSecureSnapshot Read() =>
        Volatile.Read(ref _snapshot) ?? throw new InvalidOperationException("Secure state has not been loaded.");

    /// <summary>Publishes a persisted snapshot to all providers sharing this entry.</summary>
    /// <param name="snapshot">The persisted secure state.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Publish(MobileSecureSnapshot snapshot) => Volatile.Write(ref _snapshot, snapshot);

    /// <summary>Rejects replaced identity or missing and changed retained keys.</summary>
    /// <param name="snapshot">The secure state read from the host.</param>
    /// <exception cref="InvalidOperationException">The host changed identity or retained keys.</exception>
    internal void Validate(MobileSecureSnapshot snapshot)
    {
        var previous = Volatile.Read(ref _snapshot);
        if (previous is null)
        {
            return;
        }

        if (!string.Equals(previous.ClientId, snapshot.ClientId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("Secure state belongs to a different client.");
        }

        foreach (var key in previous.Keys.Values)
        {
            if (!snapshot.Keys.TryGetValue(key.KeyId, out var replacement)
                || !CryptographicOperations.FixedTimeEquals(key.KeyMaterial, replacement.KeyMaterial))
            {
                throw new InvalidOperationException("Secure state changed or removed a retained key.");
            }
        }
    }
}
