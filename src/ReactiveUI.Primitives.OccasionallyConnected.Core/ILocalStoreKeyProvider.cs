// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Supplies the keys that a local store uses to encrypt and authenticate records at rest.</summary>
/// <remarks>
/// <para>
/// A store protects every new or rewritten record with the key returned by <see cref="GetCurrentKey"/> and records that
/// key's identifier next to the ciphertext. It resolves older records through <see cref="GetKey"/>, so a rotated key stays
/// usable for reads until every record protected by it has been rewritten.
/// </para>
/// <para>
/// Stores call these members synchronously on their storage worker. Implementations should hold keys in memory and must
/// never log, trace, or expose key material.
/// </para>
/// </remarks>
public interface ILocalStoreKeyProvider
{
    /// <summary>Gets the key that protects new and rewritten records.</summary>
    /// <returns>The current key.</returns>
    LocalStoreKey GetCurrentKey();

    /// <summary>Gets a key by its identifier so a store can read records protected by that key.</summary>
    /// <param name="keyId">The key identifier recorded with the ciphertext.</param>
    /// <returns>The key, or <see langword="null"/> when this provider does not hold it.</returns>
    LocalStoreKey? GetKey(string keyId);
}
