// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Provides a fixed current key and optional retired keys held in memory.</summary>
/// <remarks>
/// Use this provider for tests, samples, and applications that load keys from a platform key store at startup. Retired
/// keys only decrypt existing records; the store never protects new records with them.
/// </remarks>
[DebuggerDisplay("Current = {_currentKey.KeyId,nq}, Keys = {_keys.Count}")]
public sealed class StaticLocalStoreKeyProvider : ILocalStoreKeyProvider
{
    /// <summary>The key that protects new records.</summary>
    private readonly LocalStoreKey _currentKey;

    /// <summary>All keys by identifier, including the current key.</summary>
    private readonly Dictionary<string, LocalStoreKey> _keys;

    /// <summary>Initializes a new instance of the <see cref="StaticLocalStoreKeyProvider"/> class.</summary>
    /// <param name="currentKey">The key that protects new records.</param>
    /// <exception cref="ArgumentNullException"><paramref name="currentKey"/> is null.</exception>
    public StaticLocalStoreKeyProvider(LocalStoreKey currentKey)
        : this(currentKey, [])
    {
    }

    /// <summary>Initializes a new instance of the <see cref="StaticLocalStoreKeyProvider"/> class.</summary>
    /// <param name="currentKey">The key that protects new records.</param>
    /// <param name="retiredKeys">Older keys that still decrypt existing records.</param>
    /// <exception cref="ArgumentNullException">A required argument or retired key is null.</exception>
    /// <exception cref="ArgumentException">Two keys share one identifier.</exception>
    public StaticLocalStoreKeyProvider(LocalStoreKey currentKey, IEnumerable<LocalStoreKey> retiredKeys)
    {
        ArgumentExceptionHelper.ThrowIfNull(currentKey);
        ArgumentExceptionHelper.ThrowIfNull(retiredKeys);
        _currentKey = currentKey;
        _keys = new(StringComparer.Ordinal) { [currentKey.KeyId] = currentKey };
        foreach (var retiredKey in retiredKeys)
        {
            ArgumentExceptionHelper.ThrowIfNull(retiredKey, nameof(retiredKeys));
            if (_keys.ContainsKey(retiredKey.KeyId))
            {
                throw new ArgumentException("Each local store key identifier must be unique.", nameof(retiredKeys));
            }

            _keys.Add(retiredKey.KeyId, retiredKey);
        }
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public LocalStoreKey GetCurrentKey() => _currentKey;

    /// <inheritdoc/>
    public LocalStoreKey? GetKey(string keyId)
    {
        ArgumentExceptionHelper.ThrowIfNull(keyId);
        return _keys.TryGetValue(keyId, out var key) ? key : null;
    }
}
