// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Security.Cryptography;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile;

/// <summary>Represents an immutable bounded identity and key ring persisted as one secure value.</summary>
/// <param name="clientId">The stable client identifier.</param>
/// <param name="current">The current encryption key.</param>
/// <param name="keys">The retained encryption keys.</param>
internal sealed class MobileSecureSnapshot(string clientId, LocalStoreKey current, Dictionary<string, LocalStoreKey> keys)
{
    /// <summary>The maximum retained keys.</summary>
    private const int MaximumKeys = 32;

    /// <summary>The header line count.</summary>
    private const int HeaderLines = 3;

    /// <summary>The minimum complete entry line count.</summary>
    private const int MinimumLines = HeaderLines + 1;

    /// <summary>Gets the stable client identifier.</summary>
    internal string ClientId { get; } = clientId;

    /// <summary>Gets the current encryption key.</summary>
    internal LocalStoreKey Current { get; } = current;

    /// <summary>Gets retained keys, never mutated after publication.</summary>
    internal Dictionary<string, LocalStoreKey> Keys { get; } = keys;

    /// <summary>Parses a bounded secure entry without exposing its contents in exceptions.</summary>
    /// <param name="encoded">The secure entry.</param>
    /// <returns>The parsed snapshot.</returns>
    /// <exception cref="InvalidOperationException">The secure entry is malformed or exceeds its bound.</exception>
    internal static MobileSecureSnapshot Parse(string encoded)
    {
        if (encoded.Length > 4096)
        {
            throw InvalidState();
        }

        var lines = encoded.Split('\n');
        if (lines.Length is < MinimumLines or > MaximumKeys + HeaderLines || lines[0] != "1"
            || !Guid.TryParseExact(lines[1], "N", out var identity) || identity == Guid.Empty)
        {
            throw InvalidState();
        }

        var keys = new Dictionary<string, LocalStoreKey>(StringComparer.Ordinal);
        for (var index = HeaderLines; index < lines.Length; index++)
        {
            ParseKey(lines[index], keys);
        }

        if (!keys.TryGetValue(lines[2], out var current))
        {
            throw InvalidState();
        }

        return new(lines[1], current, keys);
    }

    /// <summary>Produces the complete single-entry secure representation.</summary>
    /// <returns>The secure storage value, which must never be logged.</returns>
    internal string Encode()
    {
        var builder = new StringBuilder();
        _ = builder.Append("1\n").Append(ClientId).Append('\n').Append(Current.KeyId);
        foreach (var key in Keys.Values)
        {
            _ = builder.Append('\n').Append(key.KeyId).Append('|').Append(Convert.ToBase64String(key.KeyMaterial));
        }

        return builder.ToString();
    }

    /// <summary>Creates a new ring with a fresh key and all retained old keys.</summary>
    /// <returns>The replacement snapshot.</returns>
    /// <exception cref="InvalidOperationException">The ring already holds the maximum retained keys.</exception>
    internal MobileSecureSnapshot Rotate()
    {
        if (Keys.Count >= MaximumKeys)
        {
            throw new InvalidOperationException("The retained encryption key limit has been reached.");
        }

        var key = LocalStoreKey.CreateRandom(Guid.NewGuid().ToString("N"));
        var keys = new Dictionary<string, LocalStoreKey>(Keys, StringComparer.Ordinal) { [key.KeyId] = key };
        return new(ClientId, key, keys);
    }

    /// <summary>Parses one key without leaking malformed secret material.</summary>
    /// <param name="line">The encoded key.</param>
    /// <param name="keys">The destination ring.</param>
    private static void ParseKey(string line, Dictionary<string, LocalStoreKey> keys)
    {
        var separator = line.IndexOf('|', StringComparison.Ordinal);
        if (separator <= 0)
        {
            throw InvalidState();
        }

        byte[]? material = null;
        try
        {
            var keyId = line[..separator];
            material = Convert.FromBase64String(line[(separator + 1)..]);
            if (material.Length != 32 || !keys.TryAdd(keyId, new(keyId, material)))
            {
                throw InvalidState();
            }
        }
        catch (Exception exception) when (exception is FormatException or ArgumentException)
        {
            throw InvalidState();
        }
        finally
        {
            if (material is not null)
            {
                CryptographicOperations.ZeroMemory(material);
            }
        }
    }

    /// <summary>Creates a content-free corruption exception.</summary>
    /// <returns>The exception without secret input or an inner exception.</returns>
    private static InvalidOperationException InvalidState() => new("Secure identity or key state is malformed.");
}
