// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores durable atomic server state, events and terminal operation replays in SQLite.</summary>
/// <content>Provides bounded operation selection for replay and receive reads.</content>
internal sealed partial class SqliteServerCommitJournal
{
    /// <summary>Gets or creates a reconstruction bucket with one hash lookup on current targets.</summary>
    /// <typeparam name="TKey">The composite or sequence key type.</typeparam>
    /// <typeparam name="TValue">The collection type stored for each owner.</typeparam>
    /// <param name="dictionary">The reconstruction map.</param>
    /// <param name="key">The bucket key.</param>
    /// <returns>The owned bucket.</returns>
    private static TValue GetReadBucket<TKey, TValue>(Dictionary<TKey, TValue> dictionary, TKey key)
        where TKey : notnull
        where TValue : class, new()
    {
#if NET8_0_OR_GREATER
        ref var value = ref System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrAddDefault(dictionary, key, out _);
        return value ??= new();
#else
        if (dictionary.TryGetValue(key, out var existing))
        {
            return existing;
        }

        var value = new TValue();
        dictionary.Add(key, value);
        return value;
#endif
    }

    /// <summary>Builds a parameterized composite-key selection shared by the four reconstruction queries.</summary>
    /// <param name="keys">The requested keys.</param>
    /// <returns>The SQL predicate.</returns>
    private static string CreateLedgerSelection(IReadOnlyList<ServerOperationKey> keys)
    {
        var values = new string[keys.Count];
        for (var index = 0; index < keys.Count; index++)
        {
            var suffix = index.ToString(CultureInfo.InvariantCulture);
            values[index] = $"($selectedClient{suffix}, $selectedOperation{suffix})";
        }

        return $"AND (client_id, operation_id) IN (VALUES {string.Join(",", values)})";
    }

    /// <summary>Binds the requested composite operation keys.</summary>
    /// <param name="command">The statement.</param>
    /// <param name="keys">The selected keys.</param>
    private static void BindLedgerSelection(SqliteStatement command, IReadOnlyList<ServerOperationKey> keys)
    {
        for (var index = 0; index < keys.Count; index++)
        {
            var suffix = index.ToString(CultureInfo.InvariantCulture);
            _ = command.Bind($"$selectedClient{suffix}", keys[index].ClientId);
            _ = command.Bind($"$selectedOperation{suffix}", keys[index].OperationId.Value.ToString("D"));
        }
    }
}
