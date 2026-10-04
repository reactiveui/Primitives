// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Converts SQLite scalar values used by the identity store.</summary>
internal static class SqliteIdentityStoreData
{
    /// <summary>Gets the directory that must exist before opening a database file.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <returns>The directory to create.</returns>
    internal static string GetDirectoryForCreate(string databasePath) => Path.GetDirectoryName(databasePath) is { Length: > 0 } directory ? directory : ".";

    /// <summary>Reads the SQLite schema version scalar.</summary>
    /// <param name="value">The scalar value.</param>
    /// <returns>The schema version.</returns>
    /// <exception cref="InvalidOperationException">The SQLite schema version could not be read.</exception>
    internal static long ReadUserVersion(object? value)
    {
        if (value is long userVersion)
        {
            return userVersion;
        }

        throw new InvalidOperationException("The SQLite identity schema version could not be read.");
    }

    /// <summary>Reads whether user tables exist.</summary>
    /// <param name="value">The scalar value.</param>
    /// <returns>Whether user tables exist.</returns>
    /// <exception cref="InvalidOperationException">The table count could not be read.</exception>
    internal static bool ReadHasUserTables(object? value)
    {
        if (value is long count)
        {
            return count > 0;
        }

        throw new InvalidOperationException("The SQLite identity table count could not be read.");
    }

    /// <summary>Reads a metadata scalar.</summary>
    /// <param name="value">The scalar value.</param>
    /// <returns>The metadata value.</returns>
    /// <exception cref="InvalidOperationException">The metadata value could not be read.</exception>
    internal static string ReadMetadataValue(object? value)
    {
        if (value is string text)
        {
            return text;
        }

        throw new InvalidOperationException("The SQLite identity metadata is incomplete.");
    }

    /// <summary>Reads a subscription identity scalar.</summary>
    /// <param name="value">The scalar value.</param>
    /// <returns>The subscription identity.</returns>
    /// <exception cref="InvalidOperationException">The subscription identity could not be read.</exception>
    internal static SubscriptionId ReadSubscriptionId(object? value)
    {
        if (value is string text && Guid.TryParse(text, out var subscriptionId) && subscriptionId != Guid.Empty)
        {
            return new(subscriptionId);
        }

        throw new InvalidOperationException("The SQLite subscription identity row is invalid.");
    }
}
