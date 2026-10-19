// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if !NET5_0_OR_GREATER
using System.Runtime.InteropServices;
#endif

namespace ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

/// <summary>Defines platform requirements for native SQLite persistence.</summary>
internal static class SqlitePlatform
{
    /// <summary>Gets whether Apple file synchronization is required.</summary>
    internal static bool RequiresFullFileSync =>
#if NET5_0_OR_GREATER
        OperatingSystem.IsMacOS() || OperatingSystem.IsIOS() || OperatingSystem.IsMacCatalyst() || OperatingSystem.IsTvOS();
#else
        RuntimeInformation.IsOSPlatform(OSPlatform.OSX)
        || RuntimeInformation.IsOSPlatform(OSPlatform.Create("IOS"))
        || RuntimeInformation.IsOSPlatform(OSPlatform.Create("MACCATALYST"))
        || RuntimeInformation.IsOSPlatform(OSPlatform.Create("TVOS"));
#endif

    /// <summary>Gets whether the current platform supports dedicated command workers.</summary>
    internal static bool SupportsDedicatedWorkers =>
#if NET5_0_OR_GREATER
        !OperatingSystem.IsBrowser();
#else
        !RuntimeInformation.IsOSPlatform(OSPlatform.Create("BROWSER"));
#endif

    /// <summary>Rejects platforms that cannot run the dedicated synchronous worker.</summary>
    /// <param name="supportsDedicatedWorkers">Whether dedicated workers are supported.</param>
    /// <exception cref="PlatformNotSupportedException">Dedicated workers are unavailable.</exception>
    internal static void RequireDedicatedWorkers(bool supportsDedicatedWorkers)
    {
        if (!supportsDedicatedWorkers)
        {
            throw new PlatformNotSupportedException("SQLite persistence requires a dedicated synchronous command worker and is not supported in the browser.");
        }
    }

    /// <summary>Enables and verifies Apple's stronger file and checkpoint synchronization flags.</summary>
    /// <param name="connection">The open connection.</param>
    /// <exception cref="InvalidOperationException">SQLite did not accept the required flags.</exception>
    internal static void ConfigureFullFileSync(SqliteDatabase connection)
    {
        connection.Execute("PRAGMA fullfsync = ON; PRAGMA checkpoint_fullfsync = ON;");
        using var command = connection.CreateStatement();
        command.SetSql("SELECT fullfsync FROM pragma_fullfsync;");
        VerifyFullFileSync(command.Scalar());
        command.SetSql("SELECT checkpoint_fullfsync FROM pragma_checkpoint_fullfsync;");
        VerifyFullFileSync(command.Scalar());
    }

    /// <summary>Verifies one required full file synchronization flag.</summary>
    /// <param name="value">The returned PRAGMA value.</param>
    /// <exception cref="InvalidOperationException">SQLite did not enable the flag.</exception>
    internal static void VerifyFullFileSync(object? value)
    {
        if (value is not long enabled || enabled != 1)
        {
            throw new InvalidOperationException("SQLite did not enable the required Apple full file synchronization settings.");
        }
    }
}
