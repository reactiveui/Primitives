// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Single-writer ownership tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The child ownership mode marker environment variable.</summary>
    private const string OwnershipChildModeVariable = "RXUI_SQLITE_OWNERSHIP_CHILD";

    /// <summary>The child ownership database path environment variable.</summary>
    private const string OwnershipDatabasePathVariable = "RXUI_SQLITE_OWNERSHIP_DATABASE";

    /// <summary>The child ownership signal path environment variable.</summary>
    private const string OwnershipSignalPathVariable = "RXUI_SQLITE_OWNERSHIP_SIGNAL";

    /// <summary>The marker value that enables child ownership mode.</summary>
    private const string OwnershipChildMode = "1";

    /// <summary>The marker value that enables child current-directory mode.</summary>
    private const string CurrentDirectoryChildMode = "cwd";

    /// <summary>The child ownership test tree node filter.</summary>
    private const string OwnershipChildTestTreeNodeFilter = $"/*/*/*/{nameof(WhenOwnershipChildInitializesAndWaits_ThenSignalIsPublished)}";

    /// <summary>The child current-directory test tree node filter.</summary>
    private const string CurrentDirectoryChildTestTreeNodeFilter = $"/*/*/*/{nameof(WhenCurrentDirectoryChildVerifiesCapturedPath_ThenSignalIsPublished)}";

    /// <summary>The relative database file name used by ownership path tests.</summary>
    private const string RelativeDatabaseFileName = "local.db";

    /// <summary>The temporary root directory name used by ownership path tests.</summary>
    private const string OwnershipTempRootName = "rxui-oc-sqlite-adapter";

    /// <summary>The alternate store identity used by reinitialization tests.</summary>
    private const string OwnershipSecondaryStoreIdentity = "client-beta";

    /// <summary>Verifies one process can have only one initialized writer for an adapter without multi-process coordination.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSecondAdapterInitializesSameDatabase_ThenOwnershipFailsAndFirstOwnerRemainsUsable()
    {
        using var database = TempDatabase.Create();
        await using var first = CreateAdapter(database.Path);
        await first.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        await first.InitializeAsync(new(StoreIdentity, MinimumRequiredSchemaVersion, false), CancellationToken.None);
        Func<Task> conflictingReinitialize = () => first.InitializeAsync(new(OwnershipSecondaryStoreIdentity, SchemaVersion, false), CancellationToken.None).AsTask();
        await using var second = CreateAdapter(Path.GetFullPath(database.Path));

        Func<Task> secondInitialize = () => second.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None).AsTask();

        await Assert.That(conflictingReinitialize).ThrowsExactly<InvalidOperationException>();
        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(secondInitialize);
        var subscriptionId = await first.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        var receipt = await first.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var recovery = await first.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(exception?.Message).Contains("already owned");
        await Assert.That(receipt.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That((first.Capabilities & LocalStoreCapabilities.MultiProcessCoordination) != 0).IsFalse();
    }

    /// <summary>Verifies initialization failure releases a newly acquired owner handle.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenInitializationFailsAfterOwnershipAcquire_ThenNextAdapterCanInitialize()
    {
        using var database = TempDatabase.Create();
        CreateUnversionedUserTable(database.Path);
        await using var failed = CreateAdapter(database.Path);

        Func<Task> initialize = () => failed.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None).AsTask();
        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(initialize);
        DeleteSqliteDatabaseFiles(database.Path);

        await using var retry = CreateAdapter(database.Path);
        await retry.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await retry.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);

        await Assert.That(exception?.Message).Contains("schema");
        await Assert.That(subscriptionId.Value).IsNotEqualTo(Guid.Empty);
    }

    /// <summary>Verifies relative paths are resolved once when the adapter is created.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The child process fails to signal readiness.</exception>
    [Test]
    public async Task WhenCurrentDirectoryChangesBeforeInitialize_ThenAdapterUsesConstructionPath()
    {
        var signalPath = System.IO.Path.Combine(TempDatabase.GetTemporaryDirectory(), OwnershipTempRootName, $"{Guid.NewGuid():N}.signal");
        _ = Directory.CreateDirectory(System.IO.Path.GetDirectoryName(signalPath) ?? TempDatabase.GetTemporaryDirectory());
        using var child = StartCurrentDirectoryChild(signalPath);
        var standardOutput = child.StandardOutput.ReadToEndAsync();
        var standardError = child.StandardError.ReadToEndAsync();
        CrashReceiptChildOutput? output = null;
        try
        {
            var signaled = await WaitForSignalAsync(signalPath, child, SignalWaitTimeout);
            if (!signaled)
            {
                output = await StopAndDrainCrashReceiptChildAsync(child, standardOutput, standardError);
                throw new InvalidOperationException(CreateOwnershipSignalTimeoutMessage(output));
            }

            await child.WaitForExitAsync().WaitAsync(GuardTimeout);
            output = await StopAndDrainCrashReceiptChildAsync(child, standardOutput, standardError);

            await Assert.That(output.StandardError).IsEmpty();
        }
        finally
        {
            output ??= await StopAndDrainCrashReceiptChildAsync(child, standardOutput, standardError);
            if (File.Exists(signalPath))
            {
                File.Delete(signalPath);
            }
        }
    }

    /// <summary>Child workflow that verifies relative paths are resolved once when the adapter is created.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The child current-directory environment is incomplete.</exception>
    [Test]
    public async Task WhenCurrentDirectoryChildVerifiesCapturedPath_ThenSignalIsPublished()
    {
        var signalPath = ReadCurrentDirectoryChildSignalPath();
        if (signalPath is null)
        {
            await Assert.That(Environment.GetEnvironmentVariable(OwnershipChildModeVariable)).IsNotEqualTo(CurrentDirectoryChildMode);
            return;
        }

        var originalDirectory = Environment.CurrentDirectory;
        var root = System.IO.Path.Combine(TempDatabase.GetTemporaryDirectory(), OwnershipTempRootName, Guid.NewGuid().ToString("N"));
        var constructionDirectory = System.IO.Path.Combine(root, "construction");
        var initializationDirectory = System.IO.Path.Combine(root, "initialization");
        _ = Directory.CreateDirectory(constructionDirectory);
        _ = Directory.CreateDirectory(initializationDirectory);
        var constructionDatabase = System.IO.Path.Combine(constructionDirectory, RelativeDatabaseFileName);
        var initializationDatabase = System.IO.Path.Combine(initializationDirectory, RelativeDatabaseFileName);
        try
        {
            Environment.CurrentDirectory = constructionDirectory;
            await using var adapter = CreateAdapter(RelativeDatabaseFileName);
            Environment.CurrentDirectory = initializationDirectory;

            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            await using var rejected = CreateAdapter(constructionDatabase);
            Func<Task> secondInitialize = () => rejected.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None).AsTask();

            await Assert.That(File.Exists(constructionDatabase)).IsTrue();
            await Assert.That(File.Exists(initializationDatabase)).IsFalse();
            await Assert.That(secondInitialize).ThrowsExactly<InvalidOperationException>();
        }
        finally
        {
            Environment.CurrentDirectory = originalDirectory;
            if (Directory.Exists(root))
            {
                Directory.Delete(root, true);
            }
        }

        await PublishOwnershipSignalAsync(signalPath);
    }

    /// <summary>Verifies UNC database paths are rejected before ownership claims a writer.</summary>
    /// <param name="databasePath">The UNC-style database path.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Arguments(@"\\rxui-invalid-host\share\local.db")]
    [Arguments("//rxui-invalid-host/share/local.db")]
    public async Task WhenUncDatabasePathInitializes_ThenOwnershipRejectsIt(string databasePath)
    {
        await using var adapter = CreateAdapter(databasePath);
        Func<Task> initialize = () => adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<NotSupportedException>(initialize);

        await Assert.That(exception?.Message).Contains("UNC");
    }

    /// <summary>Verifies network drive types are rejected by the ownership path policy.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDriveTypeIsNetwork_ThenOwnershipPolicyRejectsIt()
    {
        Action reject = static () => SqliteSingleWriterOwnership.ThrowIfUnsupportedDriveType(DriveType.Network);

        await Assert.That(reject).ThrowsExactly<NotSupportedException>();
    }

    /// <summary>Verifies ownership creates a missing parent directory before opening the sidecar.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDatabaseParentDoesNotExist_ThenOwnershipCreatesIt()
    {
        var root = System.IO.Path.Combine(TempDatabase.GetTemporaryDirectory(), OwnershipTempRootName, Guid.NewGuid().ToString("N"));
        var databasePath = System.IO.Path.Combine(root, "missing", RelativeDatabaseFileName);
        try
        {
            await using var adapter = CreateAdapter(databasePath);
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);

            await Assert.That(File.Exists(databasePath)).IsTrue();
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, true);
            }
        }
    }

    /// <summary>Verifies an inaccessible sidecar path fails clearly before SQLite opens.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenOwnershipSidecarPathIsDirectory_ThenOwnershipFailsClearly()
    {
        using var database = TempDatabase.Create();
        _ = Directory.CreateDirectory($"{database.Path}.rxui-owner");
        await using var adapter = CreateAdapter(database.Path);

        Func<Task> initialize = () => adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None).AsTask();
        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(initialize);

        await Assert.That(exception?.Message).Contains("ownership handle");
        await Assert.That(File.Exists(database.Path)).IsFalse();
    }

    /// <summary>Verifies ownership never follows a redirected sidecar handle.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenOwnershipSidecarIsReparsePoint_ThenInitializationRejectsIt()
    {
        using var database = TempDatabase.Create();
        var target = Path.ChangeExtension(database.Path, "owner-target");
        await File.WriteAllTextAsync(target, string.Empty);
        CreateFileSymbolicLinkOrThrow($"{database.Path}.rxui-owner", target);
        await using var adapter = CreateAdapter(database.Path);

        Func<Task> initialize = () => adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None).AsTask();

        await Assert.That(initialize).ThrowsExactly<NotSupportedException>();
        await Assert.That(File.Exists(database.Path)).IsFalse();
    }

    /// <summary>Verifies reparse-point parent paths are rejected before ownership claims a writer.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDatabaseParentIsReparsePoint_ThenOwnershipRejectsIt()
    {
        var root = System.IO.Path.Combine(TempDatabase.GetTemporaryDirectory(), OwnershipTempRootName, Guid.NewGuid().ToString("N"));
        var targetDirectory = System.IO.Path.Combine(root, "target");
        var linkDirectory = System.IO.Path.Combine(root, "link");
        _ = Directory.CreateDirectory(targetDirectory);
        try
        {
            CreateDirectorySymbolicLinkOrThrow(linkDirectory, targetDirectory);

            await using var adapter = CreateAdapter(System.IO.Path.Combine(linkDirectory, RelativeDatabaseFileName));
            Func<Task> initialize = () => adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None).AsTask();

            var exception = await Assert.ThrowsExactlyAsync<NotSupportedException>(initialize);

            await Assert.That(exception?.Message).Contains("reparse-point directories");
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, true);
            }
        }
    }

    /// <summary>Verifies reparse-point database files are rejected before ownership claims a writer.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDatabaseFileIsReparsePoint_ThenOwnershipRejectsIt()
    {
        var root = System.IO.Path.Combine(TempDatabase.GetTemporaryDirectory(), OwnershipTempRootName, Guid.NewGuid().ToString("N"));
        var targetDatabase = System.IO.Path.Combine(root, "target.db");
        var linkDatabase = System.IO.Path.Combine(root, RelativeDatabaseFileName);
        _ = Directory.CreateDirectory(root);
        await File.WriteAllTextAsync(targetDatabase, string.Empty);
        try
        {
            CreateFileSymbolicLinkOrThrow(linkDatabase, targetDatabase);

            await using var adapter = CreateAdapter(linkDatabase);
            Func<Task> initialize = () => adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None).AsTask();

            var exception = await Assert.ThrowsExactlyAsync<NotSupportedException>(initialize);

            await Assert.That(exception?.Message).Contains("reparse-point database files");
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, true);
            }
        }
    }

    /// <summary>Verifies writer ownership is released only when the first adapter is disposed.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenFirstAdapterIsDisposed_ThenSecondAdapterCanOwnSameDatabase()
    {
        using var database = TempDatabase.Create();
        await using (var first = CreateAdapter(database.Path))
        {
            await first.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        }

        await using var second = CreateAdapter(database.Path);
        await second.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await second.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);

        await Assert.That(subscriptionId.Value).IsNotEqualTo(Guid.Empty);
    }

    /// <summary>Verifies a live child-process writer rejects a parent writer and releases ownership after termination.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The child process fails to start or signal readiness.</exception>
    [Test]
    public async Task WhenChildProcessOwnsDatabase_ThenParentWriterFailsUntilChildIsKilled()
    {
        using var database = TempDatabase.Create();
        var signalPath = Path.ChangeExtension(database.Path, $"ownership-{Guid.NewGuid():N}.signal");
        using var child = StartOwnershipChild(database.Path, signalPath);
        var standardOutput = child.StandardOutput.ReadToEndAsync();
        var standardError = child.StandardError.ReadToEndAsync();
        CrashReceiptChildOutput? output = null;
        try
        {
            var signaled = await WaitForSignalAsync(signalPath, child, SignalWaitTimeout);
            if (!signaled)
            {
                output = await StopAndDrainCrashReceiptChildAsync(child, standardOutput, standardError);
                throw new InvalidOperationException(CreateOwnershipSignalTimeoutMessage(output));
            }

            await using (var rejected = CreateAdapter(database.Path))
            {
                Func<Task> secondInitialize = () => rejected.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None).AsTask();
                var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(secondInitialize);
                await Assert.That(exception?.Message).Contains("already owned");
            }

            output = await StopAndDrainCrashReceiptChildAsync(child, standardOutput, standardError);
            await using var reopened = CreateAdapter(database.Path);
            await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            var subscriptionId = await reopened.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            await Assert.That(subscriptionId.Value).IsNotEqualTo(Guid.Empty);
        }
        finally
        {
            output ??= await StopAndDrainCrashReceiptChildAsync(child, standardOutput, standardError);
        }
    }

    /// <summary>Child workflow used by the ownership parent process test.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The child ownership environment is incomplete.</exception>
    [Test]
    public async Task WhenOwnershipChildInitializesAndWaits_ThenSignalIsPublished()
    {
        var childContext = ReadOwnershipChildContext();
        if (childContext is null)
        {
            await Assert.That(Environment.GetEnvironmentVariable(OwnershipChildModeVariable)).IsNull();
            return;
        }

        await using var adapter = CreateAdapter(childContext.DatabasePath);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        await PublishOwnershipSignalAsync(childContext.SignalPath);
        await Task.Delay(Timeout.InfiniteTimeSpan);
    }

    /// <summary>Starts the owned child process that initializes and waits to be killed.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <param name="signalPath">The signal path.</param>
    /// <returns>The started child process.</returns>
    /// <exception cref="InvalidOperationException">The child test process did not start.</exception>
    private static Process StartOwnershipChild(string databasePath, string signalPath)
    {
        var testAssembly = Path.Combine(AppContext.BaseDirectory, TestAssemblyFileName);
        ProcessStartInfo startInfo = new("dotnet") { CreateNoWindow = true, RedirectStandardError = true, RedirectStandardOutput = true, UseShellExecute = false };
        startInfo.ArgumentList.Add(testAssembly);
        startInfo.ArgumentList.Add("--treenode-filter");
        startInfo.ArgumentList.Add(OwnershipChildTestTreeNodeFilter);
        startInfo.ArgumentList.Add("--output");
        startInfo.ArgumentList.Add("Detailed");
        startInfo.Environment[OwnershipChildModeVariable] = OwnershipChildMode;
        startInfo.Environment[OwnershipDatabasePathVariable] = databasePath;
        startInfo.Environment[OwnershipSignalPathVariable] = signalPath;

        var child = Process.Start(startInfo);
        return child ?? throw new InvalidOperationException("The child test process did not start.");
    }

    /// <summary>Starts the child process that verifies current-directory path capture.</summary>
    /// <param name="signalPath">The signal path.</param>
    /// <returns>The started child process.</returns>
    /// <exception cref="InvalidOperationException">The child test process did not start.</exception>
    private static Process StartCurrentDirectoryChild(string signalPath)
    {
        var testAssembly = Path.Combine(AppContext.BaseDirectory, TestAssemblyFileName);
        ProcessStartInfo startInfo = new("dotnet") { CreateNoWindow = true, RedirectStandardError = true, RedirectStandardOutput = true, UseShellExecute = false };
        startInfo.ArgumentList.Add(testAssembly);
        startInfo.ArgumentList.Add("--treenode-filter");
        startInfo.ArgumentList.Add(CurrentDirectoryChildTestTreeNodeFilter);
        startInfo.ArgumentList.Add("--output");
        startInfo.ArgumentList.Add("Detailed");
        startInfo.Environment[OwnershipChildModeVariable] = CurrentDirectoryChildMode;
        startInfo.Environment[OwnershipSignalPathVariable] = signalPath;

        var child = Process.Start(startInfo);
        return child ?? throw new InvalidOperationException("The child test process did not start.");
    }

    /// <summary>Creates a diagnostic timeout message from child process output.</summary>
    /// <param name="output">The child process output.</param>
    /// <returns>The timeout message.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string CreateOwnershipSignalTimeoutMessage(CrashReceiptChildOutput output) =>
        string.Join(
            Environment.NewLine,
            "The child process did not publish the ownership signal.",
            $"HasExited: {output.HasExited.ToString(CultureInfo.InvariantCulture)}",
            "StandardOutput:",
            output.StandardOutput,
            "StandardError:",
            output.StandardError);

    /// <summary>Atomically publishes the child ownership signal.</summary>
    /// <param name="signalPath">The signal path.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task PublishOwnershipSignalAsync(string signalPath)
    {
        var temporaryPath = $"{signalPath}.{Environment.ProcessId}.tmp";
        await File.WriteAllTextAsync(temporaryPath, "ready");
        File.Move(temporaryPath, signalPath);
    }

    /// <summary>Reads child process settings from environment variables.</summary>
    /// <returns>The child context, or null during a normal test run.</returns>
    /// <exception cref="InvalidOperationException">The child ownership environment is incomplete.</exception>
    private static OwnershipChildContext? ReadOwnershipChildContext()
    {
        if (!string.Equals(Environment.GetEnvironmentVariable(OwnershipChildModeVariable), OwnershipChildMode, StringComparison.Ordinal))
        {
            return null;
        }

        var databasePath = Environment.GetEnvironmentVariable(OwnershipDatabasePathVariable);
        var signalPath = Environment.GetEnvironmentVariable(OwnershipSignalPathVariable);
        if (string.IsNullOrWhiteSpace(databasePath) || string.IsNullOrWhiteSpace(signalPath))
        {
            throw new InvalidOperationException("The child ownership environment is incomplete.");
        }

        return new(databasePath, signalPath);
    }

    /// <summary>Reads the current-directory child signal path from environment variables.</summary>
    /// <returns>The signal path, or null during a normal test run.</returns>
    /// <exception cref="InvalidOperationException">The child current-directory environment is incomplete.</exception>
    private static string? ReadCurrentDirectoryChildSignalPath()
    {
        if (!string.Equals(Environment.GetEnvironmentVariable(OwnershipChildModeVariable), CurrentDirectoryChildMode, StringComparison.Ordinal))
        {
            return null;
        }

        var signalPath = Environment.GetEnvironmentVariable(OwnershipSignalPathVariable);
        if (string.IsNullOrWhiteSpace(signalPath))
        {
            throw new InvalidOperationException("The child current-directory environment is incomplete.");
        }

        return signalPath;
    }

    /// <summary>Creates a directory symbolic link for reparse-point tests.</summary>
    /// <param name="linkPath">The link path.</param>
    /// <param name="targetPath">The target path.</param>
    /// <exception cref="PlatformNotSupportedException">The current host cannot create directory symbolic links.</exception>
    private static void CreateDirectorySymbolicLinkOrThrow(string linkPath, string targetPath)
    {
        try
        {
            _ = Directory.CreateSymbolicLink(linkPath, targetPath);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or PlatformNotSupportedException)
        {
            throw new PlatformNotSupportedException("The current host cannot create directory symbolic links for reparse-point coverage.", exception);
        }
    }

    /// <summary>Creates a file symbolic link for reparse-point tests.</summary>
    /// <param name="linkPath">The link path.</param>
    /// <param name="targetPath">The target path.</param>
    /// <exception cref="PlatformNotSupportedException">The current host cannot create file symbolic links.</exception>
    private static void CreateFileSymbolicLinkOrThrow(string linkPath, string targetPath)
    {
        try
        {
            _ = File.CreateSymbolicLink(linkPath, targetPath);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or PlatformNotSupportedException)
        {
            throw new PlatformNotSupportedException("The current host cannot create file symbolic links for reparse-point coverage.", exception);
        }
    }

    /// <summary>Creates an existing unversioned user table to make backend initialization fail after ownership acquisition.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void CreateUnversionedUserTable(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE user_table (id INTEGER NOT NULL);";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Deletes SQLite database files created by a failed initialization test.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void DeleteSqliteDatabaseFiles(string path)
    {
        DeleteFileIfExists(path);
        DeleteFileIfExists($"{path}-wal");
        DeleteFileIfExists($"{path}-shm");
    }

    /// <summary>Deletes a file when it exists.</summary>
    /// <param name="path">The file path.</param>
    private static void DeleteFileIfExists(string path)
    {
        if (!File.Exists(path))
        {
            return;
        }

        File.Delete(path);
    }

    /// <summary>The child process ownership context.</summary>
    /// <param name="DatabasePath">The SQLite database path.</param>
    /// <param name="SignalPath">The atomic signal path.</param>
    private sealed record OwnershipChildContext(string DatabasePath, string SignalPath);
}
