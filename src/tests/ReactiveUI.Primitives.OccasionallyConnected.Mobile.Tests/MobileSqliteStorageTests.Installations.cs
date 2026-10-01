// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile.Tests;

/// <summary>Tests installation changes and independently restored secure storage.</summary>
public sealed partial class MobileSqliteStorageTests
{
    /// <summary>The Windows symbolic-link privilege failure code.</summary>
    private const int SymbolicLinkPrivilegeError = 1314;

    /// <summary>Checks retained keychain state cannot reuse a deleted database's sequence identity.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task ReinstallWithRetainedSecureStorageStartsFreshIdentityAndKey()
    {
        using var files = new MobileTestFileSystem();
        var secure = new MobileTestSecureStorage();
        ClientIdentity previous;
        string previousKey;
        await using (var original = await CreateAsync(secure, files))
        {
            previous = original.Identity;
            previousKey = original.Keys.GetCurrentKey().KeyId;
            await original.Store.InitializeAsync(new(StoreName, 1, true) { ClientId = previous.ClientId }, CancellationToken.None);
        }

        File.Delete(Path.Combine(files.AppDataDirectory, FileName));
        await using var reinstalled = await CreateAsync(secure, files);
        await reinstalled.Store.InitializeAsync(new(StoreName, 1, true) { ClientId = reinstalled.Identity.ClientId }, CancellationToken.None);
        await Assert.That(reinstalled.Identity).IsNotEqualTo(previous);
        await Assert.That(reinstalled.Keys.GetCurrentKey().KeyId).IsNotEqualTo(previousKey);
        await Assert.That(reinstalled.Keys.GetKey(previousKey)).IsNull();
        await Assert.That(await File.ReadAllTextAsync($"{reinstalled.DatabasePath}.rxui-installation"))
            .IsEqualTo(reinstalled.Identity.ClientId);
    }

    /// <summary>Checks restored matching keys recover the original identity without new provisioning.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task MatchingDatabaseAndSecureBackupPreserveIdentityAndKeys()
    {
        using var files = new MobileTestFileSystem();
        var secure = new MobileTestSecureStorage();
        ClientIdentity identity;
        await using (var original = await CreateAsync(secure, files))
        {
            identity = original.Identity;
            await original.Store.InitializeAsync(new(StoreName, 1, true) { ClientId = identity.ClientId }, CancellationToken.None);
            _ = await original.Keys.RotateAsync(CancellationToken.None);
        }

        var restored = new MobileTestSecureStorage();
        restored.Values[EntryName] = secure.Values[EntryName];
        await using var recovered = await CreateAsync(restored, files);
        await recovered.Store.InitializeAsync(new(StoreName, 1, true) { ClientId = recovered.Identity.ClientId }, CancellationToken.None);
        await Assert.That(recovered.Identity).IsEqualTo(identity);
        await Assert.That(restored.Writes).IsEqualTo(0);
    }

    /// <summary>Checks missing backup keys leave durable state untouched with an actionable recovery diagnostic.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task DatabaseOnlyBackupFailsWithExplicitRecoveryDiagnostic()
    {
        using var files = new MobileTestFileSystem();
        await using (var original = await CreateAsync(new(), files))
        {
            await original.Store.InitializeAsync(new(StoreName, 1, true) { ClientId = original.Identity.ClientId }, CancellationToken.None);
        }

        var path = Path.Combine(files.AppDataDirectory, FileName);
        var before = await File.ReadAllBytesAsync(path);
        var missing = new MobileTestSecureStorage();
        var failure = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => CreateAsync(missing, files));
        await Assert.That(failure!.Message).Contains("Restore its original secure identity");
        await Assert.That(failure.Message).Contains("pending encrypted operations cannot be recovered");
        await Assert.That((await File.ReadAllBytesAsync(path)).SequenceEqual(before)).IsTrue();
        await Assert.That(missing.Writes).IsEqualTo(0);
    }

    /// <summary>Checks a restored database cannot be paired with another installation's secure state.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task MismatchedRestoredSecureStateFailsBeforeDatabaseInitialization()
    {
        using var files = new MobileTestFileSystem();
        await using (var original = await CreateAsync(new(), files))
        {
            await original.Store.InitializeAsync(new(StoreName, 1, true) { ClientId = original.Identity.ClientId }, CancellationToken.None);
        }

        var unrelated = new MobileTestSecureStorage();
        _ = await MobileSecureState.OpenAsync(unrelated, EntryName, true, CancellationToken.None);
        var failure = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => CreateAsync(unrelated, files));
        await Assert.That(failure!.Message).Contains("installation marker and secure identity do not match");
        await Assert.That(unrelated.Writes).IsEqualTo(1);
    }

    /// <summary>Checks a second bundle cannot reset secure state before the first initializes SQLite.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task ConcurrentFactoryCannotReplaceUninitializedInstallation()
    {
        using var files = new MobileTestFileSystem();
        var secure = new MobileTestSecureStorage();
        await using var first = await CreateAsync(secure, files);
        await Assert.That((Func<Task>)(() => CreateAsync(secure, files))).ThrowsExactly<InvalidOperationException>();
        await Assert.That(secure.Writes).IsEqualTo(1);
    }

    /// <summary>Checks orphaned WAL recovery state cannot silently trigger fresh keys.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task MissingDatabaseWithRecoverySidecarsDoesNotReplaceSecureState()
    {
        using var files = new MobileTestFileSystem();
        var secure = new MobileTestSecureStorage();
        string encoded;
        await using (var original = await CreateAsync(secure, files))
        {
            encoded = secure.Values[EntryName];
        }

        var path = Path.Combine(files.AppDataDirectory, FileName);
        File.Delete(path);
        await File.WriteAllTextAsync($"{path}-wal", "durable recovery state");
        var failure = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => CreateAsync(secure, files));
        await Assert.That(failure!.Message).Contains("recovery sidecars");
        await Assert.That(secure.Values[EntryName]).IsEqualTo(encoded);
        await Assert.That(secure.Writes).IsEqualTo(1);
    }

    /// <summary>Checks cancellation after a secure write preserves the bound identity on retry.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task CancelledAcceptedProvisioningPreservesInstallationOnRetry()
    {
        using var files = new MobileTestFileSystem();
        using var cancellation = new CancellationTokenSource();
        var secure = new MobileTestSecureStorage { BeforeWrite = cancellation.CancelAsync };
        Func<Task> create = () => MobileSqliteStorage.CreateAsync(
            secure,
            files,
            FileName,
            EntryName,
            new(),
            cancellation.Token).AsTask();
        await Assert.That(create).Throws<OperationCanceledException>();
        secure.BeforeWrite = null;
        var saved = await MobileSecureState.OpenAsync(secure, EntryName, false, CancellationToken.None);
        await using var retried = await CreateAsync(secure, files);
        await retried.Store.InitializeAsync(new(StoreName, 1, true) { ClientId = retried.Identity.ClientId }, CancellationToken.None);
        await Assert.That(retried.Identity).IsEqualTo(saved.Identity);
        await Assert.That(secure.Writes).IsEqualTo(1);
    }

    /// <summary>Checks a missing installation marker does not manufacture a binding for an existing database.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task ExistingDatabaseWithoutMarkerFailsClosedWithoutModification()
    {
        using var files = new MobileTestFileSystem();
        var secure = new MobileTestSecureStorage();
        await using (var original = await CreateAsync(secure, files))
        {
            await original.Store.InitializeAsync(new(StoreName, 1, true) { ClientId = original.Identity.ClientId }, CancellationToken.None);
        }

        var path = Path.Combine(files.AppDataDirectory, FileName);
        var marker = $"{path}.rxui-installation";
        var before = await File.ReadAllBytesAsync(path);
        var encoded = secure.Values[EntryName];
        File.Delete(marker);

        var failure = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => CreateAsync(secure, files));

        await Assert.That(failure!.Message).Contains("missing its installation marker");
        await Assert.That(File.Exists(marker)).IsFalse();
        await Assert.That((await File.ReadAllBytesAsync(path)).SequenceEqual(before)).IsTrue();
        await Assert.That(secure.Values[EntryName]).IsEqualTo(encoded);
        await Assert.That(secure.Writes).IsEqualTo(1);
    }

    /// <summary>Checks platform-style directory aliases work and do not admit a second installation writer.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task AppDataDirectoryAliasPreservesIdentityAndExclusiveOwnership()
    {
        using var files = new MobileTestFileSystem();
        var root = files.AppDataDirectory;
        var physical = Path.Combine(root, "physical");
        var alias = Path.Combine(root, "alias");
        _ = Directory.CreateDirectory(physical);
        await CreateAppDataAliasAsync(alias, physical);
        try
        {
            var secure = new MobileTestSecureStorage();
            files.AppDataDirectory = alias;
            ClientIdentity identity;
            await using (var first = await CreateAsync(secure, files))
            {
                identity = first.Identity;
                await first.Store.InitializeAsync(new(StoreName, 1, true) { ClientId = identity.ClientId }, CancellationToken.None);
                files.AppDataDirectory = physical;
                await Assert.That((Func<Task>)(() => CreateAsync(secure, files))).ThrowsExactly<InvalidOperationException>();
            }

            files.AppDataDirectory = physical;
            await using var reopened = await CreateAsync(secure, files);
            await reopened.Store.InitializeAsync(new(StoreName, 1, true) { ClientId = reopened.Identity.ClientId }, CancellationToken.None);
            await Assert.That(reopened.Identity).IsEqualTo(identity);
            await Assert.That(secure.Writes).IsEqualTo(1);
        }
        finally
        {
            Directory.Delete(alias);
        }
    }

    /// <summary>Creates an app-data directory alias on hosts that permit it.</summary>
    /// <param name="alias">The alias path.</param>
    /// <param name="physical">The physical directory.</param>
    /// <returns>The alias creation completion.</returns>
    private static async Task CreateAppDataAliasAsync(string alias, string physical)
    {
        try
        {
            _ = Directory.CreateSymbolicLink(alias, physical);
        }
        catch (UnauthorizedAccessException)
        {
            Skip.Test("This host does not permit directory symbolic links; native macOS CI runs this check.");
        }
        catch (IOException exception) when ((exception.HResult & 0xffff) == SymbolicLinkPrivilegeError)
        {
            var start = new ProcessStartInfo("powershell.exe") { UseShellExecute = false, CreateNoWindow = true };
            start.ArgumentList.Add("-NoProfile");
            start.ArgumentList.Add("-Command");
            start.ArgumentList.Add(
                "New-Item -ItemType Junction -Path $env:RXUI_ALIAS_PATH -Target $env:RXUI_ALIAS_TARGET | Out-Null");
            start.Environment["RXUI_ALIAS_PATH"] = alias;
            start.Environment["RXUI_ALIAS_TARGET"] = physical;
            using var process = Process.Start(start);
            await Assert.That(process).IsNotNull();
#if NET11_0_OR_GREATER
            var status = await process!.WaitForExitStatusAsync();
            await Assert.That(status.Canceled).IsFalse();
            await Assert.That(status.Signal).IsNull();
            await Assert.That(status.ExitCode).IsEqualTo(0);
#else
            await process!.WaitForExitAsync();
            await Assert.That(process.ExitCode).IsEqualTo(0);
#endif
        }
    }
}
