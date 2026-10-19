// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile.Tests;

/// <summary>Tests secure identity provisioning and retained encryption keys.</summary>
public sealed class MobileSecureStateTests
{
    /// <summary>The secure entry name.</summary>
    private const string EntryName = "device";

    /// <summary>The concurrent provisioning count.</summary>
    private const int OpenCount = 20;

    /// <summary>The encryption key length.</summary>
    private const int KeyBytes = 32;

    /// <summary>The maximum additional retained keys.</summary>
    private const int RotationCount = 31;

    /// <summary>The asynchronous barrier timeout.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(10);

    /// <summary>Checks concurrent first-time provisioning and reopen stability.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task ProvisioningIsSerializedAndIdentitySurvivesReopen()
    {
        var storage = new MobileTestSecureStorage();
        var opens = Enumerable.Range(0, OpenCount)
            .Select(_ => MobileSecureState.OpenAsync(storage, EntryName, true, CancellationToken.None).AsTask());
        var states = await Task.WhenAll(opens);
        var reopened = await MobileSecureState.OpenAsync(storage, EntryName, false, CancellationToken.None);
        await Assert.That(storage.Writes).IsEqualTo(1);
        await Assert.That(Array.TrueForAll(states, state => state.Identity == reopened.Identity)).IsTrue();
        await Assert.That(reopened.GetCurrentKey().KeyMaterial.Length).IsEqualTo(KeyBytes);
    }

    /// <summary>Checks rotation persists current keys and retains old keys across process-style reopen.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task RotationRetainsKeysAndUpdatesSharedProviders()
    {
        var storage = new MobileTestSecureStorage();
        var first = await MobileSecureState.OpenAsync(storage, EntryName, true, CancellationToken.None);
        var second = await MobileSecureState.OpenAsync(storage, EntryName, false, CancellationToken.None);
        var original = first.GetCurrentKey();
        var rotated = await first.RotateAsync(CancellationToken.None);
        await Assert.That(second.GetCurrentKey().KeyId).IsEqualTo(rotated);
        await Assert.That(second.GetKey(original.KeyId)!.KeyMaterial.SequenceEqual(original.KeyMaterial)).IsTrue();
        var restartedStorage = new MobileTestSecureStorage();
        restartedStorage.Values[EntryName] = storage.Values[EntryName];
        var reopened = await MobileSecureState.OpenAsync(restartedStorage, EntryName, false, CancellationToken.None);
        await Assert.That(reopened.Identity).IsEqualTo(first.Identity);
        await Assert.That(reopened.GetKey(original.KeyId)!.KeyMaterial.SequenceEqual(original.KeyMaterial)).IsTrue();
        await Assert.That(reopened.GetCurrentKey().KeyId).IsEqualTo(rotated);
    }

    /// <summary>Checks secure storage failure never publishes a fallback key.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task SecureStorageFailurePropagatesWithoutChangingCurrentKey()
    {
        var storage = new MobileTestSecureStorage();
        var state = await MobileSecureState.OpenAsync(storage, EntryName, true, CancellationToken.None);
        var key = state.GetCurrentKey();
        storage.Failure = new InvalidOperationException("locked");
        await Assert.That((Func<Task>)(() => state.RotateAsync(CancellationToken.None).AsTask())).ThrowsExactly<InvalidOperationException>();
        await Assert.That(state.GetCurrentKey()).IsEqualTo(key);
        await Assert.That(storage.Writes).IsEqualTo(1);
    }

    /// <summary>Checks missing state cannot be recreated for a known database.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task MissingStateFailsClosed()
    {
        var storage = new MobileTestSecureStorage();
        await Assert.That((Func<Task>)(() => MobileSecureState.OpenAsync(storage, EntryName, false, CancellationToken.None).AsTask()))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(storage.Writes).IsEqualTo(0);
    }

    /// <summary>Checks corrupt secure state is rejected without an automatic reset.</summary>
    /// <param name="value">The malformed value.</param>
    /// <returns>The test completion.</returns>
    [Test]
    [Arguments("")]
    [Arguments("secret-corrupt-value")]
    [Arguments("2\nidentity\nkey\nkey|secret")]
    [Arguments("1\n00000000000000000000000000000000\nkey\nkey|secret")]
    public async Task CorruptStateFailsWithoutReset(string value)
    {
        var storage = new MobileTestSecureStorage();
        storage.Values[EntryName] = value;
        await Assert.That((Func<Task>)(() => MobileSecureState.OpenAsync(storage, EntryName, true, CancellationToken.None).AsTask()))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(storage.Values[EntryName]).IsEqualTo(value);
        await Assert.That(storage.Writes).IsEqualTo(0);
    }

    /// <summary>Checks cancellation before provisioning has no side effects.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task PreCancelledProvisioningDoesNotWrite()
    {
        var storage = new MobileTestSecureStorage();
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        await Assert.That((Func<Task>)(() => MobileSecureState.OpenAsync(storage, EntryName, true, cancelled.Token).AsTask()))
            .Throws<OperationCanceledException>();
        await Assert.That(storage.Writes).IsEqualTo(0);
    }

    /// <summary>Checks cancellation cannot abandon an accepted Essentials write and leave a stale cache.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task CancelledRotationPublishesTheCommittedKey()
    {
        var storage = new MobileTestSecureStorage();
        var state = await MobileSecureState.OpenAsync(storage, EntryName, true, CancellationToken.None);
        var original = state.GetCurrentKey().KeyId;
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        storage.BeforeWrite = () =>
        {
            entered.SetResult();
            return release.Task;
        };
        using var cancellation = new CancellationTokenSource();
        var rotation = state.RotateAsync(cancellation.Token).AsTask();
        await entered.Task.WaitAsync(WaitTimeout);
        await cancellation.CancelAsync();
        await Assert.That(rotation.IsCompleted).IsFalse();
        release.SetResult();
        await Assert.That((Func<Task>)(() => rotation)).Throws<OperationCanceledException>();
        await Assert.That(state.GetCurrentKey().KeyId).IsNotEqualTo(original);
        storage.BeforeWrite = null;
        var reopened = await MobileSecureState.OpenAsync(storage, EntryName, false, CancellationToken.None);
        await Assert.That(reopened.GetCurrentKey().KeyId).IsEqualTo(state.GetCurrentKey().KeyId);
    }

    /// <summary>Checks the bounded ring does not evict keys needed by durable records.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task ConcurrentRotationRetainsEveryKeyAndEnforcesBound()
    {
        var storage = new MobileTestSecureStorage();
        var state = await MobileSecureState.OpenAsync(storage, EntryName, true, CancellationToken.None);
        var original = state.GetCurrentKey().KeyId;
        var rotations = Enumerable.Range(0, RotationCount).Select(_ => state.RotateAsync(CancellationToken.None).AsTask());
        var identifiers = await Task.WhenAll(rotations);
        await Assert.That(identifiers.Distinct().Count()).IsEqualTo(RotationCount);
        await Assert.That(Array.TrueForAll(identifiers, id => state.GetKey(id) is not null)).IsTrue();
        await Assert.That(state.GetKey(original)).IsNotNull();
        await Assert.That((Func<Task>)(() => state.RotateAsync(CancellationToken.None).AsTask())).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Checks externally deleted retained keys are rejected rather than lost during rotation.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task RotationRejectsDeletedRetainedKey()
    {
        var storage = new MobileTestSecureStorage();
        var state = await MobileSecureState.OpenAsync(storage, EntryName, true, CancellationToken.None);
        var firstValue = storage.Values[EntryName];
        var current = await state.RotateAsync(CancellationToken.None);
        storage.Values[EntryName] = firstValue;
        await Assert.That((Func<Task>)(() => state.RotateAsync(CancellationToken.None).AsTask())).ThrowsExactly<InvalidOperationException>();
        await Assert.That(state.GetCurrentKey().KeyId).IsEqualTo(current);
    }

    /// <summary>Checks malformed key records fail without leaking their encoded secret into the failure message.</summary>
    /// <param name="record">The invalid retained-key record.</param>
    /// <returns>The test completion.</returns>
    [Test]
    [Arguments("key|secret-not-base64")]
    [Arguments("missing-separator")]
    [Arguments("key|AA==")]
    [Arguments("bad id|AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")]
    [Arguments("key|AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\nkey|AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")]
    [Arguments("other|AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")]
    public async Task InvalidKeyRecordsFailWithoutSecretDisclosure(string record)
    {
        var storage = new MobileTestSecureStorage();
        storage.Values[EntryName] = $"1\n{Guid.NewGuid():N}\nkey\n{record}";
        Func<Task> open = () => MobileSecureState.OpenAsync(storage, EntryName, true, CancellationToken.None).AsTask();
        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(open);
        await Assert.That(exception!.Message).IsEqualTo("Secure identity or key state is malformed.");
        await Assert.That(storage.Writes).IsEqualTo(0);
    }

    /// <summary>Checks secure entries have a strict bounded parsing size.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task OversizedStateFailsBeforeParsing()
    {
        const int InvalidLength = 4097;
        var storage = new MobileTestSecureStorage();
        storage.Values[EntryName] = new('x', InvalidLength);
        Func<Task> open = () => MobileSecureState.OpenAsync(storage, EntryName, true, CancellationToken.None).AsTask();
        await Assert.That(open).ThrowsExactly<InvalidOperationException>();
        await Assert.That(storage.Writes).IsEqualTo(0);
    }

    /// <summary>Checks a key lookup cannot invent a key that was not provisioned.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task UnknownKeyIsNotSubstitutedWithCurrentKey()
    {
        var storage = new MobileTestSecureStorage();
        var state = await MobileSecureState.OpenAsync(storage, EntryName, true, CancellationToken.None);
        await Assert.That(state.GetKey("missing-key")).IsNull();
    }

    /// <summary>Checks a missing secure entry cannot be recreated by rotating an existing provider.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task RotationRejectsMissingSecureState()
    {
        var storage = new MobileTestSecureStorage();
        var state = await MobileSecureState.OpenAsync(storage, EntryName, true, CancellationToken.None);
        var original = state.GetCurrentKey();
        storage.RemoveAll();
        Func<Task> rotate = () => state.RotateAsync(CancellationToken.None).AsTask();
        await Assert.That(rotate).ThrowsExactly<InvalidOperationException>();
        await Assert.That(state.GetCurrentKey()).IsEqualTo(original);
        await Assert.That(storage.Writes).IsEqualTo(1);
    }

    /// <summary>Checks externally replaced identity cannot silently rebind a loaded client.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task RotationRejectsExternallyReplacedIdentity()
    {
        var storage = new MobileTestSecureStorage();
        var state = await MobileSecureState.OpenAsync(storage, EntryName, true, CancellationToken.None);
        var original = state.Identity;
        storage.Values[EntryName] = storage.Values[EntryName].Replace(
            original.ClientId,
            Guid.NewGuid().ToString("N"),
            StringComparison.Ordinal);
        Func<Task> rotate = () => state.RotateAsync(CancellationToken.None).AsTask();
        await Assert.That(rotate).ThrowsExactly<InvalidOperationException>();
        await Assert.That(state.Identity).IsEqualTo(original);
        await Assert.That(storage.Writes).IsEqualTo(1);
    }

    /// <summary>Checks a host cannot swap retained key material while keeping the same key identifier.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task RotationRejectsExternallyChangedRetainedKey()
    {
        var storage = new MobileTestSecureStorage();
        var state = await MobileSecureState.OpenAsync(storage, EntryName, true, CancellationToken.None);
        var original = state.GetCurrentKey();
        var changed = LocalStoreKey.CreateRandom(original.KeyId);
        storage.Values[EntryName] = storage.Values[EntryName].Replace(
            Convert.ToBase64String(original.KeyMaterial),
            Convert.ToBase64String(changed.KeyMaterial),
            StringComparison.Ordinal);
        Func<Task> rotate = () => state.RotateAsync(CancellationToken.None).AsTask();
        await Assert.That(rotate).ThrowsExactly<InvalidOperationException>();
        await Assert.That(state.GetCurrentKey()).IsEqualTo(original);
        await Assert.That(storage.Writes).IsEqualTo(1);
    }
}
