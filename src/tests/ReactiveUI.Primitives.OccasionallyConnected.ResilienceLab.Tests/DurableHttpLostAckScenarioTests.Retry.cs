// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Checks retry-deadline diagnostics against an actual durable SQLite sample.</summary>
public sealed partial class DurableHttpLostAckScenarioTests
{
    /// <summary>A canceled retry reports the last persisted state and retains cancellation as its cause.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RetryDeadlineReportsLastDurableWriterSample()
    {
        var root = CreateTemporarySessionDirectory();
        var path = Path.Combine(root, WriterDatabaseFileName);
        var settings = CreateSessionSettings(WriterClientId);
        var clock = new DurableHttpLostAckScenario.MutableTimeProvider(DateTimeOffset.UnixEpoch);
        using var cancellation = new CancellationTokenSource();
        try
        {
            await using var session = await DurableHttpLostAckScenario.CreateClientSessionAsync(
                path,
                new("http://127.0.0.1/"),
                clock,
                settings,
                DurableHttpLostAckScenario.ClientSessionResourceFactory.Default);
            await session.Store.InitializeAsync(
                new(settings.StoreIdentity, 1, false) { ClientId = settings.ClientId },
                CancellationToken.None);
            var before = await ReadRetryTestProofAsync(session, path, settings, CancellationToken.None);
            ClientStoreProof? sampled = null;
            Exception? observed = null;
            try
            {
                _ = await DurableHttpLostAckScenario.WaitForWriterDurableSynchronizationWithReaderAsync(
                    async token =>
                    {
                        var proof = await ReadRetryTestProofAsync(session, path, settings, token);
                        sampled = proof;
                        await cancellation.CancelAsync();
                        return proof;
                    },
                    before,
                    session.Telemetry,
                    cancellation.Token);
            }
            catch (Exception exception)
            {
                observed = exception;
            }

            var timeout = observed as InvalidOperationException;
            await Assert.That(sampled).IsEqualTo(before);
            await Assert.That(timeout?.InnerException).IsTypeOf<OperationCanceledException>();
            await Assert.That(timeout?.Message).Contains("pendingCount=0");
            await Assert.That(timeout?.Message).Contains("faults=");
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>Cancellation before a new SQLite read is classified as a missing durable retry sample.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RetryDeadlineBeforeDurableSampleRetainsCancellation()
    {
        var root = CreateTemporarySessionDirectory();
        var path = Path.Combine(root, WriterDatabaseFileName);
        var settings = CreateSessionSettings(WriterClientId);
        var clock = new DurableHttpLostAckScenario.MutableTimeProvider(DateTimeOffset.UnixEpoch);
        using var cancellation = new CancellationTokenSource();
        try
        {
            await using var session = await DurableHttpLostAckScenario.CreateClientSessionAsync(
                path,
                new("http://127.0.0.1/"),
                clock,
                settings,
                DurableHttpLostAckScenario.ClientSessionResourceFactory.Default);
            await session.Store.InitializeAsync(
                new(settings.StoreIdentity, 1, false) { ClientId = settings.ClientId },
                CancellationToken.None);
            var before = await ReadRetryTestProofAsync(session, path, settings, CancellationToken.None);
            await cancellation.CancelAsync();
            Exception? observed = null;
            try
            {
                _ = await DurableHttpLostAckScenario.WaitForWriterDurableSynchronizationWithReaderAsync(
                    token => ReadRetryTestProofAsync(session, path, settings, token),
                    before,
                    session.Telemetry,
                    cancellation.Token);
            }
            catch (Exception exception)
            {
                observed = exception;
            }

            var timeout = observed as InvalidOperationException;
            await Assert.That(timeout?.Message).Contains("before a durable writer sample");
            await Assert.That(timeout?.InnerException).IsTypeOf<OperationCanceledException>();
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>Reads the initialized real SQLite store without changing its operation state.</summary>
    /// <param name="session">The public client session.</param>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="settings">The durable client identity.</param>
    /// <param name="cancellationToken">The read cancellation token.</param>
    /// <returns>The persisted proof.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static ValueTask<ClientStoreProof> ReadRetryTestProofAsync(
        DurableHttpLostAckScenario.ClientSession session,
        string path,
        DurableHttpLostAckScenario.ClientSessionSettings settings,
        CancellationToken cancellationToken) =>
        DurableHttpLostAckScenario.ReadInitializedClientStoreProofAsync(
            session.Store,
            path,
            settings.StoreIdentity,
            settings.SubscriptionId,
            null,
            cancellationToken);
}
