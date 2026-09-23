// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Checks ownership when a real observer client is canceled during startup.</summary>
public sealed partial class DurableHttpLostAckScenarioTests
{
    /// <summary>Canceling public context startup disposes its owned HTTP client and SQLite store.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CanceledObserverStartupDisposesConstructedSession()
    {
        var root = CreateTemporarySessionDirectory();
        var clock = new DurableHttpLostAckScenario.MutableTimeProvider(DateTimeOffset.UnixEpoch);
        var httpClient = new DisposalProbeHttpClient(throwOnDispose: false);
        SqliteLocalStoreAdapter? store = null;
        HttpRemoteTransportAdapter? transport = null;
        var defaults = DurableHttpLostAckScenario.ClientSessionResourceFactory.Default;
        var factory = defaults with
        {
            CreateHttpClient = () => httpClient,
            CreateStore = (path, time) => store = defaults.CreateStore(path, time),
            CreateTransport = (address, time, client) => transport = defaults.CreateTransport(address, time, client),
        };
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        try
        {
            await using var host = await DurableHttpLostAckScenario.DurableHttpLostAckHost.StartAsync(Path.Combine(root, "server.sqlite"), clock, CancellationToken.None);
            await Assert.That(async () =>
            {
                await using var observer = await DurableHttpLostAckScenario.StartObserverWithResourcesAsync(
                    Path.Combine(root, "observer.sqlite"),
                    host,
                    clock,
                    factory,
                    cancellation.Token);
            }).Throws<OperationCanceledException>();
            await Assert.That(httpClient.DisposeCount).IsEqualTo(1);
            await AssertStoreDisposedAsync(store).ConfigureAwait(false);
            await AssertTransportDisposedAsync(transport).ConfigureAwait(false);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>A cleanup fault remains paired with the original observer-start cancellation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CanceledObserverStartupPreservesDisposalFailure()
    {
        var root = CreateTemporarySessionDirectory();
        var clock = new DurableHttpLostAckScenario.MutableTimeProvider(DateTimeOffset.UnixEpoch);
        var httpClient = new DisposalProbeHttpClient(throwOnDispose: true);
        SqliteLocalStoreAdapter? store = null;
        HttpRemoteTransportAdapter? transport = null;
        var defaults = DurableHttpLostAckScenario.ClientSessionResourceFactory.Default;
        var factory = defaults with
        {
            CreateHttpClient = () => httpClient,
            CreateStore = (path, time) => store = defaults.CreateStore(path, time),
            CreateTransport = (address, time, client) => transport = defaults.CreateTransport(address, time, client),
        };
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        try
        {
            await using var host = await DurableHttpLostAckScenario.DurableHttpLostAckHost.StartAsync(Path.Combine(root, "server.sqlite"), clock, CancellationToken.None);
            Exception? observed = null;
            try
            {
                await using var observer = await DurableHttpLostAckScenario.StartObserverWithResourcesAsync(
                    Path.Combine(root, "observer.sqlite"),
                    host,
                    clock,
                    factory,
                    cancellation.Token);
            }
            catch (Exception exception)
            {
                observed = exception;
            }

            var aggregate = observed as AggregateException;
            await Assert.That(aggregate?.InnerExceptions.Count).IsEqualTo(CombinedFailureCount);
            await Assert.That(aggregate?.InnerExceptions[0]).IsTypeOf<OperationCanceledException>();
            await Assert.That(aggregate?.InnerExceptions[1]).IsTypeOf<IOException>();
            await Assert.That(httpClient.DisposeCount).IsEqualTo(1);
            await AssertStoreDisposedAsync(store).ConfigureAwait(false);
            await AssertTransportDisposedAsync(transport).ConfigureAwait(false);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }
}
