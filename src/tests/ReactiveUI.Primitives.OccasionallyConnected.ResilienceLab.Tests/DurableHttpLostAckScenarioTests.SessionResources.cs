// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Exercises owned resource cleanup when real client composition rejects an input.</summary>
public sealed partial class DurableHttpLostAckScenarioTests
{
    /// <summary>The durable writer database name within each scoped test directory.</summary>
    private const string WriterDatabaseFileName = "writer.sqlite";

    /// <summary>The valid writer identity.</summary>
    private const string WriterClientId = "client-lost-ack-writer";

    /// <summary>An unsupported transport URI fails after the SQLite store exists and closes the owned HTTP client.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvalidTransportAddressDisposesPartiallyCreatedSession()
    {
        var root = CreateTemporarySessionDirectory();
        var httpClient = new DisposalProbeHttpClient(throwOnDispose: false);
        SqliteLocalStoreAdapter? store = null;
        var defaults = DurableHttpLostAckScenario.ClientSessionResourceFactory.Default;
        var factory = defaults with
        {
            CreateHttpClient = () => httpClient,
            CreateStore = (path, clock) => store = defaults.CreateStore(path, clock),
        };
        var clock = new DurableHttpLostAckScenario.MutableTimeProvider(DateTimeOffset.UnixEpoch);
        var databasePath = Path.Combine(root, WriterDatabaseFileName);
        var settings = CreateSessionSettings(WriterClientId);
        try
        {
            await Assert.That(async () =>
            {
                await using var session = await DurableHttpLostAckScenario.CreateClientSessionAsync(
                    databasePath,
                    new("file:///invalid-transport"),
                    clock,
                    settings,
                    factory);
            }).Throws<ArgumentException>();
            await Assert.That(httpClient.DisposeCount).IsEqualTo(1);
            await AssertStoreDisposedAsync(store).ConfigureAwait(false);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>A rejected durable subscription releases the already created context, HTTP transport, client, and SQLite store.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvalidSubscriptionDisposesPartiallyCreatedSession()
    {
        var root = CreateTemporarySessionDirectory();
        var httpClient = new DisposalProbeHttpClient(throwOnDispose: false);
        SqliteLocalStoreAdapter? store = null;
        HttpRemoteTransportAdapter? transport = null;
        var defaults = DurableHttpLostAckScenario.ClientSessionResourceFactory.Default;
        var factory = defaults with
        {
            CreateHttpClient = () => httpClient,
            CreateStore = (path, clock) => store = defaults.CreateStore(path, clock),
            CreateTransport = (address, clock, client) => transport = defaults.CreateTransport(address, clock, client),
        };
        var clock = new DurableHttpLostAckScenario.MutableTimeProvider(DateTimeOffset.UnixEpoch);
        var databasePath = Path.Combine(root, WriterDatabaseFileName);
        var settings = CreateSessionSettings(WriterClientId, new(Guid.Empty));
        try
        {
            await Assert.That(async () =>
            {
                await using var session = await DurableHttpLostAckScenario.CreateClientSessionAsync(
                    databasePath,
                    new("http://127.0.0.1/"),
                    clock,
                    settings,
                    factory);
            }).Throws<InvalidOperationException>();
            await Assert.That(httpClient.DisposeCount).IsEqualTo(1);
            await AssertStoreDisposedAsync(store).ConfigureAwait(false);
            await AssertTransportDisposedAsync(transport).ConfigureAwait(false);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>A constructor failure retains its exact exception while releasing preceding real resources.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResourceFactoryFailurePreservesOriginalExceptionIdentity()
    {
        var root = CreateTemporarySessionDirectory();
        var httpClient = new DisposalProbeHttpClient(throwOnDispose: false);
        SqliteLocalStoreAdapter? store = null;
        var original = new IOException("Transport construction failed.");
        var defaults = DurableHttpLostAckScenario.ClientSessionResourceFactory.Default;
        var factory = defaults with
        {
            CreateHttpClient = () => httpClient,
            CreateStore = (path, clock) => store = defaults.CreateStore(path, clock),
            CreateTransport = (_, _, _) => throw original,
        };

        try
        {
            Exception? observed = null;
            try
            {
                await using var session = await DurableHttpLostAckScenario.CreateClientSessionAsync(
                    Path.Combine(root, WriterDatabaseFileName),
                    new("http://127.0.0.1/"),
                    new(DateTimeOffset.UnixEpoch),
                    CreateSessionSettings(WriterClientId),
                    factory);
            }
            catch (Exception exception)
            {
                observed = exception;
            }

            await Assert.That(observed).IsSameReferenceAs(original);
            await Assert.That(httpClient.DisposeCount).IsEqualTo(1);
            await AssertStoreDisposedAsync(store).ConfigureAwait(false);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>A real transport validation failure and a faulting HTTP disposal both remain visible.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SessionCreationPreservesValidationAndDisposalFailures()
    {
        var root = CreateTemporarySessionDirectory();
        var httpClient = new DisposalProbeHttpClient(throwOnDispose: true);
        SqliteLocalStoreAdapter? store = null;
        var defaults = DurableHttpLostAckScenario.ClientSessionResourceFactory.Default;
        var factory = defaults with
        {
            CreateHttpClient = () => httpClient,
            CreateStore = (path, clock) => store = defaults.CreateStore(path, clock),
        };

        try
        {
            Exception? observed = null;
            try
            {
                await using var session = await DurableHttpLostAckScenario.CreateClientSessionAsync(
                    Path.Combine(root, WriterDatabaseFileName),
                    new("file:///invalid-transport"),
                    new(DateTimeOffset.UnixEpoch),
                    CreateSessionSettings(WriterClientId),
                    factory);
            }
            catch (Exception exception)
            {
                observed = exception;
            }

            var combined = (observed as InvalidOperationException)?.InnerException as AggregateException;
            await Assert.That(combined?.InnerExceptions.Count).IsEqualTo(CombinedFailureCount);
            await Assert.That(combined?.InnerExceptions[0]).IsTypeOf<ArgumentException>();
            await Assert.That(combined?.InnerExceptions[1]).IsTypeOf<IOException>();
            await Assert.That(httpClient.DisposeCount).IsEqualTo(1);
            await AssertStoreDisposedAsync(store).ConfigureAwait(false);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>Creates a durable client identity for construction-boundary tests.</summary>
    /// <param name="clientId">The client ID supplied to the public context.</param>
    /// <param name="subscriptionId">An optional durable subscription identity.</param>
    /// <returns>The client settings.</returns>
    private static DurableHttpLostAckScenario.ClientSessionSettings CreateSessionSettings(string clientId, SubscriptionId? subscriptionId = null) =>
        new(
            clientId,
            "resilience-lab-lost-ack-writer-store",
            subscriptionId ?? SubscriptionId.New(),
            "lost-ack-first-context",
            DurableHttpLostAckScenario.ThrowingOperationIdSource.Instance);

    /// <summary>Creates an isolated directory for owned SQLite and HTTP resources.</summary>
    /// <returns>The temporary directory path.</returns>
    private static string CreateTemporarySessionDirectory()
    {
        var root = Path.Combine(Path.GetTempPath(), $"oc-lost-ack-session-{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(root);
        return root;
    }

    /// <summary>Verifies the partial SQLite adapter rejects a public operation after cleanup.</summary>
    /// <param name="store">The captured adapter.</param>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The expected store was not constructed.</exception>
    private static async Task AssertStoreDisposedAsync(SqliteLocalStoreAdapter? store)
    {
        var captured = store ?? throw new InvalidOperationException("The client store was not constructed.");
        var initialization = new LocalStoreInitialization("resilience-lab-lost-ack-writer-store", 1, false) { ClientId = WriterClientId };
        await Assert.That(async () => await captured.InitializeAsync(initialization, CancellationToken.None)).Throws<ObjectDisposedException>();
    }

    /// <summary>Verifies the partial HTTP adapter rejects a public operation after cleanup.</summary>
    /// <param name="transport">The captured adapter.</param>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The expected transport was not constructed.</exception>
    private static async Task AssertTransportDisposedAsync(HttpRemoteTransportAdapter? transport)
    {
        var captured = transport ?? throw new InvalidOperationException("The HTTP transport was not constructed.");
        var request = new TransportConnectRequest(new(new Version(1, 0), new Version(1, 0)), new(WriterClientId), []);
        await Assert.That(async () => await captured.ConnectAsync(request, CancellationToken.None)).Throws<ObjectDisposedException>();
    }
}
