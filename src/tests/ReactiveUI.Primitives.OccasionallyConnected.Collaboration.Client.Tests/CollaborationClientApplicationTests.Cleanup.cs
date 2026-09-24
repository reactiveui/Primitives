// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Extensions.DependencyInjection;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client.Tests;

/// <content>Partial dependency cleanup tests for <see cref="CollaborationClientApplication"/>.</content>
public sealed partial class CollaborationClientApplicationTests
{
    /// <summary>The transport disposal marker.</summary>
    private const string TransportName = "transport";

    /// <summary>The store disposal marker.</summary>
    private const string StoreName = "store";

    /// <summary>The initial open error message.</summary>
    private const string OpenErrorMessage = "open failed";

    /// <summary>Verifies a real failed open cleans up before a later SQLite client opens.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OpenAsyncRejectsUnsupportedSqlitePathThenOpensRealFile()
    {
        using var lease = new CollaborationClientDatabaseLease();
        var serverUri = new Uri("http://127.0.0.1:0");
        var invalid = CreateClientOptions(serverUri, ":memory:", TokenA, ClientA);

        _ = await Assert.ThrowsAsync<ArgumentException>(() => CollaborationClientApplication.OpenAsync(invalid).AsTask());

        await using var client = await OpenClientAsync(serverUri, lease.ClientAPath, TokenA, ClientA)
            .ConfigureAwait(false);
        var receipt = await client.PublishAsync(new() { Status = "ready" }, CancellationToken.None)
            .ConfigureAwait(false);
        await Assert.That(receipt.State).IsEqualTo(SyncOperationState.SavedLocally);
    }

    /// <summary>Verifies a failed open disposes transport, store, and HTTP client in order.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeAfterOpenFailureReleasesPartialDependenciesInOrder()
    {
        var order = new List<string>();
        var transport = new RecordingAsyncDisposable(order, TransportName);
        var store = new RecordingAsyncDisposable(order, StoreName);
        await using var services = new ServiceCollection().AddHttpClient().BuildServiceProvider();
        using var httpClient = services.GetRequiredService<IHttpClientFactory>().CreateClient();

        await CollaborationClientApplication.DisposeAfterOpenFailureAsync(
                null,
                store,
                transport,
                httpClient,
                new InvalidOperationException(OpenErrorMessage))
            .ConfigureAwait(false);

        await Assert.That(string.Join(",", order)).IsEqualTo("transport,store");
        await Assert.That(transport.DisposeCount).IsEqualTo(1);
        await Assert.That(store.DisposeCount).IsEqualTo(1);
        await AssertHttpClientDisposedAsync(httpClient).ConfigureAwait(false);
    }

    /// <summary>Verifies cleanup failure preserves both the open and disposal errors.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The expected cleanup failure was not captured.</exception>
    [Test]
    public async Task DisposeAfterOpenFailureRetainsBothPartialCleanupFailures()
    {
        var order = new List<string>();
        var transportError = new InvalidOperationException("transport disposal failed");
        var storeError = new InvalidOperationException("store disposal failed");
        var openError = new InvalidOperationException(OpenErrorMessage);
        var transport = new RecordingAsyncDisposable(order, TransportName, transportError);
        var store = new RecordingAsyncDisposable(order, StoreName, storeError);
        await using var services = new ServiceCollection().AddHttpClient().BuildServiceProvider();
        using var httpClient = services.GetRequiredService<IHttpClientFactory>().CreateClient();

        var aggregate = await Assert.ThrowsAsync<AggregateException>(() =>
            CollaborationClientApplication.DisposeAfterOpenFailureAsync(
                    null,
                    store,
                    transport,
                    httpClient,
                    openError)
                .AsTask()) ?? throw new InvalidOperationException("Expected aggregate cleanup failure.");

        await Assert.That(string.Join(",", order)).IsEqualTo("transport,store");
        await Assert.That(ReferenceEquals(aggregate.InnerExceptions[0], openError)).IsTrue();
        var cleanup = (AggregateException)aggregate.InnerExceptions[1];
        await Assert.That(ReferenceEquals(cleanup.InnerExceptions[0], transportError)).IsTrue();
        await Assert.That(ReferenceEquals(cleanup.InnerExceptions[1], storeError)).IsTrue();
        await AssertHttpClientDisposedAsync(httpClient).ConfigureAwait(false);
    }

    /// <summary>Verifies an owned context is the only dependency disposed after build succeeds.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The expected cleanup failure was not captured.</exception>
    [Test]
    public async Task DisposeAfterOpenFailureUsesContextOwnershipAfterBuild()
    {
        var order = new List<string>();
        var contextError = new InvalidOperationException("context disposal failed");
        var openError = new InvalidOperationException(OpenErrorMessage);
        var context = new RecordingAsyncDisposable(order, "context", contextError);
        var transport = new RecordingAsyncDisposable(order, TransportName);
        var store = new RecordingAsyncDisposable(order, StoreName);
        await using var services = new ServiceCollection().AddHttpClient().BuildServiceProvider();
        using var httpClient = services.GetRequiredService<IHttpClientFactory>().CreateClient();

        var aggregate = await Assert.ThrowsAsync<AggregateException>(() =>
            CollaborationClientApplication.DisposeAfterOpenFailureAsync(
                    context,
                    store,
                    transport,
                    httpClient,
                    openError)
                .AsTask()) ?? throw new InvalidOperationException("Expected aggregate cleanup failure.");

        await Assert.That(string.Join(",", order)).IsEqualTo("context");
        await Assert.That(transport.DisposeCount).IsEqualTo(0);
        await Assert.That(store.DisposeCount).IsEqualTo(0);
        await Assert.That(ReferenceEquals(aggregate.InnerExceptions[0], openError)).IsTrue();
        await Assert.That(ReferenceEquals(aggregate.InnerExceptions[1], contextError)).IsTrue();
        await AssertHttpClientDisposedAsync(httpClient).ConfigureAwait(false);
    }

    /// <summary>Verifies the client cannot send after the helper disposes it.</summary>
    /// <param name="httpClient">The HTTP client.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertHttpClientDisposedAsync(HttpClient httpClient)
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        _ = await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            httpClient.GetAsync(new Uri("http://127.0.0.1:1"), cancellation.Token));
    }

    /// <summary>Records disposal order and optional failure for a partially opened dependency.</summary>
    /// <param name="order">The disposal order log.</param>
    /// <param name="name">The dependency name.</param>
    /// <param name="failure">The optional disposal failure.</param>
    private sealed class RecordingAsyncDisposable(List<string> order, string name, Exception? failure = null) : IAsyncDisposable
    {
        /// <summary>Gets the disposal count.</summary>
        internal int DisposeCount { get; private set; }

        /// <inheritdoc />
        public ValueTask DisposeAsync()
        {
            order.Add(name);
            DisposeCount++;
            return failure is null ? ValueTask.CompletedTask : ValueTask.FromException(failure);
        }
    }
}
