// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Extension forwarding tests for <see cref="SnapshotRecoveryValidator"/> collaborators.</summary>
public sealed partial class SnapshotRecoveryValidatorTests
{
    /// <summary>Verifies remote session convenience overloads forward CancellationToken.None.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RemoteSessionExtensionForwardsCancellationTokenNone()
    {
        var request = CreateRequest([]);
        var result = new RemoteSnapshotRecoveryResult { Status = RemoteSnapshotRecoveryStatus.RetentionExpired };
        var session = new CapturingRemoteSnapshotRecoverySession(result);

        var actual = await session.GetSnapshotAsync(request);

        await Assert.That(actual).IsSameReferenceAs(result);
        await Assert.That(session.Request).IsSameReferenceAs(request);
        await Assert.That(session.CancellationToken).IsEqualTo(CancellationToken.None);
    }

    /// <summary>Verifies server hub convenience overloads forward CancellationToken.None.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ServerHubExtensionForwardsCancellationTokenNone()
    {
        var request = CreateRequest([]);
        var client = new ServerAuthenticatedClient("tenant", "client");
        var result = new RemoteSnapshotRecoveryResult { Status = RemoteSnapshotRecoveryStatus.RetentionExpired };
        var hub = new CapturingServerSnapshotRecoveryHub(result);

        var actual = await hub.GetSnapshotAsync(request, client);

        await Assert.That(actual).IsSameReferenceAs(result);
        await Assert.That(hub.Request).IsSameReferenceAs(request);
        await Assert.That(hub.Client).IsEqualTo(client);
        await Assert.That(hub.CancellationToken).IsEqualTo(CancellationToken.None);
    }

    /// <summary>Verifies local store convenience overloads forward CancellationToken.None.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LocalStoreExtensionForwardsCancellationTokenNone()
    {
        var request = CreateRequest([]);
        var checkpoint = CreateCheckpoint(request);
        var mutation = CreateMutation(request, checkpoint, []);
        var result = new LocalSnapshotRecoveryResult
        {
            Snapshot = new(Stream, FormatVersion, Cursor, CreatePayload(ClientStateContractId, ClientStateSchemaVersion), Revision, DateTimeOffset.UnixEpoch),
            IncludedOperationCount = 0,
            TerminalOperationCount = 0,
            PreservedPendingOperationCount = 0,
        };
        var store = new CapturingLocalSnapshotRecoveryStore(result);

        var actual = await store.ApplySnapshotRecoveryAsync(mutation);

        await Assert.That(actual).IsSameReferenceAs(result);
        await Assert.That(store.Mutation).IsSameReferenceAs(mutation);
        await Assert.That(store.CancellationToken).IsEqualTo(CancellationToken.None);
    }

    /// <summary>Captures remote snapshot recovery session extension forwarding.</summary>
    /// <param name="result">The result to return.</param>
    private sealed class CapturingRemoteSnapshotRecoverySession(RemoteSnapshotRecoveryResult result) : IRemoteSnapshotRecoverySession
    {
        /// <summary>Gets the captured request.</summary>
        public RemoteSnapshotRecoveryRequest? Request { get; private set; }

        /// <summary>Gets the captured cancellation token.</summary>
        public CancellationToken CancellationToken { get; private set; }

        /// <summary>Captures a recovery request.</summary>
        /// <param name="request">The recovery request.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The configured result.</returns>
        public ValueTask<RemoteSnapshotRecoveryResult> GetSnapshotAsync(
            RemoteSnapshotRecoveryRequest request,
            CancellationToken cancellationToken)
        {
            Request = request;
            CancellationToken = cancellationToken;
            return ValueTask.FromResult(result);
        }
    }

    /// <summary>Captures server snapshot recovery hub extension forwarding.</summary>
    /// <param name="result">The result to return.</param>
    private sealed class CapturingServerSnapshotRecoveryHub(RemoteSnapshotRecoveryResult result) : IServerSnapshotRecoveryHub
    {
        /// <summary>Gets the captured request.</summary>
        public RemoteSnapshotRecoveryRequest? Request { get; private set; }

        /// <summary>Gets the captured client identity.</summary>
        public ServerAuthenticatedClient? Client { get; private set; }

        /// <summary>Gets the captured cancellation token.</summary>
        public CancellationToken CancellationToken { get; private set; }

        /// <summary>Captures a recovery request.</summary>
        /// <param name="request">The recovery request.</param>
        /// <param name="client">The authenticated server principal.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The configured result.</returns>
        public ValueTask<RemoteSnapshotRecoveryResult> GetSnapshotAsync(
            RemoteSnapshotRecoveryRequest request,
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken)
        {
            Request = request;
            Client = client;
            CancellationToken = cancellationToken;
            return ValueTask.FromResult(result);
        }
    }

    /// <summary>Captures local snapshot recovery store extension forwarding.</summary>
    /// <param name="result">The result to return.</param>
    private sealed class CapturingLocalSnapshotRecoveryStore(LocalSnapshotRecoveryResult result) : ILocalSnapshotRecoveryStore
    {
        /// <summary>Gets the captured mutation.</summary>
        public LocalSnapshotRecoveryMutation? Mutation { get; private set; }

        /// <summary>Gets the captured cancellation token.</summary>
        public CancellationToken CancellationToken { get; private set; }

        /// <summary>Captures a local recovery mutation.</summary>
        /// <param name="mutation">The recovery mutation.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The configured result.</returns>
        public ValueTask<LocalSnapshotRecoveryResult> ApplySnapshotRecoveryAsync(
            LocalSnapshotRecoveryMutation mutation,
            CancellationToken cancellationToken)
        {
            Mutation = mutation;
            CancellationToken = cancellationToken;
            return ValueTask.FromResult(result);
        }
    }
}
