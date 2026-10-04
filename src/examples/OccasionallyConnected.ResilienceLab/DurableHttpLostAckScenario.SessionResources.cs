// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Runs the durable HTTP lost acknowledgement recovery scenario.</summary>
internal static partial class DurableHttpLostAckScenario
{
    /// <summary>Groups one client's durable identity and HTTP credential.</summary>
    /// <param name="ClientId">The authenticated client identifier.</param>
    /// <param name="StoreIdentity">The durable SQLite store identity.</param>
    /// <param name="SubscriptionId">The durable subscription identifier.</param>
    /// <param name="LabCredential">The host's lab credential.</param>
    /// <param name="OperationIdSource">The source for any new local operation identifier.</param>
    internal sealed record ClientSessionSettings(
        string ClientId,
        string StoreIdentity,
        SubscriptionId SubscriptionId,
        string LabCredential,
        IOperationIdSource OperationIdSource);

    /// <summary>Composes the concrete public client resources in their owned construction order.</summary>
    internal sealed record ClientSessionResourceFactory
    {
        /// <summary>Gets the production composition using real HTTP, SQLite, transport, context, and stream implementations.</summary>
        internal static ClientSessionResourceFactory Default { get; } = new();

        /// <summary>Gets the HTTP client constructor.</summary>
        internal Func<HttpClient> CreateHttpClient { get; init; } = static () => new() { Timeout = OperationTimeout };

        /// <summary>Gets the local SQLite store constructor.</summary>
        internal Func<string, MutableTimeProvider, SqliteLocalStoreAdapter> CreateStore { get; init; } =
            CreateSqliteStore;

        /// <summary>Gets the public HTTP transport constructor.</summary>
        internal Func<Uri, MutableTimeProvider, HttpClient, HttpRemoteTransportAdapter> CreateTransport { get; init; } =
            DurableHttpLostAckScenario.CreateTransport;

        /// <summary>Gets the public context constructor.</summary>
        internal Func<ClientSessionSettings, MutableTimeProvider, SqliteLocalStoreAdapter, HttpRemoteTransportAdapter, OccasionallyConnectedContext> CreateContext { get; init; } =
            static (settings, clock, store, transport) => DurableHttpLostAckScenario.CreateContext(
                settings.ClientId,
                settings.StoreIdentity,
                clock,
                settings.OperationIdSource,
                store,
                transport);

        /// <summary>Gets the stream registration operation.</summary>
        internal Func<OccasionallyConnectedContext, ClientSessionSettings, IOccasionallyConnectedStream<CrdtState, CrdtInput>> CreateStream { get; init; } =
            static (context, settings) => context.GetOrCreateStream(CreateStreamDefinition(settings.ClientId, settings.SubscriptionId));
    }
}
