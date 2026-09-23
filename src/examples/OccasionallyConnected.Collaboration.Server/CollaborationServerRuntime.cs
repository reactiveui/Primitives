// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Server;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

/// <summary>Owns the concrete server hub, portable HTTP endpoint and development credentials.</summary>
[System.Diagnostics.DebuggerDisplay("{Capabilities,nq}")]
public sealed class CollaborationServerRuntime : IAsyncDisposable
{
    /// <summary>The server protocol major version.</summary>
    private const int ProtocolMajorVersion = 1;

    /// <summary>The server protocol minor version.</summary>
    private const int ProtocolMinorVersion = 0;

    /// <summary>The maximum journal stream count for the example.</summary>
    private const int MaximumJournalStreams = 16;

    /// <summary>The maximum journal ledger entry count for the example.</summary>
    private const int MaximumJournalLedgerEntries = 4096;

    /// <summary>The maximum journal event count for the example.</summary>
    private const int MaximumJournalEvents = 4096;

    /// <summary>The maximum logical journal byte count for the example.</summary>
    private const long MaximumJournalLogicalBytes = 32L * 1024L * 1024L;

    /// <summary>The maximum journal subscription count for the example.</summary>
    private const int MaximumJournalSubscriptions = 128;

    /// <summary>The maximum journal subscription offer count for the example.</summary>
    private const int MaximumJournalSubscriptionOffers = 1024;

    /// <summary>The owned portable HTTP endpoint.</summary>
    private readonly HttpServerEndpoint _endpoint;

    /// <summary>The resource scope transferred to the runtime after startup completes.</summary>
    private readonly CollaborationServerResourceScope _resources;

    /// <summary>Initializes a new instance of the <see cref="CollaborationServerRuntime"/> class.</summary>
    /// <param name="endpoint">The owned portable HTTP endpoint.</param>
    /// <param name="credentials">The development credential store.</param>
    /// <param name="resources">The owned resource scope transferred from startup.</param>
    private CollaborationServerRuntime(
        HttpServerEndpoint endpoint,
        DevelopmentCredentialStore credentials,
        CollaborationServerResourceScope resources)
    {
        _endpoint = endpoint;
        Credentials = credentials;
        _resources = resources;
    }

    /// <summary>Gets the development credential store used by the ASP.NET bridge.</summary>
    public DevelopmentCredentialStore Credentials { get; }

    /// <summary>Gets the negotiated server capabilities exposed by the portable HTTP endpoint.</summary>
    public NegotiatedCapabilities Capabilities => _endpoint.DeclaredCapabilities;

    /// <summary>Creates the runtime from validated example options.</summary>
    /// <param name="options">The example options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The configured runtime.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<CollaborationServerRuntime> CreateAsync(
        CollaborationServerOptions options,
        CancellationToken cancellationToken)
    {
        ValueTask<CollaborationServerRuntime> operation;
        try
        {
            operation = CreateCore(options, cancellationToken);
        }
        catch (OperationCanceledException exception)
        {
            operation = new(CreateCanceledStartupTask(exception.CancellationToken));
        }
        catch (Exception exception)
        {
            operation = ValueTask.FromException<CollaborationServerRuntime>(exception);
        }

        return operation;
    }

    /// <summary>Dispatches one portable HTTP request to the endpoint.</summary>
    /// <param name="request">The request.</param>
    /// <param name="client">The host-authenticated client.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The portable response.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<HttpResponseMessage> HandleAsync(
        HttpRequestMessage request,
        ServerAuthenticatedClient client,
        CancellationToken cancellationToken) =>
        _endpoint.HandleAsync(request, client, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() =>
        _resources.DisposeAsync();

    /// <summary>Creates the runtime and returns acquired-resource failures through rollback.</summary>
    /// <param name="options">The example options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The configured runtime operation.</returns>
    private static ValueTask<CollaborationServerRuntime> CreateCore(
        CollaborationServerOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        cancellationToken.ThrowIfCancellationRequested();
        var credentials = new DevelopmentCredentialStore(options.Credentials);

        var directory = Path.GetDirectoryName(Path.GetFullPath(options.DatabasePath));
        if (!string.IsNullOrEmpty(directory))
        {
            _ = Directory.CreateDirectory(directory);
        }

        var resources = new CollaborationServerResourceScope();
        ValueTask<CollaborationServerRuntime> operation;
        try
        {
            var hub = resources.Track(ServerStreamHub.CreateSqlite(options.DatabasePath, CreateHubOptions(options)));
            var endpoint = resources.Track(new HttpServerEndpoint(CreateEndpointOptions(options, hub)));
            var runtime = new CollaborationServerRuntime(endpoint, credentials, resources);
            operation = new(runtime);
        }
        catch (Exception exception)
        {
            operation = resources.RollbackAsync<CollaborationServerRuntime>(exception);
        }

        return operation;
    }

    /// <summary>Creates a canceled startup operation for a pre-canceled public request.</summary>
    /// <param name="cancellationToken">The cancellation token carried by the startup failure.</param>
    /// <returns>The canceled startup task.</returns>
    private static Task<CollaborationServerRuntime> CreateCanceledStartupTask(CancellationToken cancellationToken)
    {
        var completion = new TaskCompletionSource<CollaborationServerRuntime>(TaskCreationOptions.RunContinuationsAsynchronously);
        completion.SetCanceled(cancellationToken);
        return completion.Task;
    }

    /// <summary>Creates the server hub options for the example.</summary>
    /// <param name="options">The validated example options.</param>
    /// <returns>The server hub options.</returns>
    private static ServerStreamHubOptions CreateHubOptions(CollaborationServerOptions options) =>
        new()
        {
            AuthorizationPolicy = new ConfiguredIdentityAuthorizationPolicy(),
            ConflictHandler = new() { Streams = CollaborationStreamRegistrations.CreateAll(), MaximumProducedEvents = options.MaximumReceiveEvents },
            MaximumActiveCalls = options.MaximumConcurrentRequests,
            MaximumActiveSubscriptions = options.MaximumConcurrentSubscriptions,
            MaximumBatchOperations = options.MaximumBatchOperations,
            MaximumBatchLogicalBytes = options.MaximumRequestBytes,
            MaximumReceiveGroups = options.MaximumReceiveGroups,
            MaximumReceiveEvents = options.MaximumReceiveEvents,
            MaximumReceiveLogicalBytes = options.MaximumRequestBytes,
            EmptyPollDelay = options.EmptyPollDelay,
            JournalLimits = new()
            {
                MaximumStreams = MaximumJournalStreams,
                MaximumLedgerEntries = MaximumJournalLedgerEntries,
                MaximumEvents = MaximumJournalEvents,
                MaximumLogicalBytes = MaximumJournalLogicalBytes,
                MaximumOperationCaptureCount = options.MaximumBatchOperations,
                MaximumEntryEventCount = options.MaximumReceiveEvents,
                MaximumSubscriptions = MaximumJournalSubscriptions,
                MaximumSubscriptionOffers = MaximumJournalSubscriptionOffers,
                OperationRetention = options.ServerIdempotencyRetention,
                SubscriptionRetention = options.ClientInboxRetentionRequired,
            },
        };

    /// <summary>Creates the portable HTTP endpoint options for the example.</summary>
    /// <param name="options">The validated example options.</param>
    /// <param name="hub">The configured server stream hub.</param>
    /// <returns>The portable HTTP endpoint options.</returns>
    private static HttpServerEndpointOptions CreateEndpointOptions(CollaborationServerOptions options, IServerStreamHub hub) =>
        new()
        {
            Hub = hub,
            DeclaredCapabilities = new(
                new Version(ProtocolMajorVersion, ProtocolMinorVersion),
                RemoteTransportCapabilities.BatchPush
                    | RemoteTransportCapabilities.CursorResume
                    | RemoteTransportCapabilities.ReceiveAcknowledgements
                    | RemoteTransportCapabilities.ServerIdempotency
                    | RemoteTransportCapabilities.AtomicApplyAndAcknowledge,
                options.MaximumBatchOperations,
                options.MaximumRequestBytes,
                options.ServerIdempotencyRetention,
                options.ClientInboxRetentionRequired),
            MaximumConcurrentRequests = options.MaximumConcurrentRequests,
            MaximumConcurrentAcknowledgements = options.MaximumConcurrentAcknowledgements,
            MaximumConcurrentSubscriptions = options.MaximumConcurrentSubscriptions,
            MaximumRequestBytes = options.MaximumRequestBytes,
            MaximumResponseBytes = options.MaximumRequestBytes,
            MaximumPayloadBytes = options.MaximumPayloadBytes,
            MaximumBatchOperations = options.MaximumBatchOperations,
            MaximumEventsPerBatch = options.MaximumReceiveEvents,
            MaximumCompletedOperationsPerBatch = options.MaximumReceiveGroups,
            PathBase = options.PathBase,
            LongPollTimeout = options.LongPollTimeout,
            ReplayAuthorizer = ConfiguredReplayAuthorizer.Instance,
        };

    /// <summary>Allows replay admission after the ASP.NET bridge has authenticated the development client.</summary>
    private sealed class ConfiguredReplayAuthorizer : IHttpReplayAuthorizer
    {
        /// <summary>The shared stateless instance.</summary>
        public static readonly ConfiguredReplayAuthorizer Instance = new();

        /// <inheritdoc/>
        public ValueTask<bool> AuthorizeReplayAsync(HttpReplayAuthorizationContext context, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(context);
            cancellationToken.ThrowIfCancellationRequested();
            return new(true);
        }
    }
}
