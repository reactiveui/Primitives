// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Builds public occasionally connected contexts over a SQLite store and the in-memory loopback hub.</summary>
internal static class ResilienceLabContext
{
    /// <summary>The outbox byte budget used by lab contexts.</summary>
    private const long OutboxBytes = 65_536;

    /// <summary>The SQLite database file name used by lab contexts.</summary>
    private const string DatabaseFileName = "client.db";

    /// <summary>Creates a context that owns a new SQLite store and a loopback transport.</summary>
    /// <param name="settings">The context settings.</param>
    /// <returns>The built context.</returns>
    internal static OccasionallyConnectedContext Create(ResilienceLabContextSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);
        var store = new SqliteLocalStoreAdapter(Path.Combine(settings.DirectoryPath, DatabaseFileName));
        var transport = new LoopbackTransportAdapter(ResilienceLabLoopback.CreateLoopbackOptions(
            settings.Hub,
            settings.ClientId,
            CrdtLoopbackScenarioShape.VolatileLoopbackCapabilities));
        return new OccasionallyConnectedBuilder()
            .UseClient(new(settings.ClientId, ResilienceLabLoopback.TenantId))
            .UseStore(store)
            .UseTransport(transport)
            .UseSerializer(new CrdtPayloadSerializer(ResilienceLabLoopback.Bounds))
            .UseStoreInitialization(new(settings.ClientId, 1, RequireAuthenticatedEncryptionAtRest: false) { ClientId = settings.ClientId })
            .UseTimeProvider(TimeProvider.System)
            .UseOptions(OccasionallyConnectedOptions.Default with
            {
                AutoStart = false,
                MaxConcurrentStreams = 1,
                Outbox = new() { MaxOperations = settings.OutboxOperations, MaxBytes = OutboxBytes },
            })
            .Build();
    }

    /// <summary>Creates a G-counter stream definition for one client.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="clientId">The client identifier.</param>
    /// <returns>The stream definition.</returns>
    internal static StreamDefinition<CrdtState, CrdtInput> CreateDefinition(StreamId streamId, string clientId) =>
        new()
        {
            StreamId = streamId,
            Projection = new CrdtLocalProjection(clientId, new CrdtState { Kind = CrdtKind.GCounter }, ResilienceLabLoopback.Bounds),
            InputContractId = CrdtContracts.InputContractId,
            StateContractId = CrdtContracts.StateContractId,
            InputSchemaVersion = CrdtContracts.SchemaVersion,
            StateSchemaVersion = CrdtContracts.SchemaVersion,
            TypedInput = new() { BufferCapacity = ResilienceLabLoopback.Capacity, BufferCapacityBytes = OutboxBytes, MaximumRetainedInputBytes = OutboxBytes },
        };

    /// <summary>Creates a G-counter input for one client.</summary>
    /// <param name="clientId">The client identifier.</param>
    /// <param name="value">The client's counter component.</param>
    /// <returns>The CRDT input.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    internal static CrdtInput CreateCounterInput(string clientId, long value) =>
        CrdtInput.ForMutation(CrdtMutation.GCounterSet(clientId, value));

    /// <summary>Creates a unique temporary directory for one scenario run.</summary>
    /// <param name="prefix">The directory name prefix.</param>
    /// <returns>The directory path.</returns>
    internal static string CreateTemporaryDirectory(string prefix)
    {
        var path = Path.Combine(Path.GetTempPath(), $"{prefix}-{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(path);
        return path;
    }
}
