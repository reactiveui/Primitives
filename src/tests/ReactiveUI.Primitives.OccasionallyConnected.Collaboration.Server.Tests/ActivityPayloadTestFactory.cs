// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Security.Cryptography;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests;

/// <summary>Creates activity payload envelopes used by the collaboration server tests.</summary>
internal static class ActivityPayloadTestFactory
{
    /// <summary>The activity payload contract version used by tests.</summary>
    private const int ActivityPayloadContractVersion = 1;

    /// <summary>The operation id used by initial server state payloads.</summary>
    private const string EmptyOperationId = "00000000000000000000000000000000";

    /// <summary>The SHA-256 hash prefix used by payload envelopes.</summary>
    private const string Sha256Prefix = "sha256-";

    /// <summary>The JSON timestamp emitted by <see cref="DateTimeOffset.UnixEpoch"/>.</summary>
    private const string UnixEpochJsonTimestamp = "1970-01-01T00:00:00.0000000+00:00";

    /// <summary>Creates a canonical initial activity stream state.</summary>
    /// <param name="acceptedVersion">The accepted server version to include.</param>
    /// <returns>The canonical activity payload envelope.</returns>
    internal static PayloadEnvelope CreateInitialState(string acceptedVersion)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(acceptedVersion);
        var json = $$"""
            {
              "status":"empty",
              "acceptedClientId":"server",
              "acceptedOperationId":"{{EmptyOperationId}}",
              "acceptedVersion":"{{acceptedVersion}}",
              "serverAcceptedUtc":"{{UnixEpochJsonTimestamp}}"
            }
            """;
        return CreateEnvelope(json);
    }

    /// <summary>Creates a payload envelope for direct handler validation tests.</summary>
    /// <param name="json">The JSON text.</param>
    /// <returns>The payload envelope.</returns>
    internal static PayloadEnvelope CreateEnvelope(string json)
    {
        var bytes = Encoding.UTF8.GetBytes(json);
        return new(ActivityPayloads.ContractId, ActivityPayloadContractVersion, ActivityPayloads.ContentType, bytes, CreateHash(bytes));
    }

    /// <summary>Creates a SHA-256 payload hash.</summary>
    /// <param name="bytes">The bytes to hash.</param>
    /// <returns>The payload hash string.</returns>
    private static string CreateHash(byte[] bytes) =>
        $"{Sha256Prefix}{Convert.ToBase64String(SHA256.HashData(bytes))}";
}
