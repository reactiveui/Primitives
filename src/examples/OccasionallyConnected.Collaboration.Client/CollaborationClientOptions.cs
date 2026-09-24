// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

/// <summary>Configures one collaboration client instance.</summary>
internal sealed record CollaborationClientOptions
{
    /// <summary>The development token header used by the server example.</summary>
    public const string DevelopmentTokenHeaderName = "X-OC-Demo-Token";

    /// <summary>The default logical store identity.</summary>
    public const string DefaultStoreIdentity = "collaboration-client";

    /// <summary>The default finite command wait in seconds.</summary>
    private const int DefaultWaitSeconds = 15;

    /// <summary>The default maximum HTTP request and response bytes.</summary>
    private const int DefaultMaximumTransportBytes = 1024 * 1024;

    /// <summary>Gets the collaboration server base address.</summary>
    public required Uri ServerUri { get; init; }

    /// <summary>Gets the SQLite database path for this client.</summary>
    public required string DatabasePath { get; init; }

    /// <summary>Gets the development token accepted by the server example.</summary>
    public required string Token { get; init; }

    /// <summary>Gets the stable client identifier persisted in the local database.</summary>
    public required string ClientId { get; init; }

    /// <summary>Gets the stable local store identity.</summary>
    public string StoreIdentity { get; init; } = DefaultStoreIdentity;

    /// <summary>Gets a value indicating whether the context should start when opened.</summary>
    public bool AutoStart { get; init; } = true;

    /// <summary>Gets the finite wait used by command-line operations.</summary>
    public TimeSpan WaitTimeout { get; init; } = TimeSpan.FromSeconds(DefaultWaitSeconds);

    /// <summary>Gets the maximum HTTP request and response bytes.</summary>
    public int MaximumTransportBytes { get; init; } = DefaultMaximumTransportBytes;

    /// <summary>Validates this option record.</summary>
    /// <exception cref="ArgumentException">The option record is invalid.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A numeric option is outside the supported range.</exception>
    internal void Validate()
    {
        ArgumentNullException.ThrowIfNull(ServerUri);
        if (!ServerUri.IsAbsoluteUri)
        {
            throw new ArgumentException("ServerUri must be absolute.", nameof(ServerUri));
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(DatabasePath);
        ArgumentException.ThrowIfNullOrWhiteSpace(Token);
        ArgumentException.ThrowIfNullOrWhiteSpace(ClientId);
        ArgumentException.ThrowIfNullOrWhiteSpace(StoreIdentity);
        if (WaitTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(WaitTimeout), WaitTimeout, "WaitTimeout must be positive.");
        }

        if (MaximumTransportBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(MaximumTransportBytes), MaximumTransportBytes, "MaximumTransportBytes must be positive.");
        }
    }
}
