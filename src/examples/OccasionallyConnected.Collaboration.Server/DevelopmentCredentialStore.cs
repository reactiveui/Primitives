// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

/// <summary>Authenticates local development requests by looking up configured bearer tokens.</summary>
[System.Diagnostics.DebuggerDisplay("Credentials={_clientsByToken.Count,nq}")]
public sealed class DevelopmentCredentialStore
{
    /// <summary>The authenticated clients keyed by local development token.</summary>
    private readonly ReadOnlyDictionary<string, ServerAuthenticatedClient> _clientsByToken;

    /// <summary>Initializes a new instance of the <see cref="DevelopmentCredentialStore"/> class.</summary>
    /// <param name="credentials">The configured credentials.</param>
    /// <exception cref="ArgumentException">A credential token is duplicated.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="credentials"/> or one of its entries is <see langword="null"/>.</exception>
    public DevelopmentCredentialStore(IReadOnlyList<DevelopmentCredential> credentials)
    {
        ArgumentNullException.ThrowIfNull(credentials);
        var clientsByToken = new Dictionary<string, ServerAuthenticatedClient>(StringComparer.Ordinal);
        for (var index = 0; index < credentials.Count; index++)
        {
            var credential = credentials[index] ?? throw new ArgumentNullException(nameof(credentials), "Credentials cannot contain null entries.");
            credential.Validate();
            if (!clientsByToken.TryAdd(credential.Token, credential.ToAuthenticatedClient()))
            {
                throw new ArgumentException("Development credential tokens must be unique.", nameof(credentials));
            }
        }

        _clientsByToken = new(clientsByToken);
    }

    /// <summary>Gets the request header carrying the local development token.</summary>
    public static string TokenHeaderName => "X-OC-Demo-Token";

    /// <summary>Attempts to authenticate a local development token.</summary>
    /// <param name="token">The caller-supplied token.</param>
    /// <param name="client">The trusted server identity when the token is known.</param>
    /// <returns>Whether authentication succeeded.</returns>
    public bool TryAuthenticate(string token, out ServerAuthenticatedClient client)
    {
        if (_clientsByToken.TryGetValue(token, out var knownClient))
        {
            client = knownClient;
            return true;
        }

        client = new(string.Empty, string.Empty);
        return false;
    }
}
