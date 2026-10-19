// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

/// <summary>Maps one caller-supplied local development token to a trusted server identity.</summary>
/// <param name="Token">The local development token. This value must come from user configuration.</param>
/// <param name="TenantId">The authenticated tenant bound to the token.</param>
/// <param name="ClientId">The authenticated client bound to the token.</param>
[System.Diagnostics.DebuggerDisplay("{TenantId,nq}/{ClientId,nq}")]
public sealed record DevelopmentCredential(string Token, string TenantId, string ClientId)
{
    /// <summary>Parses a semicolon-delimited credential list in token:tenant:client form.</summary>
    /// <param name="value">The credential text.</param>
    /// <returns>The parsed credentials.</returns>
    public static IReadOnlyList<DevelopmentCredential> ParseMany(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return [];
        }

        var parts = value.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var credentials = new DevelopmentCredential[parts.Length];
        for (var index = 0; index < parts.Length; index++)
        {
            credentials[index] = Parse(parts[index]);
        }

        return Array.AsReadOnly(credentials);
    }

    /// <summary>Creates the authenticated server principal represented by this credential.</summary>
    /// <returns>The trusted server identity.</returns>
    public ServerAuthenticatedClient ToAuthenticatedClient() => new(TenantId, ClientId);

    /// <summary>Validates manually constructed credentials before resource creation.</summary>
    /// <param name="credentials">The configured credentials.</param>
    /// <exception cref="ArgumentException">A credential field is blank or a token is duplicated.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="credentials"/> or one entry is <see langword="null"/>.</exception>
    internal static void ValidateAll(IReadOnlyList<DevelopmentCredential> credentials)
    {
        ArgumentNullException.ThrowIfNull(credentials);
        var tokens = new HashSet<string>(StringComparer.Ordinal);
        for (var index = 0; index < credentials.Count; index++)
        {
            var credential = credentials[index] ?? throw new ArgumentNullException(nameof(credentials), "Credentials cannot contain null entries.");
            credential.Validate();
            if (!tokens.Add(credential.Token))
            {
                throw new ArgumentException("Development credential tokens must be unique.", nameof(credentials));
            }
        }
    }

    /// <summary>Validates one manually constructed credential.</summary>
    /// <exception cref="ArgumentException">A credential field is blank.</exception>
    internal void Validate()
    {
        _ = RequireText(Token, nameof(Token));
        _ = RequireText(TenantId, nameof(TenantId));
        _ = RequireText(ClientId, nameof(ClientId));
    }

    /// <summary>Parses one token, tenant and client credential entry.</summary>
    /// <param name="value">The credential entry.</param>
    /// <returns>The parsed credential.</returns>
    /// <exception cref="ArgumentException"><paramref name="value"/> is not in token:tenant:client form.</exception>
    private static DevelopmentCredential Parse(string value)
    {
        var parts = value.Split(':', StringSplitOptions.TrimEntries);
        if (parts.Length != 3)
        {
            throw new ArgumentException("Each development credential must use token:tenant:client form.", nameof(value));
        }

        return new(
            RequireText(parts[0], "token"),
            RequireText(parts[1], "tenant"),
            RequireText(parts[2], "client"));
    }

    /// <summary>Requires a non-empty credential segment.</summary>
    /// <param name="value">The configured text.</param>
    /// <param name="name">The configuration segment name.</param>
    /// <returns>The validated text.</returns>
    private static string RequireText(string value, string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, name);
        return value;
    }
}
