// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies the authenticated client operation that originated a remote event.</summary>
[DebuggerDisplay("{ClientId,nq} {OperationId,nq}")]
public sealed record RemoteEventOrigin
{
    /// <summary>The maximum trusted client identity length in UTF-16 characters.</summary>
    private const int MaximumClientIdCharacters = 256;

    /// <summary>The strict UTF-8 encoder used to validate client identities without normalization.</summary>
    private static readonly Encoding ClientIdEncoding = new UTF8Encoding(false, true);

    /// <summary>Initializes a new instance of the <see cref="RemoteEventOrigin"/> class.</summary>
    /// <param name="clientId">The authenticated client identity.</param>
    /// <param name="operationId">The non-empty operation identifier.</param>
    /// <exception cref="ArgumentNullException"><paramref name="clientId"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="clientId"/> is blank or too long, or <paramref name="operationId"/> is empty.</exception>
    /// <exception cref="EncoderFallbackException"><paramref name="clientId"/> contains malformed UTF-16.</exception>
    public RemoteEventOrigin(string clientId, OperationId operationId)
    {
        ArgumentExceptionHelper.ThrowIfNull(clientId);
        if (clientId.Length > MaximumClientIdCharacters)
        {
            throw new ArgumentException("A remote event origin client identity is invalid.", nameof(clientId));
        }

        _ = ClientIdEncoding.GetByteCount(clientId);
#if NET5_0_OR_GREATER
        ArgumentExceptionHelper.ThrowIfNullOrWhiteSpace(clientId);
#else
        if (string.IsNullOrWhiteSpace(clientId))
        {
            throw new ArgumentException("A remote event origin client identity is invalid.", nameof(clientId));
        }
#endif

        if (operationId.Value == Guid.Empty)
        {
            throw new ArgumentException("A remote event origin must contain a non-empty operation identifier.", nameof(operationId));
        }

        ClientId = clientId;
        OperationId = operationId;
    }

    /// <summary>Gets the authenticated client identity.</summary>
    public string ClientId { get; }

    /// <summary>Gets the originating operation identifier.</summary>
    public OperationId OperationId { get; }
}
