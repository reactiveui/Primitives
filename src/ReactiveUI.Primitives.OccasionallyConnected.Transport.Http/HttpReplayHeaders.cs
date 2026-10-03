// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net.Http.Headers;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Names HTTP replay request and response headers.</summary>
internal static class HttpReplayHeaders
{
    /// <summary>The replay message identifier header.</summary>
    internal const string MessageId = "X-ReactiveUI-Replay-Message-Id";

    /// <summary>The replay nonce header.</summary>
    internal const string Nonce = "X-ReactiveUI-Replay-Nonce";

    /// <summary>The replay sent timestamp header.</summary>
    internal const string SentAt = "X-ReactiveUI-Replay-Sent-At";

    /// <summary>The trusted replay tenant identifier header.</summary>
    internal const string TenantId = "X-ReactiveUI-Replay-Tenant-Id";

    /// <summary>The replay session identifier header.</summary>
    internal const string SessionId = "X-ReactiveUI-Replay-Session-Id";

    /// <summary>The replay session secret header.</summary>
    internal const string SessionSecret = "X-ReactiveUI-Replay-Session-Secret";

    /// <summary>The replay session expiry header.</summary>
    internal const string SessionExpires = "X-ReactiveUI-Replay-Session-Expires";

    /// <summary>The replay session state marker header.</summary>
    internal const string SessionState = "X-ReactiveUI-Replay-Session-State";

    /// <summary>The stale replay session state marker value.</summary>
    internal const string StaleSessionState = "stale";

    /// <summary>The replay MAC header.</summary>
    internal const string Mac = "X-ReactiveUI-Replay-Mac";

    /// <summary>Reads at most two values so duplicate headers can be rejected without unbounded enumeration.</summary>
    /// <param name="headers">The request or response headers.</param>
    /// <param name="name">The header name.</param>
    /// <param name="value">The observed value, or null when absent.</param>
    /// <returns>Zero for absent headers, one for a single value, or two for repeated values.</returns>
    internal static int ReadValueCount(HttpHeaders headers, string name, out string? value)
    {
        const int duplicateValueCount = 2;
        value = null;
        var count = 0;
        if (headers.TryGetValues(name, out var values))
        {
            using var enumerator = values.GetEnumerator();
            while (count < duplicateValueCount && enumerator.MoveNext())
            {
                value = enumerator.Current;
                count++;
            }
        }

        return count;
    }
}
