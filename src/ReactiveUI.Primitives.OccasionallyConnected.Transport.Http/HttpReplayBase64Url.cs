// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Encodes replay secrets with unpadded base64url text.</summary>
internal static class HttpReplayBase64Url
{
    /// <summary>Encodes bytes as unpadded base64url text.</summary>
    /// <param name="bytes">The bytes to encode.</param>
    /// <returns>The encoded text.</returns>
    internal static string Encode(ReadOnlySpan<byte> bytes)
    {
        var copy = new byte[bytes.Length];
        bytes.CopyTo(copy);
        var text = Convert.ToBase64String(copy);
        return text.Replace('+', '-').Replace('/', '_').TrimEnd('=');
    }
}
