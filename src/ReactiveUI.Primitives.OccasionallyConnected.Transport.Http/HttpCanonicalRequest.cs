// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Represents owned canonical request material used for replay fingerprinting.</summary>
internal sealed class HttpCanonicalRequest
{
    /// <summary>The owned request body hash.</summary>
    private readonly byte[] _bodyHash;

    /// <summary>The owned canonical request bytes.</summary>
    private readonly byte[] _bytes;

    /// <summary>Initializes a new instance of the <see cref="HttpCanonicalRequest"/> class.</summary>
    /// <param name="method">The normalized HTTP method.</param>
    /// <param name="normalizedRelativePath">The normalized relative endpoint path.</param>
    /// <param name="canonicalQuery">The canonical query string.</param>
    /// <param name="bodyHash">The request body hash.</param>
    /// <param name="bytes">The canonical request bytes.</param>
    internal HttpCanonicalRequest(
        string method,
        string normalizedRelativePath,
        string canonicalQuery,
        ReadOnlyMemory<byte> bodyHash,
        ReadOnlyMemory<byte> bytes)
    {
        Method = method;
        NormalizedRelativePath = normalizedRelativePath;
        CanonicalQuery = canonicalQuery;
        _bodyHash = Copy(bodyHash);
        _bytes = Copy(bytes);
    }

    /// <summary>Gets the normalized HTTP method.</summary>
    internal string Method { get; }

    /// <summary>Gets the normalized relative endpoint path.</summary>
    internal string NormalizedRelativePath { get; }

    /// <summary>Gets the canonical query string.</summary>
    internal string CanonicalQuery { get; }

    /// <summary>Gets the owned request body hash.</summary>
    internal ReadOnlyMemory<byte> BodyHash => Copy(_bodyHash);

    /// <summary>Gets the owned canonical request bytes.</summary>
    internal ReadOnlyMemory<byte> Bytes => Copy(_bytes);

    /// <summary>Copies memory without LINQ.</summary>
    /// <param name="source">The source bytes.</param>
    /// <returns>The copied bytes.</returns>
    private static byte[] Copy(ReadOnlyMemory<byte> source)
    {
        var copy = new byte[source.Length];
        source.CopyTo(copy);
        return copy;
    }
}
