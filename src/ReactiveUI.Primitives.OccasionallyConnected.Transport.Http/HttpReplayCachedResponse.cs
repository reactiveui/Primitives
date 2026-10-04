// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Represents an owned immutable HTTP response retained for byte-identical replay.</summary>
internal sealed class HttpReplayCachedResponse
{
    /// <summary>The owned response body bytes.</summary>
    private readonly byte[] _body;

    /// <summary>The owned content type bytes.</summary>
    private readonly byte[]? _contentType;

    /// <summary>The owned replay response headers.</summary>
    private readonly CachedHeader[] _headers;

    /// <summary>Initializes a new instance of the <see cref="HttpReplayCachedResponse"/> class.</summary>
    /// <param name="statusCode">The cached status code.</param>
    /// <param name="contentType">The optional content type.</param>
    /// <param name="body">The cached response body.</param>
    /// <param name="headers">The exact cached response headers.</param>
    internal HttpReplayCachedResponse(
        HttpStatusCode statusCode,
        string? contentType,
        ReadOnlyMemory<byte> body,
        IReadOnlyList<KeyValuePair<string, string>>? headers = null)
    {
        StatusCode = statusCode;
        _contentType = contentType is null ? null : Encoding.UTF8.GetBytes(contentType);
        _body = Copy(body);
        _headers = CopyHeaders(headers);
    }

    /// <summary>Gets the cached status code.</summary>
    internal HttpStatusCode StatusCode { get; }

    /// <summary>Gets the optional cached content type.</summary>
    internal string? ContentType => _contentType is null ? null : Encoding.UTF8.GetString(_contentType);

    /// <summary>Gets the owned cached response body.</summary>
    internal ReadOnlyMemory<byte> Body => Copy(_body);

    /// <summary>Gets the exact cached response headers.</summary>
    internal IReadOnlyList<KeyValuePair<string, string>> Headers => CreateHeaderSnapshot(_headers);

    /// <summary>Clears retained response storage.</summary>
    internal void Clear()
    {
        HttpReplayCryptography.ZeroMemory(_body);
        if (_contentType is not null)
        {
            HttpReplayCryptography.ZeroMemory(_contentType);
        }

        for (var index = 0; index < _headers.Length; index++)
        {
            _headers[index].Clear();
        }
    }

    /// <summary>Creates a caller-owned immutable snapshot.</summary>
    /// <returns>The response snapshot.</returns>
    internal HttpReplayCachedResponse CreateSnapshot() => new(StatusCode, ContentType, _body, Headers);

    /// <summary>Copies memory without LINQ.</summary>
    /// <param name="source">The source bytes.</param>
    /// <returns>The copied bytes.</returns>
    private static byte[] Copy(ReadOnlyMemory<byte> source)
    {
        var copy = new byte[source.Length];
        source.CopyTo(copy);
        return copy;
    }

    /// <summary>Copies response headers into owned immutable storage.</summary>
    /// <param name="headers">The source headers.</param>
    /// <returns>The owned response headers.</returns>
    private static CachedHeader[] CopyHeaders(IReadOnlyList<KeyValuePair<string, string>>? headers)
    {
        if (headers is null || headers.Count == 0)
        {
            return [];
        }

        var copy = new CachedHeader[headers.Count];
        for (var index = 0; index < headers.Count; index++)
        {
            copy[index] = new(headers[index].Key, headers[index].Value);
        }

        return copy;
    }

    /// <summary>Creates a string header snapshot.</summary>
    /// <param name="headers">The retained headers.</param>
    /// <returns>The snapshot headers.</returns>
    private static KeyValuePair<string, string>[] CreateHeaderSnapshot(IReadOnlyList<CachedHeader> headers)
    {
        if (headers.Count == 0)
        {
            return [];
        }

        var copy = new KeyValuePair<string, string>[headers.Count];
        for (var index = 0; index < headers.Count; index++)
        {
            copy[index] = headers[index].CreateSnapshot();
        }

        return copy;
    }

    /// <summary>Represents one clearable retained response header.</summary>
    private sealed class CachedHeader
    {
        /// <summary>The header name bytes.</summary>
        private readonly byte[] _key;

        /// <summary>The header value bytes.</summary>
        private readonly byte[] _value;

        /// <summary>Initializes a new instance of the <see cref="CachedHeader"/> class.</summary>
        /// <param name="key">The header key.</param>
        /// <param name="value">The header value.</param>
        internal CachedHeader(string key, string value)
        {
            _key = Encoding.UTF8.GetBytes(key);
            _value = Encoding.UTF8.GetBytes(value);
        }

        /// <summary>Clears retained header storage.</summary>
        internal void Clear()
        {
            HttpReplayCryptography.ZeroMemory(_key);
            HttpReplayCryptography.ZeroMemory(_value);
        }

        /// <summary>Creates a string header snapshot.</summary>
        /// <returns>The header snapshot.</returns>
        internal KeyValuePair<string, string> CreateSnapshot() => new(Encoding.UTF8.GetString(_key), Encoding.UTF8.GetString(_value));
    }
}
