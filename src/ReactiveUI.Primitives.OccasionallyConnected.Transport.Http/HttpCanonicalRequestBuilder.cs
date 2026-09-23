// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Builds bounded canonical HTTP request bytes for replay fingerprinting.</summary>
internal sealed class HttpCanonicalRequestBuilder
{
    /// <summary>The canonical field separator byte.</summary>
    private const byte SeparatorByte = (byte)'\n';

    /// <summary>The number of canonical field separators.</summary>
    private const int SeparatorCount = 4;

    /// <summary>The configured maximum canonical request byte count.</summary>
    private readonly int _maximumCanonicalRequestBytes;

    /// <summary>Initializes a new instance of the <see cref="HttpCanonicalRequestBuilder"/> class.</summary>
    /// <param name="options">The replay protection options.</param>
    internal HttpCanonicalRequestBuilder(HttpReplayProtectionOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        options.Validate();
        _maximumCanonicalRequestBytes = options.MaximumCanonicalRequestBytes;
    }

    /// <summary>Builds canonical request material from validated HTTP endpoint input.</summary>
    /// <param name="operation">The HTTP operation kind.</param>
    /// <param name="method">The HTTP method.</param>
    /// <param name="relativePath">The relative endpoint path.</param>
    /// <param name="query">The query key-value pairs.</param>
    /// <param name="sentAtUtc">The replay timestamp.</param>
    /// <param name="body">The exact request body bytes.</param>
    /// <returns>The owned canonical request.</returns>
    /// <exception cref="ArgumentNullException">A required string or query collection is null.</exception>
    /// <exception cref="HttpRemoteTransportException">Input is invalid or too large.</exception>
    internal HttpCanonicalRequest Build(
        HttpReplayOperationKind operation,
        string method,
        string relativePath,
        IReadOnlyList<KeyValuePair<string, string>> query,
        DateTimeOffset sentAtUtc,
        ReadOnlyMemory<byte> body)
    {
        ArgumentExceptionHelper.ThrowIfNull(method);
        ArgumentExceptionHelper.ThrowIfNull(relativePath);
        ArgumentExceptionHelper.ThrowIfNull(query);
        ValidateOperation(operation);
        ValidateTimestamp(sentAtUtc);
        ValidateText(method);
        ValidatePath(relativePath);
        ValidateQuery(query);
        if (body.Length > _maximumCanonicalRequestBytes)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge, HttpStatusCode.RequestEntityTooLarge);
        }

        var normalizedMethod = method.ToUpperInvariant();
        var canonicalQuery = CreateCanonicalQuery(query);
        var bodyHash = ComputeHash(body);
        var canonicalBytes = CreateCanonicalBytes(normalizedMethod, relativePath, canonicalQuery, sentAtUtc, bodyHash);
        if (canonicalBytes.Length > _maximumCanonicalRequestBytes)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge, HttpStatusCode.RequestEntityTooLarge);
        }

        return new(normalizedMethod, relativePath, canonicalQuery, bodyHash, canonicalBytes);
    }

    /// <summary>Validates the replay operation kind.</summary>
    /// <param name="operation">The candidate operation.</param>
    /// <exception cref="HttpRemoteTransportException">The operation is not defined.</exception>
    private static void ValidateOperation(HttpReplayOperationKind operation)
    {
        if (operation is HttpReplayOperationKind.Connect
            or HttpReplayOperationKind.Push
            or HttpReplayOperationKind.Subscribe
            or HttpReplayOperationKind.Acknowledge
            or HttpReplayOperationKind.SnapshotRecovery)
        {
            return;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected, HttpStatusCode.BadRequest);
    }

    /// <summary>Validates a timestamp is expressed in UTC.</summary>
    /// <param name="sentAtUtc">The candidate timestamp.</param>
    /// <exception cref="HttpRemoteTransportException">The timestamp is not UTC.</exception>
    private static void ValidateTimestamp(DateTimeOffset sentAtUtc)
    {
        if (sentAtUtc.Offset == TimeSpan.Zero)
        {
            return;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected, HttpStatusCode.BadRequest);
    }

    /// <summary>Validates path text has normalized route segment material.</summary>
    /// <param name="value">The candidate path.</param>
    /// <exception cref="HttpRemoteTransportException">The path is empty or contains ambiguous route material.</exception>
    private static void ValidatePath(string value)
    {
        ValidateText(value);
        var segments = value.Split('/');
        for (var index = 0; index < segments.Length; index++)
        {
            ValidatePathSegment(segments[index]);
        }
    }

    /// <summary>Validates one normalized route segment.</summary>
    /// <param name="value">The candidate route segment.</param>
    /// <exception cref="HttpRemoteTransportException">The segment is empty or contains ambiguous route material.</exception>
    private static void ValidatePathSegment(string value)
    {
        if (value.Length != 0 && value is not "." and not ".." && !ContainsPathBoundary(value) && !ContainsEncodedPathBoundary(value))
        {
            return;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected, HttpStatusCode.BadRequest);
    }

    /// <summary>Validates query field text and duplicate keys.</summary>
    /// <param name="query">The query fields.</param>
    /// <exception cref="HttpRemoteTransportException">A query key or value is invalid.</exception>
    private static void ValidateQuery(IReadOnlyList<KeyValuePair<string, string>> query)
    {
        for (var index = 0; index < query.Count; index++)
        {
            ValidateText(query[index].Key);
            ValidateText(query[index].Value);
            for (var other = index + 1; other < query.Count; other++)
            {
                if (!string.Equals(query[index].Key, query[other].Key, StringComparison.Ordinal))
                {
                    continue;
                }

                throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected, HttpStatusCode.BadRequest);
            }
        }
    }

    /// <summary>Validates text has no control characters.</summary>
    /// <param name="value">The candidate text.</param>
    /// <exception cref="HttpRemoteTransportException">The text is empty or contains a control character.</exception>
    private static void ValidateText(string value)
    {
        if (!string.IsNullOrWhiteSpace(value) && !ContainsControl(value))
        {
            return;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected, HttpStatusCode.BadRequest);
    }

    /// <summary>Creates the canonical query string with ordinal key ordering.</summary>
    /// <param name="query">The query fields.</param>
    /// <returns>The canonical query string.</returns>
    private static string CreateCanonicalQuery(IReadOnlyList<KeyValuePair<string, string>> query)
    {
        if (query.Count == 0)
        {
            return string.Empty;
        }

        var fields = new KeyValuePair<string, string>[query.Count];
        for (var index = 0; index < query.Count; index++)
        {
            fields[index] = query[index];
        }

        Array.Sort(fields, CompareQueryFields);
        StringBuilder builder = new();
        for (var index = 0; index < fields.Length; index++)
        {
            if (index != 0)
            {
                _ = builder.Append('&');
            }

            _ = builder
                .Append(Uri.EscapeDataString(fields[index].Key))
                .Append('=')
                .Append(Uri.EscapeDataString(fields[index].Value));
        }

        return builder.ToString();
    }

    /// <summary>Compares query fields by ordinal key then value.</summary>
    /// <param name="left">The left field.</param>
    /// <param name="right">The right field.</param>
    /// <returns>The comparison result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CompareQueryFields(KeyValuePair<string, string> left, KeyValuePair<string, string> right) =>
        string.CompareOrdinal(left.Key, right.Key);

    /// <summary>Computes SHA-256 over memory.</summary>
    /// <param name="source">The source bytes.</param>
    /// <returns>The hash bytes.</returns>
    private static byte[] ComputeHash(ReadOnlyMemory<byte> source)
    {
#if NET6_0_OR_GREATER
        return SHA256.HashData(source.Span);
#else
        using var sha256 = SHA256.Create();
        return sha256.ComputeHash(source.ToArray());
#endif
    }

    /// <summary>Creates canonical bytes.</summary>
    /// <param name="method">The normalized method.</param>
    /// <param name="path">The normalized path.</param>
    /// <param name="query">The canonical query.</param>
    /// <param name="sentAtUtc">The replay timestamp.</param>
    /// <param name="bodyHash">The body hash.</param>
    /// <returns>The canonical bytes.</returns>
    private static byte[] CreateCanonicalBytes(
        string method,
        string path,
        string query,
        DateTimeOffset sentAtUtc,
        ReadOnlySpan<byte> bodyHash)
    {
        var methodBytes = Encoding.UTF8.GetBytes(method);
        var pathBytes = Encoding.UTF8.GetBytes(path);
        var queryBytes = Encoding.UTF8.GetBytes(query);
        var timestampBytes = Encoding.UTF8.GetBytes(sentAtUtc.UtcDateTime.Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture));
        var bytes = new byte[methodBytes.Length + pathBytes.Length + queryBytes.Length + timestampBytes.Length + bodyHash.Length + SeparatorCount];
        var offset = CopyField(methodBytes, bytes, 0);
        offset = CopyField(pathBytes, bytes, offset);
        offset = CopyField(queryBytes, bytes, offset);
        offset = CopyField(timestampBytes, bytes, offset);
        bodyHash.CopyTo(bytes.AsSpan(offset));
        return bytes;
    }

    /// <summary>Copies one canonical field followed by a separator.</summary>
    /// <param name="source">The source field.</param>
    /// <param name="destination">The destination bytes.</param>
    /// <param name="offset">The destination offset.</param>
    /// <returns>The next destination offset.</returns>
    private static int CopyField(ReadOnlySpan<byte> source, byte[] destination, int offset)
    {
        source.CopyTo(destination.AsSpan(offset));
        offset += source.Length;
        destination[offset] = SeparatorByte;
        return offset + 1;
    }

    /// <summary>Checks whether text contains a control character.</summary>
    /// <param name="value">The candidate text.</param>
    /// <returns>Whether a control character is present.</returns>
    private static bool ContainsControl(string value)
    {
        for (var index = 0; index < value.Length; index++)
        {
            if (char.IsControl(value[index]))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Checks whether path text contains a path boundary.</summary>
    /// <param name="value">The candidate text.</param>
    /// <returns>Whether a path boundary is present.</returns>
    private static bool ContainsPathBoundary(string value)
    {
        for (var index = 0; index < value.Length; index++)
        {
            if (value[index] == '/' || value[index] == '\\')
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Checks whether path text hides a boundary in percent encoding.</summary>
    /// <param name="value">The candidate text.</param>
    /// <returns>Whether an encoded boundary is present.</returns>
    private static bool ContainsEncodedPathBoundary(string value)
    {
        var decoded = Uri.UnescapeDataString(value);
        return ContainsControl(decoded) || ContainsPathBoundary(decoded);
    }
}
