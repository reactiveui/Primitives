// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Validates HTTP transport options that do not require instance state.</summary>
internal static class HttpRemoteTransportOptionsValidation
{
    /// <summary>Validates that a route remains relative to the configured base URI.</summary>
    /// <param name="path">The route path.</param>
    /// <param name="parameterName">The option name.</param>
    /// <exception cref="ArgumentException">The path is empty, rooted, absolute, query-only, or contains dot segments.</exception>
    internal static void ValidateRelativePath(string path, string parameterName)
    {
        if (!string.IsNullOrWhiteSpace(path)
            && path[0] != '/'
            && path[0] != '?'
            && !Uri.TryCreate(path, UriKind.Absolute, out _)
            && !ContainsDotSegment(path))
        {
            return;
        }

        throw new ArgumentException("HTTP transport endpoints must be non-empty relative paths.", parameterName);
    }

    /// <summary>Validates a positive integer option.</summary>
    /// <param name="value">The option value.</param>
    /// <param name="parameterName">The option name.</param>
    /// <exception cref="ArgumentOutOfRangeException">The value is less than one.</exception>
    internal static void ValidatePositive(int value, string parameterName)
    {
        if (value > 0)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(parameterName, value, "HTTP transport limits must be positive.");
    }

    /// <summary>Checks route segments without allocating a split path.</summary>
    /// <param name="path">The configured relative path.</param>
    /// <returns>Whether a segment changes or collapses the path hierarchy.</returns>
    private static bool ContainsDotSegment(string path)
    {
        var remaining = path.AsSpan();
        while (true)
        {
            var separator = remaining.IndexOf('/');
            var segment = separator < 0 ? remaining : remaining.Slice(0, separator);
            if (segment is "." or "..")
            {
                return true;
            }

            if (separator < 0)
            {
                return false;
            }

            remaining = remaining.Slice(separator + 1);
        }
    }
}
