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
    /// <exception cref="ArgumentException">The path is empty, rooted, or absolute.</exception>
    internal static void ValidateRelativePath(string path, string parameterName)
    {
        if (!string.IsNullOrWhiteSpace(path)
            && path[0] != '/'
            && !Uri.TryCreate(path, UriKind.Absolute, out _))
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
}
