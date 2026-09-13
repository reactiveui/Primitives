// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Encodes and decodes the bounded HTTP protocol DTOs.</summary>
internal sealed partial class HttpProtocolCodec
{
    /// <summary>Validates required protocol text with strict UTF-8 and an encoded byte bound.</summary>
    /// <param name="value">The protocol text.</param>
    /// <param name="maximumBytes">The maximum UTF-8 byte count.</param>
    /// <exception cref="HttpRemoteTransportException">The value is missing, malformed, or too large.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateProtocolString(string? value, int maximumBytes)
    {
        if (string.IsNullOrEmpty(value))
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        try
        {
            if (StrictUtf8.GetByteCount(value) <= maximumBytes)
            {
                return;
            }
        }
        catch (EncoderFallbackException exception)
        {
            throw CreateProtocolViolation(exception);
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Creates a stable protocol failure without retaining parser payload details.</summary>
    /// <param name="exception">The local parser or conversion exception.</param>
    /// <returns>A sanitized transport exception.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpRemoteTransportException CreateProtocolViolation(Exception exception)
    {
        _ = exception;
        return new(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Runs a local parser or domain conversion with sanitized protocol exceptions.</summary>
    /// <typeparam name="T">The protocol model produced by the parser or conversion.</typeparam>
    /// <param name="action">The parser or conversion to run.</param>
    /// <returns>The protocol model produced by <paramref name="action"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T TranslateProtocolExceptions<T>(Func<T> action)
    {
        try
        {
            return action();
        }
        catch (ArgumentException exception)
        {
            throw CreateProtocolViolation(exception);
        }
        catch (FormatException exception)
        {
            throw CreateProtocolViolation(exception);
        }
        catch (InvalidOperationException exception)
        {
            throw CreateProtocolViolation(exception);
        }
    }

    /// <summary>Runs a local protocol validator with sanitized protocol exceptions.</summary>
    /// <param name="action">The validator to run.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void TranslateProtocolExceptions(Action action)
    {
        try
        {
            action();
        }
        catch (ArgumentException exception)
        {
            throw CreateProtocolViolation(exception);
        }
    }

    /// <summary>Validates required protocol text against the codec-wide string bound.</summary>
    /// <param name="value">The protocol text.</param>
    /// <exception cref="HttpRemoteTransportException">The value is missing, malformed, or too large.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateProtocolString(string? value) => ValidateProtocolString(value, _limits.MaximumProtocolStringBytes);

    /// <summary>Validates optional protocol text when the field is present.</summary>
    /// <param name="value">The optional protocol text.</param>
    /// <exception cref="HttpRemoteTransportException">The value is present but malformed or too large.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateOptionalProtocolString(string? value)
    {
        if (value is null)
        {
            return;
        }

        ValidateProtocolString(value);
    }
}
