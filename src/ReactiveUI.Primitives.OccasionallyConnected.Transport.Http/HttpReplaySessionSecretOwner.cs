// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;
using System.Threading;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Owns retained replay session secret bytes.</summary>
/// <remarks>Secret owners have exclusive ownership and are copied, replaced, or disposed under the registry gate.</remarks>
internal sealed class HttpReplaySessionSecretOwner : IDisposable
{
    /// <summary>The retained session secret bytes.</summary>
    private byte[] _sessionSecret = [];

    /// <summary>Whether a session secret has been retained.</summary>
    private int _hasSecret;

    /// <summary>Whether the secret has been disposed.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="HttpReplaySessionSecretOwner"/> class.</summary>
    internal HttpReplaySessionSecretOwner()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="HttpReplaySessionSecretOwner"/> class.</summary>
    /// <param name="sessionSecret">The retained session secret bytes.</param>
    internal HttpReplaySessionSecretOwner(byte[] sessionSecret)
    {
        ArgumentExceptionHelper.ThrowIfNull(sessionSecret);
        _sessionSecret = sessionSecret;
        _hasSecret = 1;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        if (Volatile.Read(ref _hasSecret) == 0)
        {
            return;
        }

        HttpReplayCryptography.ZeroMemory(_sessionSecret);
    }

    /// <summary>Copies the retained secret.</summary>
    /// <returns>An owned copy of the retained secret.</returns>
    /// <exception cref="ObjectDisposedException">The secret has been disposed or was not retained.</exception>
    internal byte[] Copy()
    {
        ObjectDisposedExceptionHelper.ThrowIf(Volatile.Read(ref _disposed) != 0 || Volatile.Read(ref _hasSecret) == 0, this);
        var copy = new byte[_sessionSecret.Length];
        _sessionSecret.CopyTo(copy, 0);
        return copy;
    }

    /// <summary>Retains a session secret after capacity has been admitted.</summary>
    /// <param name="sessionSecret">The session secret text.</param>
    /// <exception cref="ObjectDisposedException">The secret owner has already retained a secret or has been disposed.</exception>
    internal void Retain(string sessionSecret)
    {
        ArgumentExceptionHelper.ThrowIfNull(sessionSecret);
        ObjectDisposedExceptionHelper.ThrowIf(Volatile.Read(ref _disposed) != 0 || Interlocked.CompareExchange(ref _hasSecret, 1, 0) != 0, this);
        _sessionSecret = Encoding.UTF8.GetBytes(sessionSecret);
    }
}
