// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Threading;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Owns a candidate replay session secret until it is disposed or transferred.</summary>
/// <remarks>Candidate instances have exclusive ownership and are retained, transferred, or disposed under the registry gate.</remarks>
internal sealed class HttpReplaySessionSecretCandidate : IDisposable
{
    /// <summary>The active secret owner.</summary>
    private readonly HttpReplaySessionSecretOwner _owner = new();

    /// <summary>Whether this candidate has been disposed or transferred.</summary>
    private int _closed;

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _closed, 1) != 0)
        {
            return;
        }

        _owner.Dispose();
    }

    /// <summary>Retains a session secret after capacity has been admitted.</summary>
    /// <param name="sessionSecret">The session secret text.</param>
    /// <exception cref="ObjectDisposedException">The candidate has already been disposed or transferred.</exception>
    internal void Retain(string sessionSecret)
    {
        ObjectDisposedExceptionHelper.ThrowIf(Volatile.Read(ref _closed) != 0, this);
        _owner.Retain(sessionSecret);
    }

    /// <summary>Transfers ownership to retained replay state.</summary>
    /// <returns>The retained secret owner.</returns>
    /// <exception cref="ObjectDisposedException">The candidate has already been disposed or transferred.</exception>
    internal HttpReplaySessionSecretOwner Transfer()
    {
        if (Interlocked.Exchange(ref _closed, 1) == 0)
        {
            return _owner;
        }

        throw new ObjectDisposedException(GetType().FullName);
    }
}
