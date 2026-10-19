// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Owns a trusted canonical operation intent fingerprint.</summary>
internal sealed class ServerCommitFingerprint
{
    /// <summary>The required SHA-256 fingerprint byte length.</summary>
    internal const int Length = 32;

    /// <summary>The owned fingerprint bytes.</summary>
    private readonly byte[] _bytes;

    /// <summary>Initializes a new instance of the <see cref="ServerCommitFingerprint"/> class.</summary>
    /// <param name="bytes">The trusted canonical fingerprint bytes.</param>
    /// <exception cref="ArgumentException">The fingerprint is not exactly 32 bytes.</exception>
    internal ServerCommitFingerprint(ReadOnlyMemory<byte> bytes)
    {
        if (bytes.Length != Length)
        {
            throw new ArgumentException("A server commit fingerprint must contain exactly 32 bytes.", nameof(bytes));
        }

        _bytes = bytes.ToArray();
    }

    /// <summary>Compares another fingerprint with this one.</summary>
    /// <param name="other">The other fingerprint.</param>
    /// <returns>Whether both fingerprints contain the same bytes.</returns>
    internal bool Matches(ServerCommitFingerprint other)
    {
        for (var index = 0; index < Length; index++)
        {
            if (_bytes[index] != other._bytes[index])
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Gets an owned copy of the fingerprint bytes.</summary>
    /// <returns>The fingerprint bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal byte[] ToArray() => _bytes.AsSpan().ToArray();
}
