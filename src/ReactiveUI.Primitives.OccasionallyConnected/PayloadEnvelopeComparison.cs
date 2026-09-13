// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Compares serialized payload content across local storage and reconciliation.</summary>
internal static class PayloadEnvelopeComparison
{
    /// <summary>Determines whether two payload envelopes contain the same canonical content.</summary>
    /// <param name="left">The first payload.</param>
    /// <param name="right">The second payload.</param>
    /// <returns>Whether the payloads match.</returns>
    internal static bool ContentEquals(PayloadEnvelope left, PayloadEnvelope right) =>
        left.SchemaVersion == right.SchemaVersion
        && string.Equals(left.ContractId, right.ContractId, StringComparison.Ordinal)
        && string.Equals(left.ContentType, right.ContentType, StringComparison.Ordinal)
        && left.PayloadLength == right.PayloadLength
        && HashEquals(left.PayloadHash, right.PayloadHash)
        && left.Payload.Span.SequenceEqual(right.Payload.Span);

    /// <summary>Determines whether two payload hashes match.</summary>
    /// <param name="left">The first hash.</param>
    /// <param name="right">The second hash.</param>
    /// <returns>Whether the hashes match.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool HashEquals(string left, string right)
    {
        var leftBytes = Encoding.UTF8.GetBytes(left);
        var rightBytes = Encoding.UTF8.GetBytes(right);
        var difference = leftBytes.Length ^ rightBytes.Length;
        var count = Math.Min(leftBytes.Length, rightBytes.Length);
        for (var index = 0; index < count; index++)
        {
            difference |= leftBytes[index] ^ rightBytes[index];
        }

        return difference == 0;
    }
}
