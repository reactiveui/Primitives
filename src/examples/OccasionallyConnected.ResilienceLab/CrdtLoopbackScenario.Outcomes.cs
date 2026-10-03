// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Runs the bounded in-memory CRDT loopback demonstration.</summary>
internal static partial class CrdtLoopbackScenario
{
    /// <summary>Gets a counter value from a received authoritative state.</summary>
    /// <param name="state">The authoritative state.</param>
    /// <returns>The counter value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetCounterValue(CrdtState state) =>
        checked((int)state.Value.Counter);

    /// <summary>Gets the LWW register value as text.</summary>
    /// <param name="state">The LWW state.</param>
    /// <returns>The register text.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string GetRegisterString(CrdtState state) =>
        FromBytes(state.Value.Bytes);

    /// <summary>Appends an expected-versus-actual invariant case.</summary>
    /// <param name="cases">The case list.</param>
    /// <param name="name">The case name.</param>
    /// <param name="expected">The expected value.</param>
    /// <param name="actual">The actual value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AppendCase(List<ResilienceLabCaseResult> cases, string name, object expected, object actual) =>
        cases.Add(new(name, expected, actual, Equals(expected, actual)));

    /// <summary>Converts text to UTF-8 bytes.</summary>
    /// <param name="value">The text value.</param>
    /// <returns>The UTF-8 bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte[] ToBytes(string value) =>
        Encoding.UTF8.GetBytes(value);

    /// <summary>Converts UTF-8 bytes to text.</summary>
    /// <param name="value">The byte value.</param>
    /// <returns>The text value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string FromBytes(ReadOnlyMemory<byte> value) =>
        Encoding.UTF8.GetString(value.Span);

    /// <summary>Creates a deterministic GUID from a small positive seed.</summary>
    /// <param name="seed">The seed.</param>
    /// <returns>The deterministic GUID.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Guid CreateGuid(int seed) =>
        new(GuidPrefix + seed.ToString(GuidSeedFormat, CultureInfo.InvariantCulture));
}
