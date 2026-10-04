// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Tests for <see cref="CrdtLoopbackHistoryEvaluator"/>.</summary>
public sealed class CrdtLoopbackHistoryEvaluatorTests
{
    /// <summary>The first client identity.</summary>
    private const string ClientAId = "device-a";

    /// <summary>The second client identity.</summary>
    private const string ClientBId = "device-b";

    /// <summary>The expected counter value.</summary>
    private const int ExpectedCounter = 8;

    /// <summary>The first counter component value.</summary>
    private const int ClientAComponent = 5;

    /// <summary>The second counter component value.</summary>
    private const int ClientBComponent = 3;

    /// <summary>The divergent counter value.</summary>
    private const int DivergentCounter = 7;

    /// <summary>The zero counter value.</summary>
    private const int ZeroCounter = 0;

    /// <summary>The expected string value.</summary>
    private const string ExpectedString = "blue";

    /// <summary>The divergent string value.</summary>
    private const string DivergentString = "red";

    /// <summary>The first cursor.</summary>
    private const string ClientACursor = "cursor-a";

    /// <summary>The second cursor.</summary>
    private const string ClientBCursor = "cursor-b";

    /// <summary>The first event identifier.</summary>
    private static readonly Guid ClientAEventId = Guid.Parse("00000000-0000-0000-0000-000000000901");

    /// <summary>The second event identifier.</summary>
    private static readonly Guid ClientBEventId = Guid.Parse("00000000-0000-0000-0000-000000000902");

    /// <summary>Verifies both counter histories commute when both clients received equivalent state sequences.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EvaluateCounterReportsExpectedWhenBothClientHistoriesCommute()
    {
        var clientA = CreateReceivedStream(
            ClientACursor,
            CreateCounterState(ClientAId, ClientAComponent),
            CreateCounterState(ClientBId, ClientBComponent));
        var clientB = CreateReceivedStream(
            ClientBCursor,
            CreateCounterState(ClientBId, ClientBComponent),
            CreateCounterState(ClientAId, ClientAComponent));

        var actual = CrdtLoopbackHistoryEvaluator.EvaluateCounter(
            clientA,
            clientB,
            CrdtKind.GCounter,
            ExpectedCounter,
            CrdtBounds.Default);

        await Assert.That(actual).IsEqualTo(ExpectedCounter);
    }

    /// <summary>Verifies counter history mismatches from either client fail the commutativity check.</summary>
    /// <param name="clientACounter">The client A aggregate counter value.</param>
    /// <param name="clientBCounter">The client B aggregate counter value.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(DivergentCounter, ExpectedCounter)]
    [Arguments(ExpectedCounter, DivergentCounter)]
    public async Task EvaluateCounterReportsMismatchWhenEitherClientHistoryDiffers(
        int clientACounter,
        int clientBCounter)
    {
        var clientA = CreateReceivedStream(ClientACursor, CreateCounterState(ClientAId, clientACounter));
        var clientB = CreateReceivedStream(ClientBCursor, CreateCounterState(ClientBId, clientBCounter));

        var actual = CrdtLoopbackHistoryEvaluator.EvaluateCounter(
            clientA,
            clientB,
            CrdtKind.GCounter,
            ExpectedCounter,
            CrdtBounds.Default);

        await Assert.That(actual).IsEqualTo(DivergentCounter);
    }

    /// <summary>Verifies counter mismatches are not hidden when the expected value equals the current sentinel.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EvaluateCounterReportsActualMismatchWhenExpectedValueIsMinimumInteger()
    {
        var clientA = CreateReceivedStream(ClientACursor, CreateCounterState(ClientAId, ZeroCounter));
        var clientB = CreateReceivedStream(ClientBCursor, CreateCounterState(ClientBId, ZeroCounter));

        var actual = CrdtLoopbackHistoryEvaluator.EvaluateCounter(
            clientA,
            clientB,
            CrdtKind.GCounter,
            int.MinValue,
            CrdtBounds.Default);

        await Assert.That(actual).IsEqualTo(ZeroCounter);
    }

    /// <summary>Verifies a client B string history mismatch is reported even when client A matches the expectation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EvaluateStringReportsClientBMismatchWhenClientAHistoryMatchesExpected()
    {
        var clientA = CreateReceivedStream(ClientACursor, CreateRegisterState(ExpectedString));
        var clientB = CreateReceivedStream(ClientBCursor, CreateRegisterState(DivergentString));

        var actual = CrdtLoopbackHistoryEvaluator.EvaluateString(
            clientA,
            clientB,
            CrdtKind.LwwRegister,
            GetRegisterString,
            ExpectedString,
            CrdtBounds.Default);

        await Assert.That(actual).IsEqualTo(DivergentString);
    }

    /// <summary>Verifies a client A string history mismatch is reported directly.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EvaluateStringReportsClientAMismatchWhenClientBHistoryMatchesExpected()
    {
        var clientA = CreateReceivedStream(ClientACursor, CreateRegisterState(DivergentString));
        var clientB = CreateReceivedStream(ClientBCursor, CreateRegisterState(ExpectedString));

        var actual = CrdtLoopbackHistoryEvaluator.EvaluateString(
            clientA,
            clientB,
            CrdtKind.LwwRegister,
            GetRegisterString,
            ExpectedString,
            CrdtBounds.Default);

        await Assert.That(actual).IsEqualTo(DivergentString);
    }

    /// <summary>Creates one received stream page for the supplied authoritative states.</summary>
    /// <param name="cursor">The stream cursor.</param>
    /// <param name="states">The authoritative states.</param>
    /// <returns>The received stream.</returns>
    private static CrdtLoopbackReceivedStream CreateReceivedStream(string cursor, params CrdtState[] states) =>
        new(states, null, cursor, [ClientAEventId, ClientBEventId], states.Length, states.Length);

    /// <summary>Creates a G-counter state with one component.</summary>
    /// <param name="clientId">The client component identifier.</param>
    /// <param name="value">The component value.</param>
    /// <returns>The CRDT state.</returns>
    private static CrdtState CreateCounterState(string clientId, int value) =>
        new() { Kind = CrdtKind.GCounter, GCounterComponents = new Dictionary<string, long> { [clientId] = value } };

    /// <summary>Creates an LWW register state.</summary>
    /// <param name="value">The register text.</param>
    /// <returns>The CRDT state.</returns>
    private static CrdtState CreateRegisterState(string value) =>
        new() { Kind = CrdtKind.LwwRegister, RegisterValue = Encoding.UTF8.GetBytes(value), RegisterStamp = CreateStamp(value.Length) };

    /// <summary>Creates a deterministic register write stamp.</summary>
    /// <param name="ticks">The timestamp tick offset.</param>
    /// <returns>The write stamp.</returns>
    private static ConflictWriteStamp CreateStamp(int ticks) =>
        new() { CommittedAtUtc = DateTimeOffset.UnixEpoch.AddTicks(ticks), ClientId = ClientAId, OperationId = new(ClientAEventId) };

    /// <summary>Gets the LWW register value as text.</summary>
    /// <param name="state">The LWW state.</param>
    /// <returns>The register text.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string GetRegisterString(CrdtState state) =>
        Encoding.UTF8.GetString(state.Value.Bytes.Span);
}
