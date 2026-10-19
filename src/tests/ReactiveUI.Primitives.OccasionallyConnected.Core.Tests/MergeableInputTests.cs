// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests the factory-constrained mergeable cases.</summary>
public sealed class MergeableInputTests
{
    /// <summary>The test client identifier.</summary>
    private const string ClientId = "client";

    /// <summary>Verifies each input holds exactly one immutable case through a record copy.</summary>
    /// <param name="kind">The selected input kind.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(MergeableInputKind.Change)]
    [Arguments(MergeableInputKind.AuthoritativeState)]
    public async Task InputPreservesSingleCase(MergeableInputKind kind)
    {
        var input = kind == MergeableInputKind.Change
            ? MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(ClientId, 1))
            : MergeableInput.ForAuthoritativeState(MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter));
        var copy = input with { };
        var decoded = MergeableCodec.DecodeInput(MergeableCodec.EncodeInput(copy));
        await Assert.That(decoded.Kind).IsEqualTo(kind);
        await Assert.That(copy.Value).IsSameReferenceAs(input.Value);
    }
}
