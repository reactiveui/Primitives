// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Tests for <see cref="CrdtLoopbackORSetProjection"/>.</summary>
public sealed class CrdtLoopbackORSetProjectionTests
{
    /// <summary>The first client identifier.</summary>
    private const string ClientAId = "device-a";

    /// <summary>The second client identifier.</summary>
    private const string ClientBId = "device-b";

    /// <summary>The blue element.</summary>
    private const string Blue = "blue";

    /// <summary>The red element.</summary>
    private const string Red = "red";

    /// <summary>The missing element.</summary>
    private const string Green = "green";

    /// <summary>The second observed sequence.</summary>
    private const int SecondSequence = 2;

    /// <summary>Verifies empty OR-set state is displayed without inventing an element.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetElementDisplayReturnsEmptyTextForEmptySet()
    {
        var actual = CrdtLoopbackORSetProjection.GetElementDisplay(CreateState([]));

        await Assert.That(actual).IsEqualTo(string.Empty);
    }

    /// <summary>Verifies multiple OR-set elements are displayed deterministically.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetElementDisplayReturnsSortedTextForMultipleElements()
    {
        var state = CreateState([CreateBinding(Red, ClientAId, 1), CreateBinding(Blue, ClientBId, 1)]);

        var actual = CrdtLoopbackORSetProjection.GetElementDisplay(state);

        await Assert.That(actual).IsEqualTo("blue,red");
    }

    /// <summary>Verifies observed remove uses an actually observed dot.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FindObservedDotReturnsMatchingElementDot()
    {
        var expected = CreateDot(ClientBId, SecondSequence);
        var state = CreateState([CreateBinding(Red, ClientAId, 1), CreateBinding(Blue, expected)]);

        var actual = CrdtLoopbackORSetProjection.FindObservedDot(state, Blue);

        await Assert.That(actual).IsEqualTo(expected);
    }

    /// <summary>Verifies a missing observed dot fails before an observed remove can be submitted.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FindObservedDotThrowsWhenElementWasNotObserved()
    {
        var state = CreateState([CreateBinding(Red, ClientAId, 1)]);

        var exception = await Assert.That(() => CrdtLoopbackORSetProjection.FindObservedDot(state, Green))
            .ThrowsExactly<InvalidOperationException>();

        await Assert.That(exception?.Message).Contains(Green);
    }

    /// <summary>Creates an OR-set state.</summary>
    /// <param name="bindings">The dot bindings.</param>
    /// <returns>The CRDT state.</returns>
    private static CrdtState CreateState(IReadOnlyList<CrdtDotElement> bindings) =>
        new() { Kind = CrdtKind.ORSet, DotBindings = bindings };

    /// <summary>Creates a dot binding.</summary>
    /// <param name="element">The element text.</param>
    /// <param name="clientId">The client identifier.</param>
    /// <param name="sequence">The client sequence.</param>
    /// <returns>The dot binding.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static CrdtDotElement CreateBinding(string element, string clientId, long sequence) =>
        CreateBinding(element, CreateDot(clientId, sequence));

    /// <summary>Creates a dot binding.</summary>
    /// <param name="element">The element text.</param>
    /// <param name="dot">The dot.</param>
    /// <returns>The dot binding.</returns>
    private static CrdtDotElement CreateBinding(string element, CrdtDot dot) =>
        new() { Dot = dot, Element = Encoding.UTF8.GetBytes(element) };

    /// <summary>Creates a CRDT dot.</summary>
    /// <param name="clientId">The client identifier.</param>
    /// <param name="sequence">The sequence.</param>
    /// <returns>The CRDT dot.</returns>
    private static CrdtDot CreateDot(string clientId, long sequence) =>
        new() { ClientId = clientId, ClientSequence = sequence };
}
