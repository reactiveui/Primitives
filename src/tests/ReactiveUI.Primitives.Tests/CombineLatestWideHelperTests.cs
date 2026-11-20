// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Internal;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Tests for <see cref="CombineLatestWideHelper"/>, <see cref="CombineLatestOctet{T1, T2, T3, T4, T5, T6, T7, T8}"/>,
/// and <see cref="CombineLatestOctetSlots{TResult, T1, T2, T3, T4, T5, T6, T7, T8}"/>.
/// </summary>
public sealed class CombineLatestWideHelperTests
{
    /// <summary>Slot 2 sample value.</summary>
    private const int Two = 2;

    /// <summary>Slot 3 sample value.</summary>
    private const int Three = 3;

    /// <summary>Slot 4 sample value.</summary>
    private const int Four = 4;

    /// <summary>Slot 5 sample value.</summary>
    private const int Five = 5;

    /// <summary>Slot 6 sample value.</summary>
    private const int Six = 6;

    /// <summary>Slot 7 sample value.</summary>
    private const int Seven = 7;

    /// <summary>Slot 8 sample value.</summary>
    private const int Eight = 8;

    /// <summary>Second octet start index.</summary>
    private const int Nine = 9;

    /// <summary>Expected sum of 1 through 8.</summary>
    private const int SumOneToEight = 36;

    /// <summary>Verifies <see cref="CombineLatestWideHelper.ValidateOctet{T1, T2, T3, T4, T5, T6, T7, T8}"/> reports the exact parameter name for null entries.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ValidateOctet_NullSource_ThrowsWithExpectedParameterName()
    {
        var s = Signal.Return(1);
        CombineLatestOctet<int, int, int, int, int, int, int, int> nullFirst = new(null!, s, s, s, s, s, s, s);
        CombineLatestOctet<int, int, int, int, int, int, int, int> nullEighthTail = new(s, s, s, s, s, s, s, null!);

        var ex1 = await Assert.That(() => CombineLatestWideHelper.ValidateOctet(nullFirst)).Throws<ArgumentNullException>();
        await Assert.That(ex1!.ParamName).IsEqualTo("source");

        var ex16 = await Assert.That(() => CombineLatestWideHelper.ValidateOctet(nullEighthTail, "source9", Nine)).Throws<ArgumentNullException>();
        await Assert.That(ex16!.ParamName).IsEqualTo("source16");
    }

    /// <summary>Verifies <see cref="CombineLatestWideHelper.AttachOctet{TResult, T1, T2, T3, T4, T5, T6, T7, T8}"/> wires all eight slots in order.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task AttachOctet_WiresAllEightSourcesInOrder()
    {
        CombineLatestOctet<int, int, int, int, int, int, int, int> octet = new(
            Signal.Return(1),
            Signal.Return(Two),
            Signal.Return(Three),
            Signal.Return(Four),
            Signal.Return(Five),
            Signal.Return(Six),
            Signal.Return(Seven),
            Signal.Return(Eight));

        var validated = CombineLatestWideHelper.ValidateOctet(octet);
        CombineLatestSignal<int> signal = new(coordinator =>
        {
            var slots = CombineLatestWideHelper.AttachOctet(coordinator, validated);
            return () => slots.Slot1.Value
                + slots.Slot2.Value
                + slots.Slot3.Value
                + slots.Slot4.Value
                + slots.Slot5.Value
                + slots.Slot6.Value
                + slots.Slot7.Value
                + slots.Slot8.Value;
        });

        RecordingWitness<int> observer = new();
        using var sub = signal.Subscribe(observer);

        await Assert.That(observer.Values.SequenceEqual([SumOneToEight])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);
    }
}
