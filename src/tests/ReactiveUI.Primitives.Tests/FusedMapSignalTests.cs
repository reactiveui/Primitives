// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="FusedMapSignal{TSource, TResult}"/>.</summary>
public sealed class FusedMapSignalTests
{
    /// <summary>Second source element.</summary>
    private const int Two = 2;

    /// <summary>Third source element.</summary>
    private const int Three = 3;

    /// <summary>Empty and Never singletons propagate directly through Map.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Map_EmptyAndNeverSources_PropagateSingletons()
    {
        await Assert.That(Signal.None<int>().Map(static x => x.ToString(CultureInfo.InvariantCulture)))
            .IsSameReferenceAs(ImmutableEmptySignal<string>.Instance);
        await Assert.That(ImmutableNeverSignal<int>.Instance.Map(static x => x.ToString(CultureInfo.InvariantCulture)))
            .IsSameReferenceAs(ImmutableNeverSignal<string>.Instance);
    }

    /// <summary>Mutating the backing list during FusedMapSignal list enumeration stops cleanly at the updated count.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_ObserverShrinksBackingListDuringEmission_CompletesWithoutError()
    {
        List<int> backing = [1, Two, Three];
        RecordingWitness<int> downstream = new();
        _ = Signal.FromEnumerable(backing)
            .Map(static x => x + 1)
            .Subscribe(
                x =>
                {
                    downstream.OnNext(x);
                    backing.Clear();
                },
                downstream.OnError,
                downstream.OnCompleted);

        await Assert.That(downstream.Values.SequenceEqual([Two])).IsTrue();
        await Assert.That(downstream.Errors).IsEmpty();
        await Assert.That(downstream.Completed).IsEqualTo(1);
    }
}
