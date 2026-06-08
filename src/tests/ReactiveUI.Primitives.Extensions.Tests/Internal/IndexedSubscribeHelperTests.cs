// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Tests.Internal;

/// <summary>Tests for <see cref="IndexedSubscribeHelper"/>, the shared indexed-subscribe loop used by the synchronous combine-then-reduce operator family.</summary>
public class IndexedSubscribeHelperTests
{
    /// <summary>Value emitted by source 0 in the index-threading test.</summary>
    private const int Source0Value = 20;

    /// <summary>Value emitted by source 1 in the index-threading test.</summary>
    private const int Source1Value = 10;

    /// <summary>Value emitted by source 2 in the index-threading test.</summary>
    private const int Source2Value = 30;

    /// <summary>First post-subscribe value used by the dispose-guard test.</summary>
    private const int DisposeValue1 = 1;

    /// <summary>Post-dispose value used by the dispose-guard test.</summary>
    private const int DisposeValue2 = 2;

    /// <summary>Post-dispose value emitted by the second source in the dispose-guard test.</summary>
    private const int DisposeValue3 = 3;

    /// <summary>Verifies that the helper threads each source's positional index through to the OnNext hook.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourcesEmit_ThenOnNextReceivesPerSourceIndex()
    {
        using var s0 = new Subject<int>();
        using var s1 = new Subject<int>();
        using var s2 = new Subject<int>();
        var captured = new List<(int Index, int Value)>();

        using var sub = IndexedSubscribeHelper.SubscribeIndexed(
            [s0, s1, s2],
            (i, v) => captured.Add((i, v)),
            static _ => { },
            static _ => { });

        s1.OnNext(Source1Value);
        s0.OnNext(Source0Value);
        s2.OnNext(Source2Value);

        const int FirstIndex = 0;
        const int SecondIndex = 1;
        const int ThirdIndex = 2;
        await Assert.That(captured).IsCollectionEqualTo(
            [(SecondIndex, Source1Value), (FirstIndex, Source0Value), (ThirdIndex, Source2Value)]);
    }

    /// <summary>Verifies that the helper forwards any source's error through the shared OnError hook.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAnySourceErrors_ThenOnErrorForwarded()
    {
        using var s0 = new Subject<int>();
        using var s1 = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException("source error");

        using var sub = IndexedSubscribeHelper.SubscribeIndexed(
            [s0, s1],
            static (_, _) => { },
            ex => caught = ex,
            static _ => { });

        s1.OnError(expected);

        await Assert.That(caught).IsEqualTo(expected);
    }

    /// <summary>Verifies that the helper threads each source's positional index through to the OnCompleted hook.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourcesComplete_ThenOnCompletedReceivesPerSourceIndex()
    {
        using var s0 = new Subject<int>();
        using var s1 = new Subject<int>();
        var completed = new List<int>();

        using var sub = IndexedSubscribeHelper.SubscribeIndexed(
            [s0, s1],
            static (_, _) => { },
            static _ => { },
            completed.Add);

        const int FirstIndex = 0;
        const int SecondIndex = 1;
        s1.OnCompleted();
        s0.OnCompleted();

        await Assert.That(completed).IsCollectionEqualTo([SecondIndex, FirstIndex]);
    }

    /// <summary>Verifies that disposing the returned aggregate stops further deliveries from every source.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposed_ThenNoFurtherDeliveries()
    {
        using var s0 = new Subject<int>();
        using var s1 = new Subject<int>();
        var captured = new List<(int Index, int Value)>();

        var sub = IndexedSubscribeHelper.SubscribeIndexed(
            [s0, s1],
            (i, v) => captured.Add((i, v)),
            static _ => { },
            static _ => { });

        s0.OnNext(DisposeValue1);
        sub.Dispose();
        s0.OnNext(DisposeValue2);
        s1.OnNext(DisposeValue3);

        await Assert.That(captured).IsCollectionEqualTo([(0, DisposeValue1)]);
    }

    /// <summary>Verifies that the helper validates its null arguments and surfaces a meaningful exception.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenNullArgs_ThenArgumentNullException()
    {
        const IReadOnlyList<IObservable<int>> NullSources = null!;
        var ex = Assert.Throws<ArgumentNullException>(() =>
            IndexedSubscribeHelper.SubscribeIndexed(NullSources, static (_, _) => { }, static _ => { }, static _ => { }));

        await Assert.That(ex).IsNotNull();
    }
}
