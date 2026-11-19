// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="MapWithSignal{TSource, TState, TResult}"/>.</summary>
public sealed class MapWithSignalTests
{
    /// <summary>Scale multiplier used in projection tests.</summary>
    private const int ScaleFactor = 10;

    /// <summary>Second source element.</summary>
    private const int Two = 2;

    /// <summary>Third source element.</summary>
    private const int Three = 3;

    /// <summary>Second projected element.</summary>
    private const int Twenty = 20;

    /// <summary>A throwing MapWith selector routes the exception to OnError and disposes upstream.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_SelectorThrows_ForwardsErrorAndDisposesUpstream()
    {
        var disposals = 0;
        IObserver<int>? upstream = null;
        var source = Signal.Create<int>(obs =>
        {
            upstream = obs;
            return new ActionDisposable(() => disposals++);
        });

        RecordingWitness<int> recorder = new();
        _ = source.MapWith(1, static int (_, _) => throw new InvalidOperationException("map")).Subscribe(recorder);
        upstream!.OnNext(1);

        await Assert.That(disposals).IsEqualTo(1);
        await Assert.That(recorder.Errors.Count).IsEqualTo(1);
    }

    /// <summary>FromEnumerable fast path projects elements with captured state without error.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_FromEnumerableSource_ProjectsValuesWithState()
    {
        List<int> observed = [];
        _ = Signal.FromEnumerable([1, Two]).MapWith(ScaleFactor, static (factor, x) => x * factor).Subscribe(observed.Add);
        await Assert.That(observed.SequenceEqual([ScaleFactor, Twenty])).IsTrue();
    }

    /// <summary>If the observer clears the backing list during OnNext, enumeration stops cleanly without Faulting.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_ObserverShrinksBackingListDuringEmission_CompletesCleanly()
    {
        List<int> backing = [1, Two, Three];
        RecordingWitness<int> downstream = new();
        _ = Signal.FromEnumerable(backing)
            .MapWith(ScaleFactor, static (factor, x) => x * factor)
            .Subscribe(
                x =>
                {
                    downstream.OnNext(x);
                    backing.Clear();
                },
                downstream.OnError,
                downstream.OnCompleted);

        await Assert.That(downstream.Values.SequenceEqual([ScaleFactor])).IsTrue();
        await Assert.That(downstream.Errors).IsEmpty();
        await Assert.That(downstream.Completed).IsEqualTo(1);
    }
}
