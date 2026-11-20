// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="KeepWithSignal{T, TState}"/>.</summary>
public sealed class KeepWithSignalTests
{
    /// <summary>Threshold value.</summary>
    private const int Threshold = 2;

    /// <summary>Third element above threshold.</summary>
    private const int Three = 3;

    /// <summary>A throwing KeepWith predicate routes the exception to OnError and disposes upstream.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_PredicateThrows_ForwardsErrorAndDisposesUpstream()
    {
        var disposals = 0;
        IObserver<int>? upstream = null;
        var source = Signal.Create<int>(obs =>
        {
            upstream = obs;
            return new ActionDisposable(() => disposals++);
        });

        RecordingWitness<int> recorder = new();
        _ = source.KeepWith(1, static bool (_, _) => throw new InvalidOperationException("keep")).Subscribe(recorder);
        upstream!.OnNext(1);

        await Assert.That(disposals).IsEqualTo(1);
        await Assert.That(recorder.Errors.Count).IsEqualTo(1);
    }

    /// <summary>FromEnumerable fast path filters elements with captured state.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_FromEnumerableSource_FiltersValuesWithState()
    {
        List<int> observed = [];
        _ = Signal.FromEnumerable([1, Threshold, Three]).KeepWith(Threshold, static (min, x) => x >= min).Subscribe(observed.Add);
        await Assert.That(observed.SequenceEqual([Threshold, Three])).IsTrue();
    }
}
