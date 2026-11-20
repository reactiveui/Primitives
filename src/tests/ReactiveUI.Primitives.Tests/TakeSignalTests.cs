// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="TakeSignal{T}"/> and <c>Take</c> reductions.</summary>
public sealed class TakeSignalTests
{
    /// <summary>Reentrant emission value.</summary>
    private const int SecondValue = 2;

    /// <summary>Outer take limit.</summary>
    private const int OuterTake = 5;

    /// <summary>Inner take limit.</summary>
    private const int InnerTake = 3;

    /// <summary>Source range length.</summary>
    private const int SourceCount = 8;

    /// <summary>Take(1) delivers at most one value and one completion under synchronous reentrant OnNext.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_TakeOneReentrantOnNext_DeliversSingleValueAndCompletes()
    {
        using Signal<int> subject = new();
        List<int> values = [];
        var completions = 0;

        using var sub = subject.Take(1).Subscribe(
            x =>
            {
                values.Add(x);
                subject.OnNext(SecondValue);
            },
            static _ => { },
            () => completions++);

        subject.OnNext(1);

        await Assert.That(values.SequenceEqual([1])).IsTrue();
        await Assert.That(completions).IsEqualTo(1);
    }

    /// <summary>Take(0) returns the empty singleton without subscribing to the upstream source.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Take_ZeroCount_ReturnsEmptyWithoutSubscribing()
    {
        var subscribed = 0;
        var cold = Signal.Defer(() =>
        {
            subscribed++;
            return Signal.Sequence(1, SourceCount);
        });

        var taken = cold.Take(0);
        RecordingWitness<int> recorder = new();
        _ = taken.Subscribe(recorder);

        await Assert.That(taken).IsSameReferenceAs(ImmutableEmptySignal<int>.Instance);
        await Assert.That(subscribed).IsEqualTo(0);
        await Assert.That(recorder.Completed).IsEqualTo(1);
    }

    /// <summary>Chained Take stages collapse to the smaller count.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Take_ChainedStages_CapsCountToMinimum()
    {
        using Signal<int> subject = new();
        List<int> values = [];
        using var sub = subject.Take(OuterTake).Take(InnerTake).Subscribe(values.Add);

        for (var i = 1; i <= SourceCount; i++)
        {
            subject.OnNext(i);
        }

        await Assert.That(values.Count).IsEqualTo(InnerTake);
    }
}
