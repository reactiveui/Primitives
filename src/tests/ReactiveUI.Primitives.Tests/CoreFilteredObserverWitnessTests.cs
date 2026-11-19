// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="CoreFilteredObserverWitness{TSource, TContext}"/>.</summary>
public sealed class CoreFilteredObserverWitnessTests
{
    /// <summary>Rejected value below threshold.</summary>
    private const int RejectedValue = 2;

    /// <summary>Minimum threshold for accepted values.</summary>
    private const int Threshold = 5;

    /// <summary>Accepted value above threshold.</summary>
    private const int AcceptedValue = 8;

    /// <summary>Verifies <see cref="CoreFilteredObserverWitness{TSource, TContext}"/> forwards matching values, suppresses after completion, and disposes its upstream handle.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNextAndComplete_FiltersItemsAndDisposesUpstream()
    {
        var upstreamDisposals = 0;
        RecordingWitness<int> downstream = new();
        CoreFilteredObserverWitness<int, int> witness = new(
            downstream,
            Threshold,
            static (value, min) => value >= min);
        witness.SetSubscription(new ActionDisposable(() => upstreamDisposals++));

        witness.OnNext(RejectedValue);
        witness.OnNext(AcceptedValue);
        witness.OnCompleted();
        witness.OnNext(AcceptedValue);
        witness.OnError(new InvalidOperationException("after-complete"));

        await Assert.That(downstream.Values.SequenceEqual([AcceptedValue])).IsTrue();
        await Assert.That(downstream.Completed).IsEqualTo(1);
        await Assert.That(downstream.Errors.Count).IsEqualTo(0);
        await Assert.That(upstreamDisposals).IsEqualTo(1);
    }

    /// <summary>Verifies predicate exceptions fault downstream and attaching after disposal disposes immediately.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task PredicateFaultAndLateSetSubscription_FaultsAndDisposesImmediately()
    {
        var upstreamDisposals = 0;
        RecordingWitness<int> downstream = new();
        CoreFilteredObserverWitness<int, int> witness = new(
            downstream,
            0,
            static (_, _) => throw new InvalidOperationException("predicate failed"));

        witness.OnNext(1);
        witness.SetSubscription(new ActionDisposable(() => upstreamDisposals++));

        await Assert.That(downstream.Errors.Count).IsEqualTo(1);
        await Assert.That(upstreamDisposals).IsEqualTo(1);
    }
}
