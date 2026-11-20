// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="CoreProjectedObserverWitness{TSource, TContext, TResult}"/>.</summary>
public sealed class CoreProjectedObserverWitnessTests
{
    /// <summary>Projection factor.</summary>
    private const int Factor = 3;

    /// <summary>Sample input.</summary>
    private const int Input = 4;

    /// <summary>Verifies <see cref="CoreProjectedObserverWitness{TSource, TContext, TResult}"/> projects active items and enforces single terminal delivery.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNextAndOnError_ProjectsActiveItemsAndEnforcesTerminal()
    {
        var upstreamDisposals = 0;
        RecordingWitness<int> downstream = new();
        CoreProjectedObserverWitness<int, int, int> witness = new(
            downstream,
            Factor,
            static (value, factor) => value * factor);
        witness.SetSubscription(new ActionDisposable(() => upstreamDisposals++));

        witness.OnNext(Input);
        InvalidOperationException expected = new("upstream failed");
        witness.OnError(expected);
        witness.OnCompleted();
        witness.OnNext(Input);

        await Assert.That(downstream.Values.SequenceEqual([Input * Factor])).IsTrue();
        await Assert.That(downstream.Errors.SequenceEqual([expected])).IsTrue();
        await Assert.That(downstream.Completed).IsEqualTo(0);
        await Assert.That(upstreamDisposals).IsEqualTo(1);
    }

    /// <summary>Verifies projection exceptions fault downstream and explicit disposal releases the subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SelectorThrowsAndExplicitDispose_ReleasesSubscription()
    {
        var upstreamDisposals = 0;
        RecordingWitness<string> downstream = new();
        CoreProjectedObserverWitness<int, int, string> witness = new(
            downstream,
            0,
            static (_, _) => throw new InvalidOperationException("selector failed"));
        witness.SetSubscription(new ActionDisposable(() => upstreamDisposals++));

        witness.OnNext(1);
        witness.Dispose();

        await Assert.That(downstream.Errors.Count).IsEqualTo(1);
        await Assert.That(upstreamDisposals).IsEqualTo(1);
    }
}
