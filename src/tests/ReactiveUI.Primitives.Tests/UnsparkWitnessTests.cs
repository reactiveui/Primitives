// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests dematerialized notification teardown.</summary>
public sealed class UnsparkWitnessTests
{
    /// <summary>An error in the notification source is forwarded and releases its subscription.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task OnError_WhenSubscribed_ThenForwardsTheErrorAndDisposes()
    {
        RecordingWitness<int> observer = new();
        using UnsparkWitness<int> witness = new(observer);
        BooleanDisposable subscription = new();
        InvalidOperationException error = new("source failure");
        witness.SetSubscription(subscription);

        witness.OnError(error);

        await Assert.That(observer.Errors.Count).IsEqualTo(1);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(error);
        await Assert.That(subscription.IsDisposed).IsTrue();
    }

    /// <summary>An error spark delivers a single terminal error and disposes the upstream subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_ErrorSparkFollowedByCompletion_DeliversSingleErrorAndDisposesUpstream()
    {
        var disposals = 0;
        IObserver<Spark<int>>? upstream = null;
        var source = Signal.Create<Spark<int>>(obs =>
        {
            upstream = obs;
            return new ActionDisposable(() => disposals++);
        });
        RecordingWitness<int> observer = new();
        InvalidOperationException failure = new("spark-error");

        _ = source.Unspark().Subscribe(observer);
        upstream!.OnNext(Spark.CreateOnError<int>(failure));
        upstream.OnCompleted();

        await Assert.That(disposals).IsEqualTo(1);
        await Assert.That(observer.Errors.Count).IsEqualTo(1);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(failure);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>Spark followed by Unspark on a custom source still blocks misbehaving emissions after completion.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Unspark_AfterSparkOnMisbehavingSource_SuppressesPostTerminalValues()
    {
        var misbehaving = Signal.Create<int>(static obs =>
        {
            obs.OnNext(1);
            obs.OnCompleted();
            obs.OnNext(1);
            obs.OnCompleted();
            return EmptyDisposable.Instance;
        });

        RecordingWitness<int> observer = new();
        _ = misbehaving.Spark().Unspark().Subscribe(observer);

        await Assert.That(observer.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);
    }

    /// <summary>Spark followed by Unspark on a built-in range signal unwraps the range directly.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Unspark_AfterSparkOnRangeSignal_ReturnsOriginalRange()
    {
        const int Count = 3;
        var range = Signal.Sequence(1, Count);
        await Assert.That(range.Spark().Unspark()).IsSameReferenceAs(range);
    }
}
