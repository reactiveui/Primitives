// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the non-null filter's failure handling.</summary>
public class KeepNotNullWitnessTests
{
    /// <summary>A failed value callback propagates its exception and releases the source.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_ObserverThrows_DisposesSubscription()
    {
        RecordingDisposable subscription = new();
        using KeepNotNullWitness<string> witness = new(new ThrowingWitness<string>(throwOnNext: true));
        witness.SetSubscription(subscription);

        await Assert.That(() => witness.OnNext("value")).ThrowsExactly<InvalidOperationException>();
        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Errors pass through the filter and release the subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnError_ForwardsErrorAndDisposesSubscription()
    {
        RecordingWitness<string> observer = new();
        RecordingDisposable subscription = new();
        using KeepNotNullWitness<string> witness = new(observer);
        witness.SetSubscription(subscription);
        InvalidOperationException error = new("source");

        witness.OnError(error);

        await Assert.That(observer.Errors.Single()).IsSameReferenceAs(error);
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies <see cref="KeepNotNullSignal{T}"/> and <see cref="FusedKeepNotNullSignal{T}"/> list, live, reduction, and switch-fusion paths.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task KeepNotNullSignalAndFusedKeepNotNullSignal_HandleListLiveAndReductions()
    {
        await Assert.That(static () => new KeepNotNullSignal<string>(null!)).Throws<ArgumentNullException>();
        KeepNotNullSignal<string> listSignal = new(ReactiveUI.Primitives.Signals.Signal.FromEnumerable(["a", null, "b"]));
        await Assert.That(() => listSignal.Subscribe(null!)).Throws<ArgumentNullException>();

        RecordingWitness<string> listObserver = new();
        using var listSub = listSignal.Subscribe(listObserver);
        await Assert.That(listObserver.Values.SequenceEqual(["a", "b"])).IsTrue();
        await Assert.That(listObserver.Completed).IsEqualTo(1);

        using ReactiveUI.Primitives.Signals.Signal<string?> live = new();
        RecordingWitness<string> liveObserver = new();
        using (new KeepNotNullSignal<string>(live).Subscribe(liveObserver))
        {
            live.OnNext(null);
            live.OnNext("ok");
            live.OnCompleted();
        }

        await Assert.That(liveObserver.Values.SequenceEqual(["ok"])).IsTrue();
        await Assert.That(liveObserver.Completed).IsEqualTo(1);

        var empty = ReactiveUI.Primitives.Signals.Signal.Empty<string?>();
        var silent = ReactiveUI.Primitives.Signals.Signal.Silent<string?>();
        await Assert.That(FusedKeepNotNullSignal<string>.Create(empty))
            .IsSameReferenceAs(ReactiveUI.Primitives.Signals.Signal.Empty<string>());
        await Assert.That(FusedKeepNotNullSignal<string>.Create(silent))
            .IsSameReferenceAs(ReactiveUI.Primitives.Signals.Signal.Silent<string>());

        FusedKeepNotNullSignal<string> fusedList = new(ReactiveUI.Primitives.Signals.Signal.FromEnumerable(["x", null, "y"]));
        await Assert.That(FusedKeepNotNullSignal<string>.Create(fusedList)).IsSameReferenceAs(fusedList);
        await Assert.That(fusedList.IsRequiredSubscribeOnCurrentThread()).IsFalse();

        RecordingWitness<string> fusedObserver = new();
        using var fusedSub = fusedList.Subscribe(fusedObserver);
        await Assert.That(fusedObserver.Values.SequenceEqual(["x", "y"])).IsTrue();
        await Assert.That(fusedObserver.Completed).IsEqualTo(1);

        var switched = SwitchSignal<int>.Create(
            FusedMapSignal<string, IObservable<int>>.Create(
                fusedList,
                static s => ReactiveUI.Primitives.Signals.Signal.Return(s.Length)));
        RecordingWitness<int> switchObserver = new();
        using var switchedSub = switched.Subscribe(switchObserver);
        await Assert.That(switchObserver.Values.SequenceEqual([1, 1])).IsTrue();
    }
}
