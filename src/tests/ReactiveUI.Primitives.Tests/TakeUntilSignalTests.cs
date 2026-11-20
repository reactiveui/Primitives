// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="TakeUntilSignal{T, TOther}"/> reductions and stop-first subscription ordering.</summary>
public sealed class TakeUntilSignalTests
{
    /// <summary>Verifies <c>TakeUntil</c> on an empty source subscribes a non-trivial <c>other</c> first, propagating immediate errors and disposing <c>other</c> on completion.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task EmptySource_WithNonTrivialOther_SubscribesOtherFirstAndPropagatesImmediateError()
    {
        InvalidOperationException expected = new("other failed immediately");
        RecordingWitness<int> faultObserver = new();
        using var faultSub = Signal.Empty<int>()
            .TakeUntil(Signal.Throw<int>(expected))
            .Subscribe(faultObserver);

        await Assert.That(faultObserver.Errors.SequenceEqual([expected])).IsTrue();
        await Assert.That(faultObserver.Completed).IsEqualTo(0);

        TrackingOtherSignal liveOther = new();
        RecordingWitness<int> completeObserver = new();
        using var completeSub = Signal.Empty<int>()
            .TakeUntil(liveOther)
            .Subscribe(completeObserver);

        await Assert.That(liveOther.SubscribedCount).IsEqualTo(1);
        await Assert.That(liveOther.DisposedCount).IsEqualTo(1);
        await Assert.That(completeObserver.Completed).IsEqualTo(1);
        await Assert.That(completeObserver.Errors).IsEmpty();
    }

    /// <summary>Verifies <see cref="TakeUntilSignal{T, TOther}.Create"/> elides the wrapper when <c>other</c> is <c>Empty</c> or <c>Never</c> and <c>source</c> is a built-in signal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Create_WithEmptyOrNeverOther_ReturnsBuiltInSourceDirectly()
    {
        var emptySource = Signal.Empty<int>();
        var rangeSource = Signal.Sequence(1, 1);

        await Assert.That(TakeUntilSignal<int, int>.Create(emptySource, Signal.Silent<int>())).IsSameReferenceAs(emptySource);
        await Assert.That(TakeUntilSignal<int, int>.Create(rangeSource, Signal.Empty<int>())).IsSameReferenceAs(rangeSource);
    }

    /// <summary>Tracks subscription and disposal counts without emitting notifications.</summary>
    private sealed class TrackingOtherSignal : IObservable<int>, IDisposable
    {
        /// <summary>Gets the number of subscriptions received.</summary>
        public int SubscribedCount { get; private set; }

        /// <summary>Gets the number of subscription disposals observed.</summary>
        public int DisposedCount { get; private set; }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<int> observer)
        {
            SubscribedCount++;
            return this;
        }

        /// <inheritdoc/>
        public void Dispose() => DisposedCount++;
    }
}
