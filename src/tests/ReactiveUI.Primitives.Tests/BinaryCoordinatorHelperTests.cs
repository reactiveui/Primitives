// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Internal;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="BinaryCoordinatorHelper"/> and <see cref="BinaryIntRanges{TResult}"/>.</summary>
public sealed class BinaryCoordinatorHelperTests
{
    /// <summary>Sample count of two.</summary>
    private const int Two = 2;

    /// <summary>Sample count of three.</summary>
    private const int Three = 3;

    /// <summary>Left sample input.</summary>
    private const int Four = 4;

    /// <summary>Right sample input.</summary>
    private const int Five = 5;

    /// <summary>Expected sum of four and five.</summary>
    private const int Nine = 9;

    /// <summary>Verifies <see cref="BinaryCoordinatorHelper.ValidateAndTryMatchRanges{TLeft, TRight, TResult}"/> and <see cref="BinaryCoordinatorHelper.TryCompleteBothSides"/>.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ValidateAndTryMatchRanges_And_TryCompleteBothSides_WorkAsExpected()
    {
        var matched = BinaryCoordinatorHelper.ValidateAndTryMatchRanges(
            Signal.Sequence(1, Three),
            Signal.Sequence(Two, Three),
            static (a, b) => a + b,
            out var ranges);
        await Assert.That(matched).IsTrue();
        await Assert.That(ranges.Selector(Four, Five)).IsEqualTo(Nine);

        var unmatched = BinaryCoordinatorHelper.ValidateAndTryMatchRanges(
            Signal.Return(1),
            Signal.Sequence(Two, Three),
            static (a, b) => a + b,
            out _);
        await Assert.That(unmatched).IsFalse();

        var leftDone = 0;
        var rightDone = 0;
        var completedSides = 0;
        await Assert.That(BinaryCoordinatorHelper.TryCompleteBothSides(ref leftDone, ref completedSides)).IsFalse();
        await Assert.That(BinaryCoordinatorHelper.TryCompleteBothSides(ref leftDone, ref completedSides)).IsFalse();
        await Assert.That(BinaryCoordinatorHelper.TryCompleteBothSides(ref rightDone, ref completedSides)).IsTrue();
    }

    /// <summary>Verifies <see cref="BinaryCoordinatorHelper.SubscribeArms{TOwner, TFirst, TSecond}"/> honors early stop and <c>DisposeArmOnCompleted</c>.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SubscribeArms_HonorsEarlyStopAndDisposeOnComplete()
    {
        var firstSubDisposals = 0;
        var secondSubscribed = false;
        IObserver<int>? firstObs = null;
        var first = Signal.Create<int>(obs =>
        {
            firstObs = obs;
            return new ActionDisposable(() => firstSubDisposals++);
        });
        var second = Signal.Create<int>(_ =>
        {
            secondSubscribed = true;
            return EmptyDisposable.Instance;
        });

        ArmState state = new() { Stopped = true };
        using var subs = BinaryCoordinatorHelper.SubscribeArms(
            state,
            out var assignedSlot,
            first,
            new(static (s, v) => s.Sum += v, static (_, _) => { }, static _ => { }, DisposeArmOnCompleted: true),
            second,
            new(static (s, v) => s.Sum += v, static (_, _) => { }, static _ => { }),
            static s => s.Stopped);

        await Assert.That(assignedSlot).IsSameReferenceAs(subs);
        await Assert.That(secondSubscribed).IsFalse();

        firstObs!.OnNext(Three);
        firstObs.OnCompleted();
        await Assert.That(state.Sum).IsEqualTo(Three);
        await Assert.That(firstSubDisposals).IsEqualTo(1);
    }

    /// <summary>Verifies <see cref="BinaryCoordinatorHelper.DrainPending{TOwner, TItem, TOutput}"/> drains queued items, delivers terminal notifications, and disposes subscriptions.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DrainPending_DrainsValuesAndDeliversTerminalWithCleanup()
    {
        var pending = BinaryCoordinatorHelper.CreatePending<int>();
        _ = pending.TryEnqueue(1);
        _ = pending.TryEnqueue(Two);
        _ = pending.TryRequestTerminal(null);

        ArmState owner = new();
        RecordingWitness<int> observer = new();
        var subDisposed = 0;
        ActionDisposable sub = new(() => subDisposed++);

        BinaryCoordinatorHelper.DrainPending(
            ref pending,
            owner,
            static (s, item) => s.Sum += item,
            observer,
            sub);

        await Assert.That(owner.Sum).IsEqualTo(Three);
        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(subDisposed).IsEqualTo(1);

        var faultedPending = BinaryCoordinatorHelper.CreatePending<int>();
        InvalidOperationException boom = new("boom");
        _ = faultedPending.TryRequestTerminal(boom);
        RecordingWitness<int> faultObserver = new();
        var faultSubDisposed = 0;

        BinaryCoordinatorHelper.DrainPending(
            ref faultedPending,
            owner,
            static (_, _) => { },
            faultObserver,
            new ActionDisposable(() => faultSubDisposed++));

        await Assert.That(faultObserver.Errors.SequenceEqual([boom])).IsTrue();
        await Assert.That(faultSubDisposed).IsEqualTo(1);
    }

    /// <summary>Mutable test state for arm callbacks.</summary>
    private sealed class ArmState
    {
        /// <summary>Gets or sets the accumulated sum.</summary>
        public int Sum { get; set; }

        /// <summary>Gets or sets a value indicating whether the coordinator is stopped.</summary>
        public bool Stopped { get; set; }
    }
}
