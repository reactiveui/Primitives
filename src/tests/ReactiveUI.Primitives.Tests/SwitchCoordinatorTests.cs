// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests generation changes and notification ordering in the switch coordinator.</summary>
public sealed class SwitchCoordinatorTests
{
    /// <summary>The value emitted by the replacement source.</summary>
    private const int ReplacementValue = 2;

    /// <summary>The value emitted by a superseded source.</summary>
    private const int StaleValue = 3;

    /// <summary>Switching preserves ordered delivery while callbacks own the coordinator gate.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SwitchTo_SwitchBetweenValues_HoldsGateDuringDelivery()
    {
        Signal<IObservable<int>> outer = new();
        CapturingObservable first = new();
        CapturingObservable second = new();
        List<int> values = [];
        LinqExtensions.SwitchCoordinator<int>? coordinator = null;
        var ownsGate = true;
        using var subscription = outer.SwitchTo().Subscribe(value =>
        {
            ownsGate &= IsHeld(coordinator!.Gate);
            values.Add(value);
        });
        coordinator = (LinqExtensions.SwitchCoordinator<int>)subscription;

        outer.OnNext(first);
        first.Observer!.OnNext(1);
        outer.OnNext(second);
        first.Observer.OnNext(StaleValue);
        second.Observer!.OnNext(ReplacementValue);

        await Assert.That(ownsGate).IsTrue();
        await Assert.That(values.SequenceEqual([1, ReplacementValue])).IsTrue();
    }

    /// <summary>A value accepted before a switch is retained, and later values from that generation are dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TryBeginSource_BetweenInnerValues_RejectsPreviousGeneration()
    {
        RecordingWitness<int> observer = new();
        using LinqExtensions.SwitchCoordinator<int> coordinator = new(observer);

        await Assert.That(coordinator.TryBeginSource(out var first)).IsTrue();
        coordinator.OnNext(first, 1);
        await Assert.That(coordinator.TryBeginSource(out var second)).IsTrue();
        coordinator.OnNext(first, StaleValue);
        coordinator.OnError(first, new InvalidOperationException("stale"));
        coordinator.OnCompleted(first);
        coordinator.OnOuterCompleted();

        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(observer.Errors).IsEmpty();

        coordinator.OnNext(second, ReplacementValue);
        coordinator.OnCompleted(second);

        await Assert.That(observer.Values.SequenceEqual([1, ReplacementValue])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);
    }

    /// <summary>Completion waits for both sources and rejects subsequent values and source generations.</summary>
    /// <param name="outerFirst">True when the outer source completes before the active inner source.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task OnCompleted_SourceOrder_CompletesOnce(bool outerFirst)
    {
        RecordingWitness<int> observer = new();
        using LinqExtensions.SwitchCoordinator<int> coordinator = new(observer);
        await Assert.That(coordinator.TryBeginSource(out var version)).IsTrue();

        if (outerFirst)
        {
            coordinator.OnOuterCompleted();
        }
        else
        {
            coordinator.OnCompleted(version);
        }

        await Assert.That(observer.Completed).IsEqualTo(0);

        if (outerFirst)
        {
            coordinator.OnCompleted(version);
        }
        else
        {
            coordinator.OnOuterCompleted();
        }

        coordinator.OnCompleted(version);
        coordinator.OnOuterCompleted();
        coordinator.OnNext(version, 1);
        coordinator.OnError(version, new InvalidOperationException("late inner"));
        coordinator.OnOuterError(new InvalidOperationException("late outer"));

        await Assert.That(coordinator.TryBeginSource(out _)).IsFalse();
        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Errors).IsEmpty();
    }

    /// <summary>The first source error is forwarded and prevents subsequent errors, values, and completion.</summary>
    /// <param name="outerFirst">True when the outer source reports the first error.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task OnError_SourceOrder_ForwardsFirstError(bool outerFirst)
    {
        RecordingWitness<int> observer = new();
        using LinqExtensions.SwitchCoordinator<int> coordinator = new(observer);
        await Assert.That(coordinator.TryBeginSource(out var version)).IsTrue();
        InvalidOperationException first = new("first");
        InvalidOperationException second = new("second");

        if (outerFirst)
        {
            coordinator.OnOuterError(first);
            coordinator.OnError(version, second);
        }
        else
        {
            coordinator.OnError(version, first);
            coordinator.OnOuterError(second);
        }

        coordinator.OnNext(version, 1);
        coordinator.OnCompleted(version);
        coordinator.OnOuterCompleted();

        await Assert.That(coordinator.TryBeginSource(out _)).IsFalse();
        await Assert.That(observer.Errors).HasSingleItem();
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(first);
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>Reports whether the calling thread owns the gate.</summary>
    /// <param name="gate">The coordinator synchronization gate.</param>
    /// <returns>True when the calling thread owns the gate; otherwise, false.</returns>
    private static bool IsHeld(Lock gate)
    {
#if NET9_0_OR_GREATER
        return gate.IsHeldByCurrentThread;
#else
        return Monitor.IsEntered(gate);
#endif
    }

    /// <summary>Retains the inner observer for notifications after a source switch.</summary>
    private sealed class CapturingObservable : IObservable<int>
    {
        /// <summary>Gets the observer captured by subscription.</summary>
        public IObserver<int>? Observer { get; private set; }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<int> observer)
        {
            Observer = observer;
            return EmptyDisposable.Instance;
        }
    }
}
