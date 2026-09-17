// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests that safe witnesses stop after one terminal signal or a throwing value callback.</summary>
public class SafeWitnessTests
{
    /// <summary>The first value pushed at a witness.</summary>
    private const int FirstValue = 1;

    /// <summary>A value pushed after the witness has terminated; it must never be delivered.</summary>
    private const int LateValue = 2;

    /// <summary>The cancel-owning wrapper forwards the first fault, disposes its cancel resource once, then drops everything after.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CancelOwningSafeWitnessForwardsTheFirstFaultThenGoesQuiet()
    {
        RecordingWitness<int> observer = new();
        var cancelled = 0;
        var safe = Witness.Safe(observer, new ActionDisposable(() => cancelled++));
        InvalidOperationException error = new("safe-fault");

        safe.OnError(error);
        safe.OnError(new InvalidOperationException("late"));
        safe.OnCompleted();
        safe.OnNext(LateValue);

        await Assert.That(observer.Errors.Count).IsEqualTo(1);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(error);
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(cancelled).IsEqualTo(1);
    }

    /// <summary>A delegate witness made safe by <see cref="Witness.Safe{T}(IObserver{T})"/> forwards the first fault exactly once, then ignores the rest.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DelegateWitnessMadeSafeForwardsTheFirstFaultThenGoesQuiet()
    {
        List<int> values = [];
        List<Exception> errors = [];
        var completed = 0;
        var safe = Witness.Safe(Witness.Create<int>(values.Add, errors.Add, () => completed++));
        InvalidOperationException error = new("delegate-fault");

        safe.OnError(error);
        safe.OnError(new InvalidOperationException("late"));
        safe.OnCompleted();
        safe.OnNext(LateValue);

        await Assert.That(errors.Count).IsEqualTo(1);
        await Assert.That(errors[0]).IsSameReferenceAs(error);
        await Assert.That(completed).IsEqualTo(0);
        await Assert.That(values.Count).IsEqualTo(0);
    }

    /// <summary>A throwing value callback stops the safe delegate witness: the exception surfaces once and the next value is dropped.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DelegateWitnessMadeSafeStopsAfterItsValueCallbackThrows()
    {
        var calls = 0;
        var safe = Witness.Safe(Witness.Create<int>(_ =>
        {
            calls++;
            throw new InvalidOperationException("next-failed");
        }));

        var thrown = Assert.Throws<InvalidOperationException>(() => safe.OnNext(FirstValue));

        // A second push must not reach the callback, so it must not throw either.
        safe.OnNext(LateValue);

        await Assert.That(thrown!.Message).IsEqualTo("next-failed");
        await Assert.That(calls).IsEqualTo(1);
    }

    /// <summary>The cancel-free wrapper forwards values, completes exactly once, and drops notifications after the completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CancelFreeSafeWitnessForwardsValuesThenCompletesExactlyOnce()
    {
        RecordingWitness<int> observer = new();
        var safe = Witness.Safe(observer);

        safe.OnNext(FirstValue);
        safe.OnCompleted();
        safe.OnCompleted();
        safe.OnNext(LateValue);
        safe.OnError(new InvalidOperationException("late"));

        await Assert.That(observer.Values.SequenceEqual([FirstValue])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
    }

    /// <summary>The cancel-free wrapper forwards the first fault and suppresses every notification after it.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CancelFreeSafeWitnessForwardsTheFirstFaultThenGoesQuiet()
    {
        RecordingWitness<int> observer = new();
        var safe = Witness.Safe(observer);
        InvalidOperationException error = new("no-cancel-fault");

        safe.OnError(error);
        safe.OnError(new InvalidOperationException("late"));
        safe.OnCompleted();
        safe.OnNext(LateValue);

        await Assert.That(observer.Errors.Count).IsEqualTo(1);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(error);
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(observer.Values.Count).IsEqualTo(0);
    }

    /// <summary>A throwing observer stops the cancel-free wrapper: the exception surfaces once and the next value is dropped.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CancelFreeSafeWitnessStopsAfterTheObserverThrows()
    {
        var safe = Witness.Safe(new ThrowingWitness<int>(throwOnNext: true));

        var thrown = Assert.Throws<InvalidOperationException>(() => safe.OnNext(FirstValue));

        // The witness has latched stopped, so this must be a silent no-op rather than a second throw.
        safe.OnNext(LateValue);

        await Assert.That(thrown!.Message).IsEqualTo("observer-next");
    }
}
