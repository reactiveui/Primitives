// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Verifies the three wrappers <see cref="Witness.Safe{T}(IObserver{T})"/> can hand back: the delegate
/// witness made safe in place, the cancel-owning safe wrapper, and the cancel-free safe wrapper. All three
/// promise the same thing — one terminal signal reaches the wrapped observer, nothing reaches it afterwards,
/// and a throwing value callback stops the witness for good.
/// </summary>
public class SafeWitnessTests
{
    /// <summary>The first value pushed at a witness.</summary>
    private const int FirstValue = 1;

    /// <summary>A value pushed after the witness has terminated; it must never be delivered.</summary>
    private const int LateValue = 2;

    /// <summary>
    /// The cancel-owning wrapper forwards the first fault, disposes its cancellation resource once, and drops
    /// every later notification. Faulting first (rather than completing first) is the path that leaves the
    /// wrapped observer holding an error it must not see twice.
    /// </summary>
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

    /// <summary>
    /// A delegate witness passed to <see cref="Witness.Safe{T}(IObserver{T})"/> is made safe in place rather
    /// than wrapped. It must still forward the first fault exactly once and ignore everything after it.
    /// </summary>
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

    /// <summary>
    /// When a safe delegate witness's value callback throws, the witness latches stopped: the exception
    /// surfaces to the caller once, and the next value is dropped instead of re-entering the broken callback.
    /// </summary>
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

        await Assert.That(thrown.Message).IsEqualTo("next-failed");
        await Assert.That(calls).IsEqualTo(1);
    }

    /// <summary>
    /// The cancel-free wrapper (a plain observer made safe without a cancellation resource) forwards values,
    /// completes exactly once, and drops notifications that arrive after the completion.
    /// </summary>
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

    /// <summary>
    /// A throwing value callback stops the cancel-free wrapper: the exception surfaces once and the next value
    /// is dropped rather than handed to the observer that already failed.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CancelFreeSafeWitnessStopsAfterTheObserverThrows()
    {
        var safe = Witness.Safe(new ThrowingWitness<int>(throwOnNext: true));

        var thrown = Assert.Throws<InvalidOperationException>(() => safe.OnNext(FirstValue));

        // The witness has latched stopped, so this must be a silent no-op rather than a second throw.
        safe.OnNext(LateValue);

        await Assert.That(thrown.Message).IsEqualTo("observer-next");
    }
}
