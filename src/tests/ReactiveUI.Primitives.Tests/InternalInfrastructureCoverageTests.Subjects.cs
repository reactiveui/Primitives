// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#pragma warning disable S103, S6966 // Coverage tests intentionally group branch-heavy scenarios.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for internal infrastructure coverage.</summary>
public partial class InternalInfrastructureCoverageTests
{
    /// <summary>Covers Signal and AsyncSignal subscriber churn, late subscriptions, disposal, and terminal no-op branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubjectsCoverMultipleSubscriberChurnLateTerminalsAndDisposalBranches()
    {
        var subject = new Signal<int>();
        var first = new RecordingWitness<int>();
        var second = new RecordingWitness<int>();
        var third = new RecordingWitness<int>();
        var fourth = new RecordingWitness<int>();
        var actionValues = new List<int>();
        using var action = subject.Subscribe(actionValues.Add);
        using var firstSubscription = subject.Subscribe(first);
        using var secondSubscription = subject.Subscribe(second);
        using var thirdSubscription = subject.Subscribe(third);
        using var fourthSubscription = subject.Subscribe(fourth);
        secondSubscription.Dispose();
        subject.OnNext(One);
        action.Dispose();
        subject.OnCompleted();
        subject.OnCompleted();
        subject.OnNext(Two);
        var lateCompleted = new RecordingWitness<int>();
        subject.Subscribe(lateCompleted).Dispose();
        await Assert.That(first.Values.SequenceEqual(ExpectedSingleOne)).IsTrue();
        await Assert.That(first.Completed).IsEqualTo(1);
        await Assert.That(second.Values.Count).IsEqualTo(0);
        await Assert.That(third.Values.SequenceEqual(ExpectedSingleOne)).IsTrue();
        await Assert.That(fourth.Values.SequenceEqual(ExpectedSingleOne)).IsTrue();
        await Assert.That(actionValues.SequenceEqual(ExpectedSingleOne)).IsTrue();
        await Assert.That(lateCompleted.Completed).IsEqualTo(1);
        var faulted = new Signal<int>();
        var faultObserver = new RecordingWitness<int>();
        var actionFaults = 0;
        using var faultSubscription = faulted.Subscribe(faultObserver);
        var fault = new InvalidOperationException("fault");
        faulted.OnError(fault);
        faulted.OnError(new InvalidOperationException("late"));
        var lateFault = new RecordingWitness<int>();
        faulted.Subscribe(lateFault).Dispose();
        await Assert.That(lateFault.Errors[0]).IsSameReferenceAs(fault);
        await Assert.That(actionFaults).IsEqualTo(0);
        Assert.Throws<ArgumentNullException>(() => faulted.OnError(null!));
        var actionFaulted = new Signal<int>();
        using var faultingAction = actionFaulted.Subscribe(_ => actionFaults++);
        Assert.Throws<InvalidOperationException>(() => actionFaulted.OnError(new InvalidOperationException("action-fault")));
        var disposedSubject = new Signal<int>();
        disposedSubject.Dispose();
        disposedSubject.Dispose();
        Assert.Throws<ObjectDisposedException>(() => disposedSubject.Subscribe(new RecordingWitness<int>()));
        Assert.Throws<ObjectDisposedException>(() => disposedSubject.OnNext(One));
        actionFaults = await AssertAsyncSignalSubscriberChurnAndTerminals(actionFaults).ConfigureAwait(false);
    }

    /// <summary>
    /// Covers internal broadcaster copy-on-write branches, signal action late-terminal branches, and buffer disposal/error branches.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task InternalInfrastructureBranchesCoverObserverChurnAndTerminalEdges()
    {
        Broadcaster<int> broadcaster = default;
        var first = new RecordingWitness<int>();
        var second = new RecordingWitness<int>();
        var third = new RecordingWitness<int>();
        var fourth = new RecordingWitness<int>();
        var missing = new RecordingWitness<int>();
        broadcaster.Add(first);
        broadcaster.Add(second);
        broadcaster.Add(third);
        broadcaster.Add(fourth);
        await Assert.That(broadcaster.HasObservers).IsTrue();
        broadcaster.Remove(missing);
        broadcaster.Remove(second);
        broadcaster.Next(One);
        broadcaster.Error(new InvalidOperationException("broadcast"));
        broadcaster.Completed();
        var copy = broadcaster;
        await Assert.That(broadcaster.Equals(copy)).IsTrue();
        await Assert.That(broadcaster.Equals((object)copy)).IsTrue();
        await Assert.That(broadcaster.Equals("not a broadcaster")).IsFalse();
        await Assert.That(broadcaster.GetHashCode()).IsNotEqualTo(0);
        broadcaster.Clear();
        await Assert.That(broadcaster.HasObservers).IsFalse();
        await Assert.That(first.Values.SequenceEqual(ExpectedSingleOne)).IsTrue();
        await Assert.That(second.Values.Count).IsEqualTo(0);
        await Assert.That(third.Values.SequenceEqual(ExpectedSingleOne)).IsTrue();
        await Assert.That(fourth.Values.SequenceEqual(ExpectedSingleOne)).IsTrue();
        await Assert.That(first.Errors.Count).IsEqualTo(1);
        await Assert.That(third.Completed).IsEqualTo(1);
        await Assert.That(fourth.Completed).IsEqualTo(1);
        var completedSignal = new Signal<int>();
        completedSignal.OnCompleted();
        completedSignal.Subscribe(_ =>
        {
        }).Dispose();
        var failedSignal = new Signal<int>();
        failedSignal.OnError(new InvalidOperationException("late action"));
        Assert.Throws<InvalidOperationException>(() => failedSignal.Subscribe(_ =>
{
}).Dispose());
        var source = new Signal<int>();
        var buffers = new List<IList<int>>();
        using (source.Buffer(Three, Two).Subscribe(buffers.Add))
        {
            source.OnNext(One);
            source.OnNext(Two);

            // The window (size 3) is incomplete; completion flushes the partial trailing window.
            source.OnCompleted();
        }

        await Assert.That(buffers.Count).IsEqualTo(1);
        await Assert.That(buffers[0].SequenceEqual([One, Two])).IsTrue();
        var errorSource = new Signal<int>();
        var bufferError = false;
        using (errorSource.Buffer(Two, One).Subscribe(
            _ =>
        {
        },
            _ => bufferError = true,
            () =>
        {
        }))
        {
            errorSource.OnError(new InvalidOperationException("buffer-error"));
        }

        await Assert.That(bufferError).IsTrue();
    }

    /// <summary>Covers observer exception paths and typed catch/finally branches with deterministic synchronous sources.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ObserverExceptionCatchFinallyAndTerminalPredicateBranchesCoverRemainders()
    {
        var keepErrors = new List<string>();
        var allErrors = new List<string>();
        var distinctErrors = new List<string>();
        var catchValues = new List<int>();
        var catchErrors = new List<string>();
        var finallyCalls = 0;
        Signal.FromEnumerable([One, Two]).Keep(value => value == One ? true : throw new InvalidOperationException("keep-predicate")).Subscribe(
            _ =>
        {
        },
            ex => keepErrors.Add(ex.Message));
        Signal.FromEnumerable([One, Two]).All(value => value == One ? true : throw new InvalidOperationException("all-predicate")).Subscribe(
            _ =>
        {
        },
            ex => allErrors.Add(ex.Message));
        Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable(["a", "bb"]).DistinctBy(value => value.Length == 1 ? value.Length : throw new InvalidOperationException("distinct-key")).Subscribe(
            _ =>
{
},
            ex => distinctErrors.Add(ex.Message)).Dispose());
        Signal.Fail<int>(new InvalidOperationException("typed-catch")).Recover<int, InvalidOperationException>(_ => Signal.Emit(Five)).Subscribe(catchValues.Add, ex => catchErrors.Add(ex.Message));
        Signal.Fail<int>(new InvalidOperationException("handler-fault")).Recover<int, InvalidOperationException>(_ => throw new FormatException("handler-threw")).Subscribe(
            _ =>
        {
        },
            ex => catchErrors.Add(ex.Message));
        Signal.Fail<int>(new ArgumentException("not-matched")).Recover<int, InvalidOperationException>(_ => Signal.Emit(Six)).Subscribe(
            _ =>
        {
        },
            ex => catchErrors.Add(ex.Message));
        Signal.Fail<int>(new InvalidOperationException("finally-error")).OnCleanup(() => finallyCalls++).Subscribe(
            _ =>
        {
        },
            _ =>
        {
        });
        await Assert.That(keepErrors.SequenceEqual(ExpectedKeepErrors)).IsTrue();
        await Assert.That(allErrors.SequenceEqual(ExpectedAllErrors)).IsTrue();
        await Assert.That(distinctErrors.Count).IsEqualTo(0);
        await Assert.That(catchValues.SequenceEqual(ExpectedSingleFive)).IsTrue();
        await Assert.That(catchErrors.SequenceEqual(ExpectedCatchErrors)).IsTrue();
        await Assert.That(finallyCalls).IsEqualTo(1);
    }

    /// <summary>Covers terminal observers that must ignore protocol violations after their first terminal signal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TerminalObserversIgnoreLateSignalsAndForwardPredicateFailures()
    {
        var errors = new List<string>();
        var values = new List<object>();
        var badIntegers = new ScriptedObservable<int>(observer =>
        {
            observer.OnNext(One);
            observer.OnError(new InvalidOperationException("first-terminal"));
            observer.OnNext(Two);
            observer.OnCompleted();
        });
        var lateCompletionIntegers = new ScriptedObservable<int>(observer =>
        {
            observer.OnNext(One);
            observer.OnCompleted();
            observer.OnError(new InvalidOperationException("late-error"));
            observer.OnNext(Two);
        });
        badIntegers.Count().Subscribe(value => values.Add(value), ex => errors.Add("count:" + ex.Message));
        badIntegers.LongCount().Subscribe(value => values.Add(value), ex => errors.Add("long-count:" + ex.Message));
        badIntegers.Count(value => value > 0).Subscribe(value => values.Add(value), ex => errors.Add("count-predicate:" + ex.Message));
        badIntegers.LongCount(value => value > 0).Subscribe(value => values.Add(value), ex => errors.Add("long-count-predicate:" + ex.Message));
        badIntegers.Any().Subscribe(value => values.Add(value), ex => errors.Add("any:" + ex.Message));
        badIntegers.Any(value => value > 0).Subscribe(value => values.Add(value), ex => errors.Add("any-predicate:" + ex.Message));
        badIntegers.All(value => value > 0).Subscribe(value => values.Add(value), ex => errors.Add("all:" + ex.Message));
        badIntegers.Contains(Two).Subscribe(value => values.Add(value), ex => errors.Add("contains:" + ex.Message));
        lateCompletionIntegers.Count().Subscribe(value => values.Add(value), ex => errors.Add("late-count:" + ex.Message));
        lateCompletionIntegers.LongCount().Subscribe(value => values.Add(value), ex => errors.Add("late-long-count:" + ex.Message));
        lateCompletionIntegers.Any(value => value > 0).Subscribe(value => values.Add(value), ex => errors.Add("late-any:" + ex.Message));
        lateCompletionIntegers.All(value => value > 0).Subscribe(value => values.Add(value), ex => errors.Add("late-all:" + ex.Message));
        lateCompletionIntegers.Contains(One).Subscribe(value => values.Add(value), ex => errors.Add("late-contains:" + ex.Message));
        Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One, Two]).Count(value => value == One ? true : throw new InvalidOperationException("count-predicate-fault")).Subscribe(
            _ =>
{
},
            ex => errors.Add(ex.Message)).Dispose());
        Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One, Two]).LongCount(value => value == One ? true : throw new InvalidOperationException("long-count-predicate-fault")).Subscribe(
            _ =>
{
},
            ex => errors.Add(ex.Message)).Dispose());
        Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One, Two]).Any(value => value == One ? false : throw new InvalidOperationException("any-predicate-fault")).Subscribe(
            _ =>
{
},
            ex => errors.Add(ex.Message)).Dispose());
        Signal.FromEnumerable([One, Two]).Contains(Two, new ThrowingComparer()).Subscribe(
            _ =>
        {
        },
            ex => errors.Add(ex.Message));
        await Assert.That(errors).Contains("count:first-terminal");
        await Assert.That(errors).Contains("long-count:first-terminal");
        await Assert.That(errors).Contains("count-predicate:first-terminal");
        await Assert.That(errors).Contains("long-count-predicate:first-terminal");
        await Assert.That(errors).Contains("all:first-terminal");
        await Assert.That(errors).Contains("contains:first-terminal");
        await Assert.That(errors).Contains("comparer-fault");
        await Assert.That(values).Contains(1);
        await Assert.That(values).Contains(1L);
        await Assert.That(values).Contains(true);
    }
}
