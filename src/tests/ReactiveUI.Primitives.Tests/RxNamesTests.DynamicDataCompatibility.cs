// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for Rx compatibility operators required by DynamicData migration.</summary>
public partial class RxNamesTests
{
    /// <summary>Verifies <c>SubscribeSafe</c> converts downstream value-handler exceptions into a terminal error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafeStopsAfterDownstreamOnNextException()
    {
        Signal<int> source = new();
        InvalidOperationException expected = new(Boom);
        Exception? observed = null;
        var completed = 0;
        var delivered = 0;

        using var subscription = source.SubscribeSafe(
            _ =>
            {
                delivered++;
                throw expected;
            },
            error => observed = error,
            () => completed++);

        source.OnNext(One);
        source.OnNext(Two);

        await Assert.That(delivered).IsEqualTo(One);
        await Assert.That(observed).IsSameReferenceAs(expected);
        await Assert.That(completed).IsEqualTo(0);
    }

    /// <summary>Verifies enumerable <c>Merge(maxConcurrent)</c> waits to subscribe to later sources.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EnumerableMergeHonorsMaxConcurrent()
    {
        Signal<int> first = new();
        Signal<int> second = new();
        List<int> values = [];

        using var subscription = new IObservable<int>[] { first, second }.Merge(One).Subscribe(values.Add);

        second.OnNext(Two);
        first.OnNext(One);

        await Assert.That(values.SequenceEqual([One])).IsTrue();

        first.OnCompleted();
        second.OnNext(Two);

        await Assert.That(values.SequenceEqual([One, Two])).IsTrue();
    }

    /// <summary>Verifies <c>SelectMany</c> subscribes to later inner sources before earlier inner sources complete.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SelectManyMergesInnerSourcesConcurrently()
    {
        Signal<int> outer = new();
        Signal<int> first = new();
        Signal<int> second = new();
        List<int> values = [];

        using var subscription = outer.SelectMany(value => value == One ? first : second).Subscribe(values.Add);

        outer.OnNext(One);
        outer.OnNext(Two);
        second.OnNext(Two);
        first.OnNext(One);

        await Assert.That(values.SequenceEqual([Two, One])).IsTrue();

        first.OnCompleted();
        second.OnCompleted();
        outer.OnCompleted();
    }

    /// <summary>Verifies the Rx migration aliases used by DynamicData produce expected values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DynamicDataCompatibilityAliasesProduceExpectedValues()
    {
        var startWith = Collect(Signal.FromEnumerable([Three]).StartWith(One, Two));
        var enumerableSelectMany = Collect(Signal.FromEnumerable([One, Two]).SelectMany(static value => new[] { value, value + Ten }));
        var repeatedSelectMany = Collect(Signal.FromEnumerable([One, Two]).SelectMany(Signal.Return(Ten)));
        var recovered = Collect(Signal.Throw<int>(new InvalidOperationException(Boom)).Catch((InvalidOperationException _) => Signal.Return(Two)));

        await Assert.That(startWith.SequenceEqual([One, Two, Three])).IsTrue();
        await Assert.That(enumerableSelectMany.SequenceEqual([One, One + Ten, Two, Two + Ten])).IsTrue();
        await Assert.That(repeatedSelectMany.SequenceEqual([Ten, Ten])).IsTrue();
        await Assert.That(recovered.SequenceEqual([Two])).IsTrue();
    }

    /// <summary>Verifies timed <c>Buffer</c> flushes scheduled batches and the final batch on completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task BufferFlushesTimedAndFinalBatches()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        Signal<int> source = new();
        List<IList<int>> batches = [];

        using var subscription = source.Buffer(TimeSpan.FromTicks(Two), clock).Subscribe(batches.Add);
        source.OnNext(One);
        source.OnNext(Two);

        await Assert.That(batches.Count).IsEqualTo(0);

        clock.AdvanceBy(TimeSpan.FromTicks(Two));

        await Assert.That(batches.Count).IsEqualTo(One);
        await Assert.That(batches[0].SequenceEqual([One, Two])).IsTrue();

        source.OnNext(Three);
        source.OnCompleted();

        await Assert.That(batches.Count).IsEqualTo(Two);
        await Assert.That(batches[1].SequenceEqual([Three])).IsTrue();
    }

    /// <summary>Verifies <c>Throttle</c> emits only the latest value after the quiet period.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ThrottleEmitsLatestAfterQuietPeriod()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        Signal<int> source = new();
        List<int> values = [];

        using var subscription = source.Throttle(TimeSpan.FromTicks(Two), clock).Subscribe(values.Add);
        source.OnNext(One);
        clock.AdvanceBy(TimeSpan.FromTicks(One));
        source.OnNext(Two);
        clock.AdvanceBy(TimeSpan.FromTicks(One));

        await Assert.That(values.Count).IsEqualTo(0);

        clock.AdvanceBy(TimeSpan.FromTicks(One));

        await Assert.That(values.SequenceEqual([Two])).IsTrue();
    }

    /// <summary>Verifies <c>Finally</c> runs once when a subscription completes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FinallyRunsOnceOnCompletion()
    {
        var cleanupCount = 0;
        List<int> values = [];

        Signal.FromEnumerable([One]).Finally(() => cleanupCount++).Subscribe(values.Add);

        await Assert.That(values.SequenceEqual([One])).IsTrue();
        await Assert.That(cleanupCount).IsEqualTo(One);
    }

    /// <summary>Verifies bounded enumerable merge handles completion, null sources, and source failures.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EnumerableMergeMaxConcurrentHandlesTerminalAndFailureBranches()
    {
        var completed = 0;
        Enumerable.Empty<IObservable<int>>().Merge(Two)
            .Subscribe(_ => { }, ex => throw ex, () => completed++);

        await Assert.That(completed).IsEqualTo(One);

        Exception? nullError = null;
        IEnumerable<IObservable<int>> sourcesWithNull = [null!];
        sourcesWithNull.Merge(One).Subscribe(_ => { }, error => nullError = error);

        await Assert.That(nullError is InvalidOperationException).IsTrue();

        InvalidOperationException enumerableError = new("enumerable");
        Exception? observedEnumerableError = null;
        ThrowingSources(enumerableError).Merge(One).Subscribe(_ => { }, error => observedEnumerableError = error);

        await Assert.That(observedEnumerableError).IsSameReferenceAs(enumerableError);

        InvalidOperationException sourceError = new("source");
        RecordingWitness<int> failed = new();
        IEnumerable<IObservable<int>> scripted =
        [
            new ScriptedObservable<int>(observer =>
            {
                observer.OnError(sourceError);
                observer.OnCompleted();
            })
        ];
        scripted.Merge(One).Subscribe(failed);

        await Assert.That(failed.Errors.Count).IsEqualTo(One);
        await Assert.That(failed.Errors[0]).IsSameReferenceAs(sourceError);

        Signal<int> first = new();
        Signal<int> second = new();
        var sequentialCompleted = 0;
        using (new IObservable<int>[] { first, second }.Merge(One)
            .Subscribe(_ => { }, ex => throw ex, () => sequentialCompleted++))
        {
            first.OnCompleted();
            second.OnCompleted();
        }

        await Assert.That(sequentialCompleted).IsEqualTo(One);

        Assert.Throws<ArgumentNullException>(() => ((IEnumerable<IObservable<int>>)null!).Merge());
        Assert.Throws<ArgumentNullException>(() => ((IEnumerable<IObservable<int>>)null!).Merge(One));
        Assert.Throws<ArgumentOutOfRangeException>(() => scripted.Merge(0));
    }

    /// <summary>Verifies SubscribeSafe overloads stop after terminal notifications and dispose upstream.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafeOverloadsStopAfterTerminalNotifications()
    {
        List<int> values = [];
        Signal.FromEnumerable([One]).SubscribeSafe(values.Add, _ => { });

        await Assert.That(values.SequenceEqual([One])).IsTrue();

        InvalidOperationException expected = new(Boom);
        var errorOnlyCount = 0;
        new ScriptedObservable<int>(observer =>
        {
            observer.OnError(expected);
            observer.OnError(new InvalidOperationException("late"));
        }).SubscribeSafe(error => errorOnlyCount += ReferenceEquals(error, expected) ? One : 0);

        await Assert.That(errorOnlyCount).IsEqualTo(One);

        var completed = 0;
        new ScriptedObservable<int>(observer =>
        {
            observer.OnCompleted();
            observer.OnNext(Two);
            observer.OnCompleted();
        }).SubscribeSafe(_ => { }, () => completed++);

        await Assert.That(completed).IsEqualTo(One);

        RecordingWitness<int> witness = new();
        new ScriptedObservable<int>(observer =>
        {
            observer.OnCompleted();
            observer.OnNext(Three);
            observer.OnError(new InvalidOperationException("late"));
        }).SubscribeSafe(witness);

        await Assert.That(witness.Values.Count).IsEqualTo(0);
        await Assert.That(witness.Completed).IsEqualTo(One);
        await Assert.That(witness.Errors.Count).IsEqualTo(0);
    }

    /// <summary>Verifies Rx side-effect, synchronization, indexed select, and task concat aliases.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DynamicDataRxConvenienceAliasesCoverPublicEntryPoints()
    {
        List<int> values = [];
        List<string> sideEffects = [];
        var completed = 0;
        Signal.FromEnumerable([One, Two])
            .Do(
                value => sideEffects.Add("next:" + value),
                error => sideEffects.Add("error:" + error.Message),
                () => completed++)
            .Subscribe(values.Add);

        await Assert.That(values.SequenceEqual([One, Two])).IsTrue();
        await Assert.That(sideEffects.SequenceEqual(["next:1", "next:2"])).IsTrue();
        await Assert.That(completed).IsEqualTo(One);

        Signal<int> source = new();
        RecordingWitness<int> synchronized = new();
        using (source.SynchronizeObject(new object()).Subscribe(synchronized))
        {
            source.OnNext(One);
            source.OnCompleted();
        }

        await Assert.That(synchronized.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(synchronized.Completed).IsEqualTo(One);

        Signal<int> errorSource = new();
        RecordingWitness<int> synchronizedError = new();
        using (errorSource.SynchronizeObject(new object()).Subscribe(synchronizedError))
        {
            errorSource.OnError(new InvalidOperationException(Boom));
        }

        await Assert.That(synchronizedError.Errors.Count).IsEqualTo(One);
        await Assert.That(synchronizedError.Errors[0].Message).IsEqualTo(Boom);

        List<int> aliasValues = [];
        Signal.FromEnumerable([Three]).StartWith((IEnumerable<int>)[One, Two]).Subscribe(aliasValues.Add);
        Signal.FromEnumerable([One, Two]).Merge(Signal.FromEnumerable([Three])).Subscribe(aliasValues.Add);
        Signal.FromEnumerable([One, Two]).Select(static (value, index) => value + index).Subscribe(aliasValues.Add);
        Signal.FromEnumerable([Task.FromResult(One), Task.FromResult(Two)]).Concat().Subscribe(aliasValues.Add);
        ((IEnumerable<IObservable<int>>)[Signal.Return(One), Signal.Return(Two)]).Merge().Subscribe(aliasValues.Add);
        await Task.Yield();

        await Assert.That(aliasValues.SequenceEqual([One, Two, Three, One, Two, Three, One, Three, One, Two, One, Two]))
            .IsTrue();

        await Assert.That(Signal.Return(One).Buffer(TimeSpan.FromTicks(One))).IsNotNull();
        await Assert.That(Signal.Return(One).Throttle(TimeSpan.FromTicks(One))).IsNotNull();

        Assert.Throws<ArgumentNullException>(() => Signal.Return(One).Do(_ => { }, null!, () => { }));
        Assert.Throws<ArgumentNullException>(() => Signal.Return(One).SynchronizeObject(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Return(One).Merge(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Return(One).Select((Func<int, int, int>)null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<Task<int>>)null!).Concat());
    }

    /// <summary>Verifies enumerable SelectMany forwards projected values and stops after failures or completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SelectManyEnumerableStopsAfterErrorsAndTerminalNotifications()
    {
        RecordingWitness<int> completed = new();
        new ScriptedObservable<int>(observer =>
        {
            observer.OnNext(One);
            observer.OnCompleted();
            observer.OnCompleted();
            observer.OnNext(Two);
            observer.OnError(new InvalidOperationException("late"));
        }).SelectMany(static value => new[] { value }).Subscribe(completed);

        await Assert.That(completed.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(completed.Completed).IsEqualTo(One);
        await Assert.That(completed.Errors.Count).IsEqualTo(0);

        Signal<int> nullSource = new();
        RecordingWitness<int> nullResult = new();
        using var nullSubscription = nullSource
            .SelectMany((Func<int, IEnumerable<int>>)(_ => null!))
            .Subscribe(nullResult);
        nullSource.OnNext(One);
        nullSource.OnNext(Two);

        await Assert.That(nullResult.Errors.Count).IsEqualTo(One);
        await Assert.That(nullResult.Errors[0] is InvalidOperationException).IsTrue();

        Signal<int> throwingSource = new();
        RecordingWitness<int> throwingResult = new();
        using var throwingSubscription = throwingSource
            .SelectMany((Func<int, IEnumerable<int>>)(_ => throw new InvalidOperationException("selector")))
            .Subscribe(throwingResult);
        throwingSource.OnNext(One);

        await Assert.That(throwingResult.Errors.Count).IsEqualTo(One);
        await Assert.That(throwingResult.Errors[0].Message).IsEqualTo("selector");

        Signal<int> observerThrowingSource = new();
        RecordingWitness<int> observerThrowingResult = new();
        using var observerThrowingSubscription = observerThrowingSource
            .SelectMany(static value => new[] { value, value + One })
            .Subscribe(
                value =>
                {
                    if (value != Two)
                    {
                        return;
                    }

                    throw new InvalidOperationException("observer");
                },
                observerThrowingResult.OnError,
                observerThrowingResult.OnCompleted);
        observerThrowingSource.OnNext(One);

        await Assert.That(observerThrowingResult.Errors.Count).IsEqualTo(One);
        await Assert.That(observerThrowingResult.Errors[0].Message).IsEqualTo("observer");

        Signal<int> disposedDuringEnumeration = new();
        var stoppedValues = 0;
        IDisposable? stoppedSubscription = null;
        stoppedSubscription = disposedDuringEnumeration
            .SelectMany(static value => new[] { value, value + One })
            .Subscribe(value =>
            {
                stoppedValues++;
                stoppedSubscription?.Dispose();
            });
        disposedDuringEnumeration.OnNext(One);

        await Assert.That(stoppedValues).IsEqualTo(One);

        Assert.Throws<ArgumentNullException>(() => Signal.Return(One).SelectMany((Func<int, IEnumerable<int>>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Return(One).SelectMany((IObservable<int>)null!));
    }

    /// <summary>Produces an enumerable that throws when enumeration starts.</summary>
    /// <param name="error">The exception thrown by the enumerable.</param>
    /// <returns>An enumerable observable sequence.</returns>
    private static MoveNextThrowsEnumerable ThrowingSources(Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);

        return new MoveNextThrowsEnumerable(error);
    }

    /// <summary>An enumerable whose enumerator throws from <see cref="System.Collections.IEnumerator.MoveNext"/>.</summary>
    /// <param name="error">The exception thrown by the enumerable.</param>
    private sealed class MoveNextThrowsEnumerable(Exception error) :
        IEnumerable<IObservable<int>>,
        IEnumerator<IObservable<int>>
    {
        /// <inheritdoc/>
        public IObservable<int> Current => throw new InvalidOperationException("No current value is available.");

        /// <inheritdoc/>
        object System.Collections.IEnumerator.Current => Current;

        /// <inheritdoc/>
        public IEnumerator<IObservable<int>> GetEnumerator() => this;

        /// <inheritdoc/>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();

        /// <inheritdoc/>
        public bool MoveNext() => throw error;

        /// <inheritdoc/>
        public void Reset()
        {
        }

        /// <inheritdoc/>
        public void Dispose()
        {
        }
    }
}
