// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Adds deterministic coverage for operator edge branches left after the broad contract suites.</summary>
public partial class InternalInfrastructureCoverageTests
{
    /// <summary>The integer constant one.</summary>
    private const int One = 1;

    /// <summary>The integer constant two.</summary>
    private const int Two = 2;

    /// <summary>The integer constant three.</summary>
    private const int Three = 3;

    /// <summary>The integer constant four.</summary>
    private const int Four = 4;

    /// <summary>The integer constant five.</summary>
    private const int Five = 5;

    /// <summary>The integer constant six.</summary>
    private const int Six = 6;

    /// <summary>The integer constant seven.</summary>
    private const int Seven = 7;

    /// <summary>The integer constant nine.</summary>
    private const int Nine = 9;

    /// <summary>The integer constant ninety-nine.</summary>
    private const int NinetyNine = 99;

    /// <summary>The timeout in seconds used when waiting for asynchronous branches.</summary>
    private const int TimeoutSeconds = 2;

    /// <summary>The delay in milliseconds between condition polls.</summary>
    private const int PollDelayMilliseconds = 10;

    /// <summary>The long constant zero.</summary>
    private const long ZeroLong = 0L;

    /// <summary>The long constant one.</summary>
    private const long OneLong = 1L;

    /// <summary>The long constant two.</summary>
    private const long TwoLong = 2L;

    /// <summary>The expected side-effect log produced by the tapped source and the faulted tap.</summary>
    private static readonly string[] ExpectedTapSideEffects =
        ["next:1", "next:2", "next:3", "next:4", "completed", "error:do-error"];

    /// <summary>The expected keys retained when distinct-by length is applied.</summary>
    private static readonly string[] ExpectedDistinctKeys = ["aa", "ccc", "dd", "e"];

    /// <summary>The expected emptiness results for the empty and non-empty sources.</summary>
    private static readonly bool[] ExpectedIsEmptyValues = [true, false];

    /// <summary>The expected values produced by the looped string signal.</summary>
    private static readonly string[] ExpectedRepeatValues = ["r", "r", "r"];

    /// <summary>The expected error type names produced by the task factory continuations.</summary>
    private static readonly string[] ExpectedTaskErrorNames =
        [nameof(InvalidOperationException), nameof(TaskCanceledException)];

    /// <summary>The expected single zero-tick emission produced by one-shot timer factories.</summary>
    private static readonly long[] ExpectedSingleZeroTick = [ZeroLong];

    /// <summary>The expected zero-through-two tick emissions produced by periodic timer factories.</summary>
    private static readonly long[] ExpectedZeroToTwoTicks = [ZeroLong, OneLong, TwoLong];

    /// <summary>The expected message from the single delayed-error branch.</summary>
    private static readonly string[] ExpectedDelayErrors = ["delay-error"];

    /// <summary>The expected error type name from the expire-timeout branch.</summary>
    private static readonly string[] ExpectedTimeoutErrors = [nameof(TimeoutException)];

    /// <summary>The expected single true value emitted by the true signal.</summary>
    private static readonly bool[] ExpectedTrueValues = [true];

    /// <summary>The expected single false value emitted by the false signal.</summary>
    private static readonly bool[] ExpectedFalseValues = [false];

    /// <summary>The expected message from the keep-predicate fault branch.</summary>
    private static readonly string[] ExpectedKeepErrors = ["keep-predicate"];

    /// <summary>The expected message from the all-predicate fault branch.</summary>
    private static readonly string[] ExpectedAllErrors = ["all-predicate"];

    /// <summary>The expected messages from the recover handler-fault and unmatched branches.</summary>
    private static readonly string[] ExpectedCatchErrors = ["handler-threw", "not-matched"];

    /// <summary>The expected value emitted by the scheduled return signal.</summary>
    private static readonly string[] ExpectedScheduledReturn = ["scheduled"];

    /// <summary>The expected message from the map-selector fault branch.</summary>
    private static readonly string[] ExpectedMappedErrors = ["map-fault"];

    /// <summary>The expected long count produced by the range-backed distinct long-count alias.</summary>
    private static readonly long[] ExpectedRangeDistinctLongCount = [TwoLong];

    /// <summary>The expected one-through-four sequence emitted by the four-element source.</summary>
    private static readonly int[] ExpectedOneToFour = [One, Two, Three, Four];

    /// <summary>The expected single null produced by the default-if-empty branch.</summary>
    private static readonly int?[] ExpectedSingleNull = [null];

    /// <summary>The expected one-and-two prefix retained by take-while and distinct branches.</summary>
    private static readonly int[] ExpectedOneTwo = [One, Two];

    /// <summary>The expected three-and-four suffix retained by skip-while and delay branches.</summary>
    private static readonly int[] ExpectedThreeFour = [Three, Four];

    /// <summary>The expected single nine produced by the fork-join sum branch.</summary>
    private static readonly int[] ExpectedSingleNine = [Nine];

    /// <summary>The expected three-through-five sequence emitted by the range factory.</summary>
    private static readonly int[] ExpectedThreeToFive = [Three, Four, Five];

    /// <summary>The expected repeated five values emitted by the bounded loop factory.</summary>
    private static readonly int[] ExpectedFiveFive = [Five, Five];

    /// <summary>The expected single seven produced by the start and scheduled branches.</summary>
    private static readonly int[] ExpectedSingleSeven = [Seven];

    /// <summary>The expected single one observed by retained subscribers.</summary>
    private static readonly int[] ExpectedSingleOne = [One];

    /// <summary>The expected single five observed by retained subscribers.</summary>
    private static readonly int[] ExpectedSingleFive = [Five];

    /// <summary>The expected two-through-four prefix produced by the single prepend branch.</summary>
    private static readonly int[] ExpectedTwoToFour = [Two, Three, Four];

    /// <summary>The expected three repeated sevens produced by the bounded loop signal.</summary>
    private static readonly int[] ExpectedSevenSevenSeven = [Seven, Seven, Seven];

    /// <summary>The expected paired sums produced by the zip alias.</summary>
    private static readonly int[] ExpectedFiveSevenNine = [Five, Seven, Nine];

    /// <summary>The expected two-and-three sequence produced by the observer prepend branch.</summary>
    private static readonly int[] ExpectedTwoThree = [Two, Three];

    /// <summary>The expected one-through-three sequence produced by the prepend/append branches.</summary>
    private static readonly int[] ExpectedOneToThree = [One, Two, Three];

    /// <summary>The expected single two produced by the distinct count alias.</summary>
    private static readonly int[] ExpectedSingleTwo = [Two];

    /// <summary>The expected three-and-five sequence produced by the combine-latest branch.</summary>
    private static readonly int[] ExpectedThreeFive = [Three, Five];

    /// <summary>Asserts the argument guards exposed by the parity operator surface.</summary>
    /// <param name="source">A non-null source used to exercise instance guards.</param>
    private static void AssertParityOperatorArgumentGuards(IObservable<int> source)
    {
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Prepend(null!, One, Two));
        Assert.Throws<ArgumentNullException>(() => source.Prepend((int[])null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Prepend<int>(null!, (IEnumerable<int>)[One]));
        Assert.Throws<ArgumentNullException>(() => source.Prepend((IEnumerable<int>)null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.ObserveOn<int>(null!, Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() => source.ObserveOn(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.SubscribeOn<int>(null!, Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() => source.SubscribeOn(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Tap<int>(null!, _ => { }, _ => { }, () => { }));
        Assert.Throws<ArgumentNullException>(() => source.Tap(null!, _ => { }, () => { }));
        Assert.Throws<ArgumentNullException>(() => source.Tap(_ => { }, null!, () => { }));
        Assert.Throws<ArgumentNullException>(() => source.Tap(_ => { }, _ => { }, null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.IgnoreValues<int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.DistinctBy<int, int>(null!, value => value));
        Assert.Throws<ArgumentNullException>(() => source.DistinctBy<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.UniqueBy<int, int>(null!, value => value));
        Assert.Throws<ArgumentNullException>(() => source.UniqueBy<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.TakeWhile<int>(null!, value => true));
        Assert.Throws<ArgumentNullException>(() => source.TakeWhile(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.SkipWhile<int>(null!, value => true));
        Assert.Throws<ArgumentNullException>(() => source.SkipWhile(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.FlatMap<int, int>(null!, value => Signal.Emit(value)));
        Assert.Throws<ArgumentNullException>(() => source.FlatMap<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.FlatMapValues<int, int>(null!, value => [value]));
        Assert.Throws<ArgumentNullException>(() => source.FlatMapValues<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => source.FlatMap<int, int, int>(null!, (outer, inner) => outer + inner));
        Assert.Throws<ArgumentNullException>(() => source.FlatMap<int, int, int>(value => Signal.Emit(value), null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Count<int>(null!));
        Assert.Throws<ArgumentNullException>(() => source.Count(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.LongCount<int>(null!));
        Assert.Throws<ArgumentNullException>(() => source.LongCount(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Any<int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Any<int>(null!, value => true));
        Assert.Throws<ArgumentNullException>(() => source.Any(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.All<int>(null!, value => true));
        Assert.Throws<ArgumentNullException>(() => source.All(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Contains(null!, One));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.DelayStart<int>(null!, TimeSpan.Zero));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Calm<int>(null!, TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => source.Probe(TimeSpan.FromTicks(-1)));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Timestamp<int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.TimeInterval<int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.ForkJoin<int, int, int>(null!, Signal.Emit(One), (left, right) => left + right));
        Assert.Throws<ArgumentNullException>(() => source.ForkJoin<int, int, int>(null!, (left, right) => left + right));
        Assert.Throws<ArgumentNullException>(() => source.ForkJoin<int, int, int>(Signal.Emit(One), null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).AsObservable());
        Assert.Throws<ArgumentNullException>(() => ((IEnumerable<int>)null!).ToObservable());
        Assert.Throws<ArgumentNullException>(() => ((IEnumerable<int>)null!).ToObservable(CancellationToken.None));
        Assert.Throws<ArgumentOutOfRangeException>(() => source.Take(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => source.Skip(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => source.Reattempt(-1));
    }

    /// <summary>Covers <see cref="AsyncSignal{T}"/> subscriber churn, late subscriptions, disposal, and terminal no-op branches.</summary>
    /// <param name="actionFaults">The shared action-fault counter incremented by the completion callback.</param>
    private static void AssertAsyncSignalSubscriberChurnAndTerminals(ref int actionFaults)
    {
        var completionFaults = actionFaults;
        var asyncSignal = new AsyncSignal<int>();
        Assert.Throws<InvalidOperationException>(() => _ = asyncSignal.Value);
        Assert.Throws<ArgumentNullException>(() => asyncSignal.OnCompleted(null!));
        Assert.Throws<ArgumentNullException>(() => asyncSignal.OnError(null!));
        var asyncFirst = new RecordingObserver<int>();
        var asyncSecond = new RecordingObserver<int>();
        using var asyncSubscription = asyncSignal.Subscribe(asyncFirst);
        using var asyncSecondSubscription = asyncSignal.Subscribe(asyncSecond);
        asyncSecondSubscription.Dispose();
        asyncSignal.OnNext(Five);
        asyncSignal.OnCompleted(() => completionFaults++);
        asyncSignal.OnCompleted();
        asyncSignal.OnCompleted();
        asyncSignal.OnNext(Six);
        actionFaults = completionFaults;
        var asyncLate = new RecordingObserver<int>();
        asyncSignal.Subscribe(asyncLate).Dispose();
        Assert.Equal(Five, asyncSignal.Value);
        Assert.Equal(Five, asyncSignal.GetResult());
        Assert.Equal(ExpectedSingleFive, asyncFirst.Values);
        Assert.Equal(0, asyncSecond.Values.Count);
        Assert.Equal(ExpectedSingleFive, asyncLate.Values);
        Assert.Equal(1, asyncLate.Completed);

        var asyncError = new AsyncSignal<int>();
        var asyncErrorObserver = new RecordingObserver<int>();
        asyncError.Subscribe(asyncErrorObserver).Dispose();
        var asyncFault = new InvalidOperationException("async-fault");
        asyncError.OnError(asyncFault);
        asyncError.OnError(new InvalidOperationException("late"));
        Assert.Throws<InvalidOperationException>(() => asyncError.GetResult());
        var asyncErrorLate = new RecordingObserver<int>();
        asyncError.Subscribe(asyncErrorLate).Dispose();
        Assert.Same(asyncFault, asyncErrorLate.Errors[0]);

        var disposedAsync = new AsyncSignal<int>();
        disposedAsync.Dispose();
        disposedAsync.Dispose();
        Assert.Throws<ObjectDisposedException>(() => disposedAsync.OnNext(One));
        Assert.Throws<ObjectDisposedException>(() => disposedAsync.Subscribe(new RecordingObserver<int>()));
    }

    /// <summary>Polls a condition until it succeeds or the timeout elapses.</summary>
    /// <param name="condition">The condition to evaluate on each poll.</param>
    /// <param name="timeout">The maximum time to wait for the condition.</param>
    /// <returns>A task that completes when the condition is satisfied.</returns>
    private static async Task SpinUntil(Func<bool> condition, TimeSpan timeout)
    {
        var attempts = (int)(timeout.TotalMilliseconds / PollDelayMilliseconds);
        for (var attempt = 0; attempt < attempts; attempt++)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(PollDelayMilliseconds).ConfigureAwait(false);
        }

        throw new TimeoutException("Timed out waiting for asynchronous coverage branch.");
    }

    /// <summary>An equality comparer that throws when comparing values.</summary>
    private sealed class ThrowingComparer : IEqualityComparer<int>
    {
        /// <summary>Defers to a faulting comparison so the equality comparison throws when invoked.</summary>
        /// <param name="x">The first value to compare.</param>
        /// <param name="y">The second value to compare.</param>
        /// <returns>This method never returns; the faulting comparison always throws.</returns>
        public bool Equals(int x, int y) => Fail();

        /// <summary>Returns the hash code for the specified value.</summary>
        /// <param name="obj">The value to hash.</param>
        /// <returns>The hash code for the value.</returns>
        public int GetHashCode(int obj) => obj.GetHashCode();

        /// <summary>Throws to simulate a faulting comparison.</summary>
        /// <returns>This method never returns; it always throws.</returns>
        private static bool Fail() => throw new InvalidOperationException("comparer-fault");
    }

    /// <summary>An observable that replays a scripted sequence of observer callbacks on subscribe.</summary>
    /// <typeparam name="T">The type of the elements produced by the observable.</typeparam>
    private sealed class ScriptedObservable<T> : IObservable<T>
    {
        /// <summary>The scripted callback invoked against each subscribing observer.</summary>
        private readonly Action<IObserver<T>> _script;

        /// <summary>Initializes a new instance of the <see cref="ScriptedObservable{T}"/> class.</summary>
        /// <param name="script">The scripted callback to invoke on each subscription.</param>
        public ScriptedObservable(Action<IObserver<T>> script) => _script = script;

        /// <summary>Subscribes the observer and replays the scripted callback.</summary>
        /// <param name="observer">The observer to drive with the script.</param>
        /// <returns>An empty disposable subscription.</returns>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            _script(observer);
            return EmptyDisposable.Instance;
        }
    }

    /// <summary>A minimal virtual-time sequencer used to exercise scheduling edge branches.</summary>
    private sealed class MinimalVirtualClock : VirtualTimeSequencerBase<long, long>
    {
        /// <summary>The scheduled work items keyed by their absolute due time.</summary>
        private readonly SortedDictionary<long, Queue<Scheduled>> _scheduled = [];

        /// <summary>Initializes a new instance of the <see cref="MinimalVirtualClock"/> class.</summary>
        public MinimalVirtualClock()
        {
        }

        /// <summary>Initializes a new instance of the <see cref="MinimalVirtualClock"/> class.</summary>
        /// <param name="comparer">The comparer used to order scheduled times.</param>
        public MinimalVirtualClock(IComparer<long> comparer)
            : base(0L, comparer)
        {
        }

        /// <summary>Schedules an action at the specified absolute due time.</summary>
        /// <typeparam name="TState">The type of the state passed to the action.</typeparam>
        /// <param name="state">The state passed to the action when invoked.</param>
        /// <param name="dueTime">The absolute due time at which to run the action.</param>
        /// <param name="action">The action to run when the due time is reached.</param>
        /// <returns>A disposable that cancels the scheduled action.</returns>
        public override IDisposable ScheduleAbsolute<TState>(TState state, long dueTime, Func<ISequencer, TState, IDisposable> action)
        {
            var scheduled = new Scheduled(dueTime, () => action(this, state));
            if (!_scheduled.TryGetValue(dueTime, out var queue))
            {
                queue = new();
                _scheduled.Add(dueTime, queue);
            }

            queue.Enqueue(scheduled);
            return new ActionDisposable(() => scheduled.IsCancelled = true);
        }

        /// <summary>Adds a relative offset to an absolute time.</summary>
        /// <param name="absolute">The absolute time.</param>
        /// <param name="relative">The relative offset to add.</param>
        /// <returns>The resulting absolute time.</returns>
        protected override long Add(long absolute, long relative) => absolute + relative;

        /// <summary>Returns the next non-cancelled scheduled item, if any.</summary>
        /// <returns>The next scheduled item, or <see langword="null"/> when none remain.</returns>
        protected override IScheduledItem<long>? GetNext()
        {
            while (_scheduled.Count > 0)
            {
                using var enumerator = _scheduled.GetEnumerator();
                enumerator.MoveNext();
                var first = enumerator.Current;
                var item = first.Value.Dequeue();
                if (first.Value.Count == 0)
                {
                    _scheduled.Remove(first.Key);
                }

                if (!item.IsCancelled)
                {
                    return item;
                }
            }

            return null;
        }

        /// <summary>Converts an absolute time to a <see cref="DateTimeOffset"/>.</summary>
        /// <param name="absolute">The absolute time to convert.</param>
        /// <returns>The equivalent <see cref="DateTimeOffset"/>.</returns>
        protected override DateTimeOffset ToDateTimeOffset(long absolute) => DateTimeOffset.UnixEpoch.AddTicks(absolute);

        /// <summary>Converts a time span to the relative tick representation.</summary>
        /// <param name="timeSpan">The time span to convert.</param>
        /// <returns>The number of ticks represented by the time span.</returns>
        protected override long ToRelative(TimeSpan timeSpan) => timeSpan.Ticks;

        /// <summary>A single scheduled work item tracked by the virtual clock.</summary>
        private sealed class Scheduled : IScheduledItem<long>
        {
            /// <summary>The action to run when the item is invoked.</summary>
            private readonly Func<IDisposable> _action;

            /// <summary>Initializes a new instance of the <see cref="Scheduled"/> class.</summary>
            /// <param name="dueTime">The absolute due time for the item.</param>
            /// <param name="action">The action to run when invoked.</param>
            public Scheduled(long dueTime, Func<IDisposable> action)
            {
                DueTime = dueTime;
                _action = action;
            }

            /// <summary>Gets the absolute due time for the item.</summary>
            public long DueTime { get; }

            /// <summary>Gets or sets a value indicating whether the item has been cancelled.</summary>
            public bool IsCancelled { get; set; }

            /// <summary>Invokes the scheduled action unless the item has been cancelled.</summary>
            public void Invoke()
            {
                if (IsCancelled)
                {
                    return;
                }

                _action().Dispose();
            }
        }
    }

    /// <summary>An observer that can be configured to throw on specific callbacks.</summary>
    /// <typeparam name="T">The type of the observed values.</typeparam>
    private sealed class ThrowingObserver<T> : IObserver<T>
    {
        /// <summary>A value indicating whether to throw on <see cref="OnNext"/>.</summary>
        private readonly bool _throwOnNext;

        /// <summary>A value indicating whether to throw on <see cref="OnError"/>.</summary>
        private readonly bool _throwOnError;

        /// <summary>A value indicating whether to throw on <see cref="OnCompleted"/>.</summary>
        private readonly bool _throwOnCompleted;

        /// <summary>Initializes a new instance of the <see cref="ThrowingObserver{T}"/> class.</summary>
        /// <param name="throwOnNext">Configures throwing from the value callback.</param>
        /// <param name="throwOnError">Configures throwing from the error callback.</param>
        /// <param name="throwOnCompleted">Configures throwing from the completion callback.</param>
        public ThrowingObserver(bool throwOnNext = false, bool throwOnError = false, bool throwOnCompleted = false)
        {
            _throwOnNext = throwOnNext;
            _throwOnError = throwOnError;
            _throwOnCompleted = throwOnCompleted;
        }

        /// <summary>Gets a value indicating whether an error callback has been observed.</summary>
        public bool SeenError { get; private set; }

        /// <summary>Handles completion, throwing when configured to do so.</summary>
        public void OnCompleted()
        {
            if (!_throwOnCompleted)
            {
                return;
            }

            throw new InvalidOperationException("observer-completed");
        }

        /// <summary>Handles an error, throwing when configured to do so.</summary>
        /// <param name="error">The error to handle.</param>
        public void OnError(Exception error)
        {
            SeenError = true;
            if (!_throwOnError)
            {
                return;
            }

            throw new InvalidOperationException("observer-error");
        }

        /// <summary>Handles a value, throwing when configured to do so.</summary>
        /// <param name="value">The value to handle.</param>
        public void OnNext(T value)
        {
            if (!_throwOnNext)
            {
                return;
            }

            throw new InvalidOperationException("observer-next");
        }
    }

    /// <summary>A scheduled item probe that delegates invocation to a supplied factory.</summary>
    private sealed class ScheduledProbe : ScheduledItem<int>
    {
        /// <summary>The factory invoked when the item runs.</summary>
        private readonly Func<IDisposable> _invoke;

        /// <summary>Initializes a new instance of the <see cref="ScheduledProbe"/> class.</summary>
        /// <param name="dueTime">The due time for the scheduled item.</param>
        /// <param name="invoke">The factory invoked when the item runs.</param>
        public ScheduledProbe(int dueTime, Func<IDisposable> invoke)
            : base(dueTime, Comparer<int>.Default)
        {
            _invoke = invoke;
        }

        /// <summary>Invokes the supplied factory.</summary>
        /// <returns>The disposable returned by the factory.</returns>
        protected override IDisposable InvokeCore() => _invoke();
    }

    /// <summary>An observer that records all values, errors, and completion counts.</summary>
    /// <typeparam name="T">The type of the observed values.</typeparam>
    private sealed class RecordingObserver<T> : IObserver<T>
    {
        /// <summary>Gets the recorded values.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets the recorded errors.</summary>
        public List<Exception> Errors { get; } = [];

        /// <summary>Gets the number of completion callbacks observed.</summary>
        public int Completed { get; private set; }

        /// <summary>Records a completion callback.</summary>
        public void OnCompleted() => Completed++;

        /// <summary>Records an error callback.</summary>
        /// <param name="error">The error to record.</param>
        public void OnError(Exception error) => Errors.Add(error);

        /// <summary>Records a value callback.</summary>
        /// <param name="value">The value to record.</param>
        public void OnNext(T value) => Values.Add(value);
    }
}
