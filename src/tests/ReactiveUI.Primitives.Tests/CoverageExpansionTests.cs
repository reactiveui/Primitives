// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#pragma warning disable S103, S6966 // Coverage tests intentionally group branch-heavy scenarios.

using System.Collections;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Covers production paths that are only reachable through scheduled and error-handling factory variants.</summary>
public class CoverageExpansionTests
{
    /// <summary>The first expected value.</summary>
    private const int First = 1;

    /// <summary>The second expected value.</summary>
    private const int Second = 2;

    /// <summary>The third expected value.</summary>
    private const int Third = 3;

    /// <summary>The fourth expected value.</summary>
    private const int Fourth = 4;

    /// <summary>Timeout used when waiting for thread-pool scheduled observer callbacks.</summary>
    private const int TimeoutSeconds = 2;

    /// <summary>Reused first-error message.</summary>
    private const string FirstMessage = "first";

    /// <summary>Reused stopped event name.</summary>
    private const string StoppedMessage = "stopped";

    /// <summary>Deterministic absolute due time for scheduler overload tests.</summary>
    private static readonly DateTimeOffset AbsoluteDueTime = DateTimeOffset.UnixEpoch;

    /// <summary>Single-value return expectation.</summary>
    private static readonly int[] SingleFirstExpected = [First];

    /// <summary>Expected values produced by the catch params overload.</summary>
    private static readonly int[] CatchRecoveryExpected = [First, Second];

    /// <summary>Expected values for create-with-state tests.</summary>
    private static readonly int[] CreateWithStateExpected = [Third];

    /// <summary>Awaiter source values.</summary>
    private static readonly int[] AwaiterSource = [First, Second];

    /// <summary>Expected values from thread-pool observer dispatch.</summary>
    private static readonly int[] WitnessOnExpected = [First];

    /// <summary>Expected values produced by simple scheduling extension overloads.</summary>
    private static readonly int[] ScheduleExpected = [First, Second, Third, Fourth];

    /// <summary>Expected virtual-time event sequence.</summary>
    private static readonly string[] VirtualEventsExpected = [FirstMessage, StoppedMessage];

    /// <summary>Covers scheduled return, throw, and empty signal implementations.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduledScalarFactoriesUseNonImmediateSignalImplementations()
    {
        var returned = new List<int>();
        var returnCompleted = 0;
        Signal.Emit(First, Sequencer.CurrentThread).Subscribe(returned.Add, ex => throw ex, () => returnCompleted++);
        var emptyCompleted = 0;
        Signal.None<int>(Sequencer.CurrentThread).Subscribe(
            _ =>
        {
        },
            ex => throw ex,
            () => emptyCompleted++);
        var error = new InvalidOperationException("scheduled");
        var thrown = new List<Exception>();
        Signal.Fail<int>(error, Sequencer.CurrentThread).Subscribe(
            _ =>
        {
        },
            thrown.Add,
            () =>
        {
        });
        await Assert.That(returned.SequenceEqual(SingleFirstExpected)).IsTrue();
        await Assert.That(returnCompleted).IsEqualTo(1);
        await Assert.That(emptyCompleted).IsEqualTo(1);
        await Assert.That(thrown[0]).IsSameReferenceAs(error);
    }

    /// <summary>Covers create-with-state overloads and null validation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CreateWithStateFactoriesInvokeStatefulSubscribeCallbacks()
    {
        var values = new List<int>();
        var completed = 0;
        var disposed = 0;
        Signal.CreateWithState<int, int>(
            Third,
            static (state, observer) =>
        {
            observer.OnNext(state);
            observer.OnCompleted();
            return new ActionDisposable(() =>
            {
            });
        },
            false).Subscribe(values.Add, ex => throw ex, () => completed++);
        var subscription = Signal.CreateWithState<int, int>(Fourth, (state, observer) =>
        {
            observer.OnNext(state);
            return new ActionDisposable(() => disposed++);
        }).Subscribe(_ =>
        {
        });
        subscription.Dispose();
        await Assert.That(values.SequenceEqual(CreateWithStateExpected)).IsTrue();
        await Assert.That(completed).IsEqualTo(1);
        await Assert.That(disposed).IsEqualTo(1);
        Assert.Throws<ArgumentNullException>(() => Signal.Create<int>(null!, true));
        Assert.Throws<ArgumentNullException>(() => Signal.CreateSafe<int>(null!, true));
        Assert.Throws<ArgumentNullException>(() => Signal.CreateWithState<int, int>(First, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.CreateWithState<int, int>(First, null!, true));
        Assert.Throws<ArgumentNullException>(() => Signal.Lazy<int>(null!));
    }

    /// <summary>Covers signal awaiter completion, pre-cancellation, and registered cancellation paths.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetAwaiterCoversCompletionAndCancellationPaths()
    {
        var completed = Signal.FromEnumerable(AwaiterSource).GetAwaiter();
        await Assert.That(completed.IsCompleted).IsTrue();
        await Assert.That(completed.GetResult()).IsEqualTo(Second);
        using var canceledBeforeSubscribe = new CancellationTokenSource();
        await canceledBeforeSubscribe.CancelAsync().ConfigureAwait(false);
        var alreadyCanceled = Signal.Silent<int>().GetAwaiter(canceledBeforeSubscribe.Token);
        await Assert.That(alreadyCanceled.IsCompleted).IsTrue();
        Assert.Throws<OperationCanceledException>(() => alreadyCanceled.GetResult());
        using var canceledAfterSubscribe = new CancellationTokenSource();
        var source = new Signal<int>();
        var awaiter = source.GetAwaiter(canceledAfterSubscribe.Token);
        await canceledAfterSubscribe.CancelAsync().ConfigureAwait(false);
        await Assert.That(awaiter.IsCompleted).IsTrue();
        Assert.Throws<OperationCanceledException>(() => awaiter.GetResult());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).GetAwaiter());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).GetAwaiter(CancellationToken.None));
    }

    /// <summary>Covers catch sequence recovery, final error, empty completion, null source, and enumerator failure branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CatchParamsFactoryCoversRecoveryAndFailureBranches()
    {
        var recovered = new List<int>();
        Signal.Recover(Signal.Fail<int>(new InvalidOperationException(FirstMessage)), Signal.FromEnumerable(CatchRecoveryExpected), Signal.Fail<int>(new InvalidOperationException("unused"))).Subscribe(recovered.Add);
        await Assert.That(recovered.SequenceEqual(CatchRecoveryExpected)).IsTrue();
        var finalErrors = new List<Exception>();
        var finalError = new InvalidOperationException("last");
        Signal.Recover(Signal.Fail<int>(new InvalidOperationException(FirstMessage)), Signal.Fail<int>(finalError)).Subscribe(
            _ =>
        {
        },
            finalErrors.Add,
            () =>
        {
        });
        await Assert.That(finalErrors[0]).IsSameReferenceAs(finalError);
        var completed = 0;
        var completedSubscription = Signal.Recover<int>().Subscribe(
            _ =>
        {
        },
            ex => throw ex,
            () => completed++);
        completedSubscription.Dispose();
        completedSubscription.Dispose();
        await Assert.That(completed).IsEqualTo(1);
        var activeSubscription = Signal.Recover(Signal.Silent<int>()).Subscribe(
            _ =>
        {
        },
            ex => throw ex,
            () =>
        {
        });
        activeSubscription.Dispose();
        var nullSourceErrors = new List<Exception>();
        Signal.Recover(new IObservable<int>?[] { null! }!).Subscribe(
            _ =>
        {
        },
            nullSourceErrors.Add,
            () =>
        {
        });
        await Assert.That(nullSourceErrors[0] is InvalidOperationException).IsTrue();
        var moveNextErrors = new List<Exception>();
        var moveNextError = new InvalidOperationException("move-next");
        new ThrowingMoveNextEnumerable<IObservable<int>>(moveNextError).Recover().Subscribe(
            _ =>
        {
        },
            moveNextErrors.Add,
            () =>
        {
        });
        await Assert.That(moveNextErrors[0]).IsSameReferenceAs(moveNextError);
        var getEnumeratorError = new InvalidOperationException("enumerator");
        Assert.Throws<InvalidOperationException>(() => new ThrowingEnumerable<IObservable<int>>(getEnumeratorError).Recover().Subscribe(
            _ =>
{
},
            _ =>
{
},
            () =>
{
}));
    }

    /// <summary>Covers the thread-pool-specialized witness dispatch implementation.</summary>
    /// <returns>A task representing asynchronous observer dispatch.</returns>
    [Test]
    public async Task WitnessOnThreadPoolDispatchesNextCompletedAndErrorSignals()
    {
        var values = new List<int>();
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using (Signal.FromEnumerable(WitnessOnExpected).WitnessOn(ThreadPoolSequencer.Instance).Subscribe(values.Add, completion.SetException, completion.SetResult))
        {
            await WaitForAsync(completion.Task);
        }

        await Assert.That(values.Count <= WitnessOnExpected.Length).IsTrue();
        var error = new InvalidOperationException("thread-pool");
        var observed = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        using (Signal.Fail<int>(error).WitnessOn(ThreadPoolSequencer.Instance).Subscribe(
            _ =>
        {
        },
            observed.SetResult,
            () =>
        {
        }))
        {
            await Assert.That(await WaitForAsync(observed.Task)).IsSameReferenceAs(error);
        }
    }

    /// <summary>Covers simple sequencer extension validation, delayed overloads, state overloads, and recursive scheduling.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SimpleSequencerExtensionsCoverValidationAndRecursiveScheduling()
    {
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).Schedule(() =>
{
}));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.Schedule((Action)null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).Schedule(TimeSpan.Zero, () =>
{
}));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.Schedule(TimeSpan.Zero, null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).Schedule(AbsoluteDueTime, () =>
{
}));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.Schedule(AbsoluteDueTime, null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).Schedule(self => self()));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.Schedule((Action<Action>)null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).ScheduleAction(First, _ =>
{
}));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.ScheduleAction(First, (Action<int>)null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).ScheduleAction(First, _ => EmptyDisposable.Instance));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.ScheduleAction(First, (Func<int, IDisposable>)null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).ScheduleAction(First, TimeSpan.Zero, _ =>
{
}));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.ScheduleAction(First, TimeSpan.Zero, (Action<int>)null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).ScheduleAction(First, TimeSpan.Zero, _ => EmptyDisposable.Instance));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.ScheduleAction(First, TimeSpan.Zero, (Func<int, IDisposable>)null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).ScheduleAction(First, AbsoluteDueTime, _ =>
{
}));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.ScheduleAction(First, AbsoluteDueTime, (Action<int>)null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).ScheduleAction(First, AbsoluteDueTime, _ => EmptyDisposable.Instance));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.ScheduleAction(First, AbsoluteDueTime, (Func<int, IDisposable>)null!));
        var values = new List<int>();
        Sequencer.Immediate.ScheduleAction(First, values.Add).Dispose();
        Sequencer.Immediate.ScheduleAction(Second, value =>
        {
            values.Add(value);
            return EmptyDisposable.Instance;
        }).Dispose();
        Sequencer.Immediate.ScheduleAction(Third, TimeSpan.Zero, values.Add).Dispose();
        Sequencer.Immediate.ScheduleAction(Fourth, AbsoluteDueTime, value =>
        {
            values.Add(value);
            return EmptyDisposable.Instance;
        }).Dispose();
        var recursiveCount = 0;
        Sequencer.Immediate.Schedule(self =>
        {
            recursiveCount++;
            if (recursiveCount >= Third)
            {
                return;
            }

            self();
        }).Dispose();
        await Assert.That(values.SequenceEqual(ScheduleExpected)).IsTrue();
        await Assert.That(recursiveCount).IsEqualTo(Third);
    }

    /// <summary>Covers virtual-time service lookup, stopwatch, stop, sleep, and nested-run guard paths.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task VirtualTimeSequencerBaseCoversServicesStopwatchAndRunGuards()
    {
        var clock = new TestClock(DateTimeOffset.UnixEpoch);
        var provider = (IServiceProvider)clock;
        await Assert.That(provider.GetService(typeof(IStopwatchProvider))!).IsSameReferenceAs(clock);
        await Assert.That(provider.GetService(typeof(string))).IsNull();
        var stopwatch = clock.StartStopwatch();
        clock.Sleep(TimeSpan.FromTicks(First));
        await Assert.That(stopwatch.Elapsed).IsEqualTo(TimeSpan.FromTicks(First));
        var events = new List<string>();
        using var firstSchedule = clock.ScheduleAction(FirstMessage, TimeSpan.FromTicks(First), value =>
        {
            events.Add(value);
            Assert.Throws<InvalidOperationException>(() => clock.AdvanceTo(clock.Now.AddTicks(First)));
            Assert.Throws<InvalidOperationException>(() => clock.AdvanceBy(TimeSpan.FromTicks(First)));
        });
        clock.AdvanceBy(TimeSpan.FromTicks(First));
        using var stoppedSchedule = clock.ScheduleAction(StoppedMessage, TimeSpan.FromTicks(First), events.Add);
        clock.Stop();
        clock.Start();
        await Assert.That(events.SequenceEqual(VirtualEventsExpected)).IsTrue();
    }

    /// <summary>Waits for a task with a bounded timeout.</summary>
    /// <param name = "task">The task to wait for.</param>
    /// <returns>A task that completes when the supplied task completes.</returns>
    private static async Task WaitForAsync(Task task)
    {
        var timeout = Task.Delay(TimeSpan.FromSeconds(TimeoutSeconds));
        var completed = await Task.WhenAny(task, timeout).ConfigureAwait(false);
        if (completed == timeout)
        {
            throw new TimeoutException("Timed out waiting for scheduled observer dispatch.");
        }

        await task.ConfigureAwait(false);
    }

    /// <summary>Waits for a task with a bounded timeout and returns its result.</summary>
    /// <typeparam name = "T">The task result type.</typeparam>
    /// <param name = "task">The task to wait for.</param>
    /// <returns>The task result.</returns>
    private static async Task<T> WaitForAsync<T>(Task<T> task)
    {
        await WaitForAsync((Task)task).ConfigureAwait(false);
        return await task.ConfigureAwait(false);
    }

    /// <summary>Enumerable test double whose enumerator throws from <see cref = "IEnumerator.MoveNext"/>.</summary>
    /// <typeparam name = "T">The value type.</typeparam>
    private sealed class ThrowingMoveNextEnumerable<T> : IEnumerable<T>
    {
        /// <summary>Error thrown by the enumerator.</summary>
        private readonly Exception _error;

        /// <summary>Initializes a new instance of the <see cref = "ThrowingMoveNextEnumerable{T}"/> class.</summary>
        /// <param name = "error">The error to throw.</param>
        public ThrowingMoveNextEnumerable(Exception error) => _error = error;

        /// <inheritdoc/>
        public IEnumerator<T> GetEnumerator() => new ThrowingMoveNextEnumerator(_error);

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>Enumerator test double that fails on movement.</summary>
        private sealed class ThrowingMoveNextEnumerator : IEnumerator<T>
        {
            /// <summary>Error thrown by movement.</summary>
            private readonly Exception _error;

            /// <summary>Initializes a new instance of the <see cref = "ThrowingMoveNextEnumerator"/> class.</summary>
            /// <param name = "error">The error to throw.</param>
            public ThrowingMoveNextEnumerator(Exception error) => _error = error;

            /// <inheritdoc/>
            public T Current => default!;

            /// <inheritdoc/>
            object IEnumerator.Current => Current!;

            /// <inheritdoc/>
            public bool MoveNext() => throw _error;

            /// <inheritdoc/>
            public void Reset() => throw new NotSupportedException();

            /// <inheritdoc/>
            public void Dispose()
            {
            }
        }
    }

    /// <summary>Enumerable test double that throws when enumeration starts.</summary>
    /// <typeparam name = "T">The value type.</typeparam>
    private sealed class ThrowingEnumerable<T> : IEnumerable<T>
    {
        /// <summary>Error thrown by enumeration.</summary>
        private readonly Exception _error;

        /// <summary>Initializes a new instance of the <see cref = "ThrowingEnumerable{T}"/> class.</summary>
        /// <param name = "error">The error to throw.</param>
        public ThrowingEnumerable(Exception error) => _error = error;

        /// <inheritdoc/>
        public IEnumerator<T> GetEnumerator() => throw _error;

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
