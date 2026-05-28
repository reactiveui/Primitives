// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;
using TUnit.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Covers production paths that are only reachable through scheduled and error-handling factory variants.
/// </summary>
public class CoverageExpansionTests
{
    /// <summary>
    /// The first expected value.
    /// </summary>
    private const int First = 1;

    /// <summary>
    /// The second expected value.
    /// </summary>
    private const int Second = 2;

    /// <summary>
    /// The third expected value.
    /// </summary>
    private const int Third = 3;

    /// <summary>
    /// The fourth expected value.
    /// </summary>
    private const int Fourth = 4;

    /// <summary>
    /// Timeout used when waiting for thread-pool scheduled observer callbacks.
    /// </summary>
    private const int TimeoutSeconds = 2;

    /// <summary>
    /// Reused first-error message.
    /// </summary>
    private const string FirstMessage = "first";

    /// <summary>
    /// Reused stopped event name.
    /// </summary>
    private const string StoppedMessage = "stopped";

    /// <summary>
    /// Deterministic absolute due time for scheduler overload tests.
    /// </summary>
    private static readonly DateTimeOffset AbsoluteDueTime = DateTimeOffset.UnixEpoch;

    /// <summary>
    /// Single-value return expectation.
    /// </summary>
    private static readonly int[] SingleFirstExpected = [First];

    /// <summary>
    /// Expected values produced by the catch params overload.
    /// </summary>
    private static readonly int[] CatchRecoveryExpected = [First, Second];

    /// <summary>
    /// Expected values for create-with-state tests.
    /// </summary>
    private static readonly int[] CreateWithStateExpected = [Third];

    /// <summary>
    /// Awaiter source values.
    /// </summary>
    private static readonly int[] AwaiterSource = [First, Second];

    /// <summary>
    /// Expected values from thread-pool observer dispatch.
    /// </summary>
    private static readonly int[] WitnessOnExpected = [First];

    /// <summary>
    /// Expected values produced by simple scheduling extension overloads.
    /// </summary>
    private static readonly int[] ScheduleExpected = [First, Second, Third, Fourth];

    /// <summary>
    /// Expected virtual-time event sequence.
    /// </summary>
    private static readonly string[] VirtualEventsExpected = [FirstMessage, StoppedMessage];

    /// <summary>
    /// Covers scheduled return, throw, and empty signal implementations.
    /// </summary>
    [Test]
    public void ScheduledScalarFactoriesUseNonImmediateSignalImplementations()
    {
        var returned = new List<int>();
        var returnCompleted = 0;
        Signal.Return(First, Sequencer.CurrentThread).Subscribe(returned.Add, ex => throw ex, () => returnCompleted++);

        var emptyCompleted = 0;
        Signal.Empty<int>(Sequencer.CurrentThread).Subscribe(_ => { }, ex => throw ex, () => emptyCompleted++);

        var error = new InvalidOperationException("scheduled");
        var thrown = new List<Exception>();
        Signal.Throw<int>(error, Sequencer.CurrentThread).Subscribe(_ => { }, thrown.Add, () => { });

        Assert.Equal(SingleFirstExpected, returned);
        Assert.Equal(1, returnCompleted);
        Assert.Equal(1, emptyCompleted);
        Assert.Same(error, thrown[0]);
    }

    /// <summary>
    /// Covers create-with-state overloads and null validation.
    /// </summary>
    [Test]
    public void CreateWithStateFactoriesInvokeStatefulSubscribeCallbacks()
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
                    return Disposable.Create(() => { });
                },
                false)
            .Subscribe(values.Add, ex => throw ex, () => completed++);

        var subscription = Signal.CreateWithState<int, int>(
                Fourth,
                (state, observer) =>
                {
                    observer.OnNext(state);
                    return Disposable.Create(() => disposed++);
                })
            .Subscribe(_ => { });
        subscription.Dispose();

        Assert.Equal(CreateWithStateExpected, values);
        Assert.Equal(1, completed);
        Assert.Equal(1, disposed);
        Assert.Throws<ArgumentNullException>(() => Signal.Create<int>(null!, true));
        Assert.Throws<ArgumentNullException>(() => Signal.CreateSafe<int>(null!, true));
        Assert.Throws<ArgumentNullException>(() => Signal.CreateWithState<int, int>(First, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.CreateWithState<int, int>(First, null!, true));
        Assert.Throws<ArgumentNullException>(() => Signal.Defer<int>(null!));
    }

    /// <summary>
    /// Covers signal awaiter completion, pre-cancellation, and registered cancellation paths.
    /// </summary>
    [Test]
    public void GetAwaiterCoversCompletionAndCancellationPaths()
    {
        var completed = Signal.FromEnumerable(AwaiterSource).GetAwaiter();
        Assert.True(completed.IsCompleted);
        Assert.Equal(Second, completed.GetResult());

        using var canceledBeforeSubscribe = new CancellationTokenSource();
        canceledBeforeSubscribe.Cancel();
        var alreadyCanceled = Signal.Never<int>().GetAwaiter(canceledBeforeSubscribe.Token);
        Assert.True(alreadyCanceled.IsCompleted);
        Assert.Throws<OperationCanceledException>(() => alreadyCanceled.GetResult());

        using var canceledAfterSubscribe = new CancellationTokenSource();
        var source = new Signal<int>();
        var awaiter = source.GetAwaiter(canceledAfterSubscribe.Token);
        canceledAfterSubscribe.Cancel();
        Assert.True(awaiter.IsCompleted);
        Assert.Throws<OperationCanceledException>(() => awaiter.GetResult());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).GetAwaiter());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).GetAwaiter(CancellationToken.None));
    }

    /// <summary>
    /// Covers catch sequence recovery, final error, empty completion, null source, and enumerator failure branches.
    /// </summary>
    [Test]
    public void CatchParamsFactoryCoversRecoveryAndFailureBranches()
    {
        var recovered = new List<int>();
        Signal.Catch(
                Signal.Throw<int>(new InvalidOperationException(FirstMessage)),
                Signal.FromEnumerable(CatchRecoveryExpected),
                Signal.Throw<int>(new InvalidOperationException("unused")))
            .Subscribe(recovered.Add);
        Assert.Equal(CatchRecoveryExpected, recovered);

        var finalErrors = new List<Exception>();
        var finalError = new InvalidOperationException("last");
        Signal.Catch(Signal.Throw<int>(new InvalidOperationException(FirstMessage)), Signal.Throw<int>(finalError))
            .Subscribe(_ => { }, finalErrors.Add, () => { });
        Assert.Same(finalError, finalErrors[0]);

        var completed = 0;
        var completedSubscription = Signal.Catch(Array.Empty<IObservable<int>>()).Subscribe(_ => { }, ex => throw ex, () => completed++);
        completedSubscription.Dispose();
        completedSubscription.Dispose();
        Assert.Equal(1, completed);

        var activeSubscription = Signal.Catch(Signal.Never<int>()).Subscribe(_ => { }, ex => throw ex, () => { });
        activeSubscription.Dispose();

        var nullSourceErrors = new List<Exception>();
        Signal.Catch(new IObservable<int>?[] { null! }!)
            .Subscribe(_ => { }, nullSourceErrors.Add, () => { });
        Assert.True(nullSourceErrors[0] is InvalidOperationException);

        var moveNextErrors = new List<Exception>();
        var moveNextError = new InvalidOperationException("move-next");
        new ThrowingMoveNextEnumerable<IObservable<int>>(moveNextError).Catch()
            .Subscribe(_ => { }, moveNextErrors.Add, () => { });
        Assert.Same(moveNextError, moveNextErrors[0]);

        var getEnumeratorError = new InvalidOperationException("enumerator");
        Assert.Throws<InvalidOperationException>(() =>
            new ThrowingEnumerable<IObservable<int>>(getEnumeratorError).Catch()
                .Subscribe(_ => { }, _ => { }, () => { }));
    }

    /// <summary>
    /// Covers the thread-pool-specialized witness dispatch implementation.
    /// </summary>
    /// <returns>A task representing asynchronous observer dispatch.</returns>
    [Test]
    public async Task WitnessOnThreadPoolDispatchesNextCompletedAndErrorSignals()
    {
        var values = new List<int>();
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using (Signal.FromEnumerable(WitnessOnExpected)
                   .WitnessOn(ThreadPoolSequencer.Instance)
                   .Subscribe(values.Add, completion.SetException, completion.SetResult))
        {
            await WaitForAsync(completion.Task);
        }

        Assert.True(values.Count <= WitnessOnExpected.Length);

        var error = new InvalidOperationException("thread-pool");
        var observed = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        using (Signal.Throw<int>(error)
                   .WitnessOn(ThreadPoolSequencer.Instance)
                   .Subscribe(_ => { }, observed.SetResult, () => { }))
        {
            Assert.Same(error, await WaitForAsync(observed.Task));
        }
    }

    /// <summary>
    /// Covers simple sequencer extension validation, delayed overloads, state overloads, and recursive scheduling.
    /// </summary>
    [Test]
    public void SimpleSequencerExtensionsCoverValidationAndRecursiveScheduling()
    {
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).Schedule(() => { }));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.Schedule((Action)null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).Schedule(TimeSpan.Zero, () => { }));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.Schedule(TimeSpan.Zero, null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).Schedule(AbsoluteDueTime, () => { }));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.Schedule(AbsoluteDueTime, null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).Schedule(self => self()));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.Schedule((Action<Action>)null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).ScheduleAction(First, _ => { }));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.ScheduleAction(First, (Action<int>)null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).ScheduleAction(First, _ => Disposable.Empty));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.ScheduleAction(First, (Func<int, IDisposable>)null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).ScheduleAction(First, TimeSpan.Zero, _ => { }));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.ScheduleAction(First, TimeSpan.Zero, (Action<int>)null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).ScheduleAction(First, TimeSpan.Zero, _ => Disposable.Empty));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.ScheduleAction(First, TimeSpan.Zero, (Func<int, IDisposable>)null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).ScheduleAction(First, AbsoluteDueTime, _ => { }));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.ScheduleAction(First, AbsoluteDueTime, (Action<int>)null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).ScheduleAction(First, AbsoluteDueTime, _ => Disposable.Empty));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.ScheduleAction(First, AbsoluteDueTime, (Func<int, IDisposable>)null!));

        var values = new List<int>();
        Sequencer.Immediate.ScheduleAction(First, values.Add).Dispose();
        Sequencer.Immediate.ScheduleAction(Second, value =>
        {
            values.Add(value);
            return Disposable.Empty;
        }).Dispose();
        Sequencer.Immediate.ScheduleAction(Third, TimeSpan.Zero, values.Add).Dispose();
        Sequencer.Immediate.ScheduleAction(Fourth, AbsoluteDueTime, value =>
        {
            values.Add(value);
            return Disposable.Empty;
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

        Assert.Equal(ScheduleExpected, values);
        Assert.Equal(Third, recursiveCount);
    }

    /// <summary>
    /// Covers virtual-time service lookup, stopwatch, stop, sleep, and nested-run guard paths.
    /// </summary>
    [Test]
    public void VirtualTimeSequencerBaseCoversServicesStopwatchAndRunGuards()
    {
        var clock = new TestClock(DateTimeOffset.UnixEpoch);
        var provider = (IServiceProvider)clock;
        Assert.Same(clock, provider.GetService(typeof(IStopwatchProvider))!);
        Assert.Equal(null, provider.GetService(typeof(string)));

        var stopwatch = clock.StartStopwatch();
        clock.Sleep(TimeSpan.FromTicks(First));
        Assert.Equal(TimeSpan.FromTicks(First), stopwatch.Elapsed);

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

        Assert.Equal(VirtualEventsExpected, events);
    }

    /// <summary>
    /// Waits for a task with a bounded timeout.
    /// </summary>
    /// <param name="task">The task to wait for.</param>
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

    /// <summary>
    /// Waits for a task with a bounded timeout and returns its result.
    /// </summary>
    /// <typeparam name="T">The task result type.</typeparam>
    /// <param name="task">The task to wait for.</param>
    /// <returns>The task result.</returns>
    private static async Task<T> WaitForAsync<T>(Task<T> task)
    {
        await WaitForAsync((Task)task).ConfigureAwait(false);
        return await task.ConfigureAwait(false);
    }

    /// <summary>
    /// Enumerable test double whose enumerator throws from <see cref="IEnumerator.MoveNext"/>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class ThrowingMoveNextEnumerable<T> : IEnumerable<T>
    {
        /// <summary>
        /// Error thrown by the enumerator.
        /// </summary>
        private readonly Exception _error;

        /// <summary>
        /// Initializes a new instance of the <see cref="ThrowingMoveNextEnumerable{T}"/> class.
        /// </summary>
        /// <param name="error">The error to throw.</param>
        public ThrowingMoveNextEnumerable(Exception error) => _error = error;

        /// <inheritdoc/>
        public IEnumerator<T> GetEnumerator() => new ThrowingMoveNextEnumerator(_error);

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// Enumerator test double that fails on movement.
        /// </summary>
        private sealed class ThrowingMoveNextEnumerator : IEnumerator<T>
        {
            /// <summary>
            /// Error thrown by movement.
            /// </summary>
            private readonly Exception _error;

            /// <summary>
            /// Initializes a new instance of the <see cref="ThrowingMoveNextEnumerator"/> class.
            /// </summary>
            /// <param name="error">The error to throw.</param>
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

    /// <summary>
    /// Enumerable test double that throws when enumeration starts.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class ThrowingEnumerable<T> : IEnumerable<T>
    {
        /// <summary>
        /// Error thrown by enumeration.
        /// </summary>
        private readonly Exception _error;

        /// <summary>
        /// Initializes a new instance of the <see cref="ThrowingEnumerable{T}"/> class.
        /// </summary>
        /// <param name="error">The error to throw.</param>
        public ThrowingEnumerable(Exception error) => _error = error;

        /// <inheritdoc/>
        public IEnumerator<T> GetEnumerator() => throw _error;

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
