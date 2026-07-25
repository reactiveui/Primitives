// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="PrioritySemaphoreSignal{T}"/>.</summary>
public sealed class PrioritySemaphoreSignalTests
{
    /// <summary>The first emitted value.</summary>
    private const int FirstValue = 1;

    /// <summary>The second emitted value.</summary>
    private const int SecondValue = 2;

    /// <summary>The third emitted value.</summary>
    private const int ThirdValue = 3;

    /// <summary>The fourth emitted value.</summary>
    private const int FourthValue = 4;

    /// <summary>The initial maximum count.</summary>
    private const int InitialMaximumCount = 1;

    /// <summary>The number of values expected after the first drain.</summary>
    private const int FirstDrainCount = 2;

    /// <summary>The number of worker tasks used to stress concurrent signaling.</summary>
    private const int StressWorkers = 4;

    /// <summary>The number of operations each stress worker performs.</summary>
    private const int StressIterations = 4;

    /// <summary>The number of seeded values for the concurrent drain scenario.</summary>
    private const int SeededValueCount = 12;

    /// <summary>The initial semaphore capacity used in the concurrent drain scenario.</summary>
    private const int InitialDrainCapacity = 3;

    /// <summary>The amount added during alternating capacity updates.</summary>
    private const int CapacityJitter = 2;

    /// <summary>The number of polling loops while waiting for signal drain.</summary>
    private const int PollIterations = 500;

    /// <summary>The wait duration for each drain polling loop.</summary>
    private const int PollDelayMilliseconds = 1;

    /// <summary>Task-group offsets for the concurrent operations phase.</summary>
    private const int OnNextTaskOffsetMultiplier = 2;

    /// <summary>The number of worker groups (release, capacity, and OnNext) started for each stress worker.</summary>
    private const int StressWorkerGroups = 3;

    /// <summary>The wait duration for terminal serialization probes.</summary>
    private static readonly TimeSpan TerminalProbeTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Constructor and observer validation follow the inner signal contract.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorAndSubscribeValidationFollowSignalContract()
    {
        using var signal = new PrioritySemaphoreSignal<int>(InitialMaximumCount);

        await Assert.That(signal.MaximumCount).IsEqualTo(InitialMaximumCount);
        await Assert.That(signal.HasObservers).IsFalse();
        await Assert.That(signal.IsDisposed).IsFalse();
        await Assert.That(() => signal.Subscribe(null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => signal.OnError(null!)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Queued values drain in priority order as semaphore slots are released.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task QueuedValuesDrainByPriorityWhenCapacityIsAvailable()
    {
        using var signal = new PrioritySemaphoreSignal<int>(0);
        var observer = new RecordingObserver<int>();
        var subscription = signal.Subscribe(observer);
        await Assert.That(signal.HasObservers).IsTrue();

        signal.OnNext(ThirdValue);
        signal.OnNext(FirstValue);
        signal.OnNext(SecondValue);

        await Assert.That(observer.Values.Length).IsEqualTo(0);

        signal.MaximumCount = FirstDrainCount;

        await Assert.That(signal.MaximumCount).IsEqualTo(FirstDrainCount);
        await Assert.That(observer.Values.SequenceEqual([FirstValue, SecondValue])).IsTrue();

        signal.Release();
        signal.Release();

        await Assert.That(observer.Values.SequenceEqual([FirstValue, SecondValue, ThirdValue])).IsTrue();

        subscription.Dispose();

        await Assert.That(signal.HasObservers).IsFalse();
    }

    /// <summary>Completion drains queued values and ignores later attempts to enqueue or complete again.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompletedSignalDrainsRemainingValuesAndStopsQueue()
    {
        using var signal = new PrioritySemaphoreSignal<int>(0);
        var observer = new RecordingObserver<int>();
        using var subscription = signal.Subscribe(observer);

        signal.OnNext(SecondValue);
        signal.OnNext(FirstValue);
        signal.OnCompleted();
        signal.OnCompleted();
        signal.OnNext(ThirdValue);
        signal.MaximumCount = 1;

        await Assert.That(observer.Values.SequenceEqual([FirstValue, SecondValue])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(observer.Errors.Length).IsEqualTo(0);
    }

    /// <summary>Releasing more times than capacity usage does not allow negative count or extra capacity.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReleaseDoesNotAllowNegativeCount()
    {
        using var signal = new PrioritySemaphoreSignal<int>(1);
        var observer = new RecordingObserver<int>();
        var subscription = signal.Subscribe(observer);

        signal.OnNext(FirstValue);

        signal.Release();
        signal.Release();
        signal.Release();

        signal.OnNext(SecondValue);
        signal.OnNext(ThirdValue);

        await Assert.That(observer.Values.SequenceEqual([FirstValue, SecondValue])).IsTrue();

        signal.Release();

        await Assert.That(observer.Values.SequenceEqual([FirstValue, SecondValue, ThirdValue])).IsTrue();

        subscription.Dispose();
    }

    /// <summary>Error stops the queue and forwards the exact exception.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ErrorStopsQueueAndForwardsException()
    {
        using var signal = new PrioritySemaphoreSignal<int>(0);
        var observer = new RecordingObserver<int>();
        using var subscription = signal.Subscribe(observer);
        var expected = new InvalidOperationException("expected");

        signal.OnNext(FirstValue);
        signal.OnError(expected);
        signal.OnNext(SecondValue);
        signal.MaximumCount = 1;

        await Assert.That(observer.Values.Length).IsEqualTo(0);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>A scheduled priority semaphore emits through the configured sequencer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduledInnerSignalDeliversThroughSequencerAndDisposes()
    {
        var sequencer = new QueuedSequencer();
        var observer = new RecordingObserver<int>();
        var signal = new PrioritySemaphoreSignal<int>(InitialMaximumCount, sequencer);
        using var subscription = signal.Subscribe(observer);

        signal.OnNext(FourthValue);

        await Assert.That(observer.Values.Length).IsEqualTo(0);
        await Assert.That(sequencer.ScheduleCount).IsEqualTo(1);

        sequencer.DrainAll();

        await Assert.That(observer.Values.SequenceEqual([FourthValue])).IsTrue();

        signal.Dispose();
        signal.Dispose();
        signal.OnNext(FirstValue);

        sequencer.DrainAll();

        await Assert.That(signal.IsDisposed).IsTrue();
        await Assert.That(signal.HasObservers).IsFalse();
        await Assert.That(observer.Values.SequenceEqual([FourthValue])).IsTrue();
    }

    /// <summary>Concurrent release and production drain work without concurrent downstream <see langword="OnNext"/> calls.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConcurrentOperationsDoNotNotifyObserverConcurrently()
    {
        using var signal = new PrioritySemaphoreSignal<int>(InitialDrainCapacity);
        var observer = new ConcurrencyProbe();
        var subscription = signal.Subscribe(observer);
        for (var i = 0; i < SeededValueCount; i++)
        {
            signal.OnNext(i);
        }

        var go = new ManualResetEventSlim();
        var tasks = new Task[StressWorkers * StressWorkerGroups];
        var next = 0;
        for (var t = 0; t < StressWorkers; t++)
        {
            tasks[t] = Task.Run(() =>
            {
                go.Wait();
                for (var i = 0; i < StressIterations; i++)
                {
                    signal.Release();
                    signal.OnNext(Interlocked.Increment(ref next));
                }
            });
        }

        for (var t = 0; t < StressWorkers; t++)
        {
            tasks[StressWorkers + t] = Task.Run(() =>
            {
                go.Wait();
                for (var i = 0; i < StressIterations; i++)
                {
                    signal.MaximumCount = InitialDrainCapacity + (i % CapacityJitter);
                }
            });
        }

        for (var t = 0; t < StressWorkers; t++)
        {
            tasks[(StressWorkers * OnNextTaskOffsetMultiplier) + t] = Task.Run(() =>
            {
                go.Wait();
                for (var i = 0; i < StressIterations; i++)
                {
                    signal.OnNext(InitialDrainCapacity + Interlocked.Increment(ref next));
                }
            });
        }

        go.Set();
        await Task.WhenAll(tasks);

        signal.MaximumCount = int.MaxValue;

        const int ExpectedValues = SeededValueCount + (StressWorkers * StressIterations * 2);
        for (var i = 0; i < PollIterations && observer.OnNextCount < ExpectedValues; i++)
        {
            await Task.Delay(PollDelayMilliseconds);
        }

        await Assert.That(observer.OverlapDetected).IsFalse();
        await Assert.That(observer.OnNextCount).IsEqualTo(ExpectedValues);

        subscription.Dispose();
    }

    /// <summary>Terminal completion notifications remain serialized with value delivery under concurrent drain.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConcurrentCompletionDoesNotOverlapObserverOnNext()
    {
        using var signal = new PrioritySemaphoreSignal<int>(0);
        using var releaseOnNext = new ManualResetEventSlim();
        using var observer = new TerminalOverlapProbe(releaseOnNext);
        using var subscription = signal.Subscribe(observer);

        signal.OnNext(ThirdValue);
        signal.OnNext(FirstValue);
        signal.OnNext(SecondValue);

        // Drive the drain on a dedicated thread so the probe can block it without starving the
        // thread pool; the started gate is observed synchronously to keep the race deterministic.
        var drainThread = StartThread(() => signal.MaximumCount = 1);
        await Assert.That(observer.WaitForOnNextStarted(TerminalProbeTimeout)).IsTrue();

        var completionThread = StartThread(signal.OnCompleted);

        releaseOnNext.Set();
        drainThread.Join();
        completionThread.Join();

        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(observer.Values.SequenceEqual([FirstValue, SecondValue, ThirdValue])).IsTrue();
        await Assert.That(observer.OverlapDetected).IsFalse();
    }

    /// <summary>Terminal error notifications remain serialized with value delivery under concurrent drain.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConcurrentErrorDoesNotOverlapObserverOnNext()
    {
        using var signal = new PrioritySemaphoreSignal<int>(0);
        using var releaseOnNext = new ManualResetEventSlim();
        using var observer = new TerminalOverlapProbe(releaseOnNext);
        using var subscription = signal.Subscribe(observer);
        var expected = new InvalidOperationException("expected");

        signal.OnNext(ThirdValue);
        signal.OnNext(FirstValue);
        signal.OnNext(SecondValue);

        // Drive the drain on a dedicated thread so the probe can block it without starving the
        // thread pool; the started gate is observed synchronously to keep the race deterministic.
        var drainThread = StartThread(() => signal.MaximumCount = 1);
        await Assert.That(observer.WaitForOnNextStarted(TerminalProbeTimeout)).IsTrue();

        var errorThread = StartThread(() => signal.OnError(expected));

        releaseOnNext.Set();
        drainThread.Join();
        errorThread.Join();

        await Assert.That(observer.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(observer.Values.Length).IsEqualTo(1);
        await Assert.That(observer.OverlapDetected).IsFalse();
    }

    /// <summary>A terminal notification arriving after the signal is already terminal is ignored.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorAfterTerminalIsIgnored()
    {
        using var signal = new PrioritySemaphoreSignal<int>(InitialMaximumCount);
        var observer = new RecordingObserver<int>();
        using var subscription = signal.Subscribe(observer);
        var first = new InvalidOperationException("first");

        signal.OnError(first);
        signal.OnError(new InvalidOperationException("second"));

        await Assert.That(observer.Errors.Length).IsEqualTo(1);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(first);
    }

    /// <summary>A throwing delivery still releases drain ownership so a later release can drain again.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DrainReleasesOwnershipWhenDeliveryThrows()
    {
        using var signal = new PrioritySemaphoreSignal<int>(InitialMaximumCount);
        var failure = new InvalidOperationException("boom");
        var deliveries = 0;
        using var subscription = signal.Subscribe(
            _ =>
            {
                deliveries++;
                throw failure;
            },
            static _ => { },
            static () => { });

        // Delivery throws out of the drain loop; the finally path must still release ownership.
        var first = Assert.Throws<InvalidOperationException>(() => signal.OnNext(FirstValue));
        await Assert.That(first).IsSameReferenceAs(failure);

        // Capacity is now exhausted, so this value only enqueues.
        signal.OnNext(SecondValue);

        // Releasing frees capacity and must begin a fresh drain, proving ownership was released.
        var second = Assert.Throws<InvalidOperationException>(signal.Release);
        await Assert.That(second).IsSameReferenceAs(failure);
        await Assert.That(deliveries).IsEqualTo(FirstDrainCount);
    }

    /// <summary>Starts a background thread running the supplied action.</summary>
    /// <param name="action">The work to run.</param>
    /// <returns>The started thread.</returns>
    private static Thread StartThread(Action action)
    {
        var thread = new Thread(() => action()) { IsBackground = true };
        thread.Start();
        return thread;
    }

    /// <summary>Test sequencer that queues scheduled work until drained explicitly.</summary>
    private sealed class QueuedSequencer : ISequencer
    {
        /// <summary>A fixed deterministic clock value for the test sequencer.</summary>
        private static readonly DateTimeOffset FixedNow = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        /// <summary>The queue of scheduled work items awaiting drain.</summary>
        private readonly ConcurrentQueue<IWorkItem> _items = new();

        /// <summary>The number of scheduled work items.</summary>
        private int _scheduleCount;

        /// <inheritdoc />
        public DateTimeOffset Now => FixedNow;

        /// <inheritdoc />
        public long Timestamp => FixedNow.Ticks;

        /// <summary>Gets the number of scheduled work items.</summary>
        public int ScheduleCount => Volatile.Read(ref _scheduleCount);

        /// <inheritdoc />
        public void Schedule(IWorkItem item)
        {
            _ = Interlocked.Increment(ref _scheduleCount);
            _items.Enqueue(item);
        }

        /// <inheritdoc />
        public void Schedule(IWorkItem item, long dueTimestamp) => Schedule(item);

        /// <summary>Executes all queued work items.</summary>
        public void DrainAll()
        {
            while (_items.TryDequeue(out var item))
            {
                item.Execute();
            }
        }
    }

    /// <summary>Observer that records all received notifications.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class RecordingObserver<T> : IObserver<T>
    {
        /// <summary>Guards notification state.</summary>
        private readonly Lock _gate = new();

        /// <summary>The recorded values.</summary>
        private readonly List<T> _values = [];

        /// <summary>The recorded errors.</summary>
        private readonly List<Exception> _errors = [];

        /// <summary>The number of completed notifications.</summary>
        private int _completed;

        /// <summary>Gets the recorded values.</summary>
        public T[] Values
        {
            get
            {
                lock (_gate)
                {
                    return [.. _values];
                }
            }
        }

        /// <summary>Gets the recorded errors.</summary>
        public Exception[] Errors
        {
            get
            {
                lock (_gate)
                {
                    return [.. _errors];
                }
            }
        }

        /// <summary>Gets the completed notification count.</summary>
        public int Completed
        {
            get
            {
                lock (_gate)
                {
                    return _completed;
                }
            }
        }

        /// <inheritdoc />
        public void OnCompleted()
        {
            lock (_gate)
            {
                _completed++;
            }
        }

        /// <inheritdoc />
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                _errors.Add(error);
            }
        }

        /// <inheritdoc />
        public void OnNext(T value)
        {
            lock (_gate)
            {
                _values.Add(value);
            }
        }
    }

    /// <summary>Observer used to detect concurrent <see cref="IObserver{T}.OnCompleted"/> and <see cref="IObserver{T}.OnError"/> overlap.</summary>
    private sealed class TerminalOverlapProbe : IObserver<int>, IDisposable
    {
        /// <summary>Gate to hold the first value until assertions can observe interleaving.</summary>
        private readonly ManualResetEventSlim _releaseOnNext;

        /// <summary>Guards internal state.</summary>
        private readonly Lock _gate = new();

        /// <summary>Captured errors.</summary>
        private readonly List<Exception> _errors = [];

        /// <summary>Captured values.</summary>
        private readonly List<int> _values = [];

        /// <summary>Signals that one <see cref="OnNext"/> value started delivering.</summary>
        private readonly ManualResetEventSlim _onNextStarted = new();

        /// <summary>Whether the observer is currently inside <see cref="OnNext"/>.</summary>
        private int _insideOnNext;

        /// <summary>Whether a terminal notification was observed while a value was being delivered.</summary>
        private int _overlapDetected;

        /// <summary>Whether a value was emitted.</summary>
        private int _onNextCount;

        /// <summary>Completed notification count.</summary>
        private int _completed;

        /// <summary>Initializes a new instance of the <see cref="TerminalOverlapProbe"/> class.</summary>
        /// <param name="releaseOnNext">Gate used by tests to hold <see cref="OnNext"/> in-flight.</param>
        public TerminalOverlapProbe(ManualResetEventSlim releaseOnNext) => _releaseOnNext = releaseOnNext;

        /// <summary>Gets whether overlapping terminal and value notifications were observed.</summary>
        public bool OverlapDetected => Volatile.Read(ref _overlapDetected) != 0;

        /// <summary>Gets the number of delivered values.</summary>
        public int[] Values
        {
            get
            {
                lock (_gate)
                {
                    return [.. _values];
                }
            }
        }

        /// <summary>Gets the number of completed notifications.</summary>
        public int Completed => Volatile.Read(ref _completed);

        /// <summary>Gets the errors delivered to this observer.</summary>
        public Exception[] Errors
        {
            get
            {
                lock (_gate)
                {
                    return [.. _errors];
                }
            }
        }

        /// <summary>Blocks until <see cref="OnNext"/> has started delivering at least one value.</summary>
        /// <param name="timeout">The maximum time to wait.</param>
        /// <returns><see langword="true"/> when delivery started within the timeout.</returns>
        public bool WaitForOnNextStarted(TimeSpan timeout) => _onNextStarted.Wait(timeout);

        /// <inheritdoc />
        public void Dispose() => _onNextStarted.Dispose();

        /// <inheritdoc />
        public void OnNext(int value)
        {
            if (Interlocked.Exchange(ref _insideOnNext, 1) != 0)
            {
                _ = Interlocked.Exchange(ref _overlapDetected, 1);
            }

            lock (_gate)
            {
                _onNextCount++;
                _values.Add(value);
            }

            _onNextStarted.Set();
            _releaseOnNext.Wait();
            _ = Interlocked.Exchange(ref _insideOnNext, 0);
        }

        /// <inheritdoc />
        public void OnCompleted()
        {
            if (Volatile.Read(ref _insideOnNext) != 0)
            {
                _ = Interlocked.Exchange(ref _overlapDetected, 1);
            }

            _ = Interlocked.Increment(ref _completed);
        }

        /// <inheritdoc />
        public void OnError(Exception error)
        {
            if (Volatile.Read(ref _insideOnNext) != 0)
            {
                _ = Interlocked.Exchange(ref _overlapDetected, 1);
            }

            lock (_gate)
            {
                _errors.Add(error);
            }
        }
    }

    /// <summary>Observer used to detect overlapping <see cref="IObserver{T}.OnNext"/> notifications.</summary>
    private sealed class ConcurrencyProbe : IObserver<int>
    {
        /// <summary>The spin delay used to amplify overlap detection.</summary>
        private const int ProbeSpinWaitIterations = 1000;

        /// <summary>Non-zero while a notification is in-flight.</summary>
        private int _inside;

        /// <summary>Whether two notifications overlapped.</summary>
        private int _overlapDetected;

        /// <summary>Count of delivered notifications.</summary>
        private int _onNextCount;

        /// <summary>Gets whether overlapping notifications were observed.</summary>
        public bool OverlapDetected => Volatile.Read(ref _overlapDetected) != 0;

        /// <summary>Gets the number of delivered notifications.</summary>
        public int OnNextCount => Volatile.Read(ref _onNextCount);

        /// <summary>Records a single notification value, pausing briefly to amplify overlap races.</summary>
        /// <param name="value">The observed value.</param>
        public void OnNext(int value)
        {
            if (Interlocked.Exchange(ref _inside, 1) != 0)
            {
                _ = Interlocked.Exchange(ref _overlapDetected, 1);
            }

            _ = Interlocked.Increment(ref _onNextCount);
            Thread.SpinWait(ProbeSpinWaitIterations);
            _ = Interlocked.Exchange(ref _inside, 0);
        }

        /// <summary>Implements the generic observer interface for value-only checks.</summary>
        public void OnCompleted()
        {
        }

        /// <summary>Implements the generic observer interface for value-only checks.</summary>
        /// <param name="error">The observed error.</param>
        public void OnError(Exception error)
        {
        }
    }
}
