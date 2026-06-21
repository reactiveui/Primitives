// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Collections.Concurrent;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="ScheduledSignal{T}"/>.</summary>
public sealed class ScheduledSignalTests
{
    /// <summary>The first emitted value.</summary>
    private const int FirstValue = 1;

    /// <summary>The second emitted value.</summary>
    private const int SecondValue = 2;

    /// <summary>The third emitted value.</summary>
    private const int ThirdValue = 3;

    /// <summary>The fourth emitted value.</summary>
    private const int FourthValue = 4;

    /// <summary>The final emitted value used by stress tests.</summary>
    private const int FinalValue = 999;

    /// <summary>The number of concurrent workers used by stress tests.</summary>
    private const int WorkerCount = 8;

    /// <summary>The number of subscribe/dispose iterations each stress worker runs.</summary>
    private const int WorkerIterations = 64;

    /// <summary>The expected terminal exception message.</summary>
    private const string TerminalThrowMessage = "terminal";

    /// <summary>The default observer receives the original terminal event and the restored replay.</summary>
    private const int DefaultObserverTerminalReplayCount = 2;

    /// <summary>Constructor validates the scheduler argument.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullScheduler()
    {
        await Assert.That(() => new ScheduledSignal<int>(null!)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>A signal without a default observer schedules values and completion for subscribers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WithoutDefaultObserverSchedulesValuesAndCompletion()
    {
        var sequencer = new QueuedSequencer();
        var observer = new RecordingObserver<int>();
        using var signal = new ScheduledSignal<int>(sequencer);
        await Assert.That(signal.HasObservers).IsFalse();
        using var subscription = signal.Subscribe(observer);
        await Assert.That(signal.HasObservers).IsTrue();

        signal.OnNext(FirstValue);

        await Assert.That(observer.Values.Length).IsEqualTo(0);
        await Assert.That(sequencer.ScheduleCount).IsEqualTo(1);

        sequencer.DrainAll();

        await Assert.That(observer.Values.SequenceEqual([FirstValue])).IsTrue();

        signal.OnCompleted();

        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(signal.HasObservers).IsFalse();

        sequencer.DrainAll();

        await Assert.That(observer.Completed).IsEqualTo(1);
    }

    /// <summary>The default observer is disabled while explicit subscribers are present and restored afterwards.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DefaultObserverIsRestoredAfterLastSubscriberDisposes()
    {
        var sequencer = new QueuedSequencer();
        var defaultObserver = new RecordingObserver<int>();
        using var signal = new ScheduledSignal<int>(sequencer, defaultObserver);
        await Assert.That(signal.HasObservers).IsTrue();

        signal.OnNext(FirstValue);
        sequencer.DrainAll();

        var firstObserver = new RecordingObserver<int>();
        var secondObserver = new RecordingObserver<int>();
        using var firstSubscription = signal.Subscribe(firstObserver);
        using var secondSubscription = signal.Subscribe(secondObserver);

        signal.OnNext(SecondValue);
        sequencer.DrainAll();

        firstSubscription.Dispose();

        signal.OnNext(ThirdValue);
        sequencer.DrainAll();

        secondSubscription.Dispose();

        signal.OnNext(FourthValue);
        sequencer.DrainAll();

        await Assert.That(defaultObserver.Values.SequenceEqual([FirstValue, FourthValue])).IsTrue();
        await Assert.That(firstObserver.Values.SequenceEqual([SecondValue])).IsTrue();
        await Assert.That(secondObserver.Values.SequenceEqual([SecondValue, ThirdValue])).IsTrue();
    }

    /// <summary>Errors are scheduled and null errors are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ErrorNotificationsAreScheduledAndNullErrorsAreRejected()
    {
        var sequencer = new QueuedSequencer();
        var observer = new RecordingObserver<int>();
        using var signal = new ScheduledSignal<int>(sequencer);
        using var subscription = signal.Subscribe(observer);
        var expected = new InvalidOperationException("expected");

        signal.OnError(expected);

        await Assert.That(observer.Errors.Length).IsEqualTo(0);
        await Assert.That(signal.HasObservers).IsFalse();

        sequencer.DrainAll();

        await Assert.That(observer.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(observer.Completed).IsEqualTo(0);

        using var nullErrorSignal = new ScheduledSignal<int>(Sequencer.Immediate);
        await Assert.That(() => nullErrorSignal.OnError(null!)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Dispose is idempotent and prevents future use.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeDisposesObserversAndRejectsFutureUse()
    {
        var sequencer = new QueuedSequencer();
        var defaultObserver = new RecordingObserver<int>();
        var signal = new ScheduledSignal<int>(sequencer, defaultObserver);
        var observer = new RecordingObserver<int>();
        var subscription = signal.Subscribe(observer);

        signal.Dispose();
        subscription.Dispose();
        signal.Dispose();

        await Assert.That(signal.IsDisposed).IsTrue();
        await Assert.That(signal.HasObservers).IsFalse();
        await Assert.That(defaultObserver.Values.Length).IsEqualTo(0);
        await Assert.That(() => signal.Subscribe(new RecordingObserver<int>())).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(() => signal.OnNext(FirstValue)).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(signal.OnCompleted).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(() => signal.OnError(new InvalidOperationException())).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>The protected dispose path marks the signal disposed when managed cleanup is not requested.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeWithoutManagedCleanupMarksSignalDisposed()
    {
        var signal = new ExposedScheduledSignal<int>(Sequencer.Immediate);

        signal.DisposeWithoutManagedCleanup();

        await Assert.That(signal.IsDisposed).IsTrue();
        await Assert.That(() => signal.OnNext(FirstValue)).ThrowsExactly<ObjectDisposedException>();
        signal.Dispose();
    }

    /// <summary>Subscribe rollback restores the default observer when a late terminal observer throws.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeRestoresDefaultObserverWhenTerminalReplayThrows()
    {
        var defaultObserver = new RecordingObserver<int>();
        using var signal = new ScheduledSignal<int>(Sequencer.Immediate, defaultObserver);
        signal.OnCompleted();

        var exception = await Assert.That(() => signal.Subscribe(new ThrowingTerminalObserver<int>())).ThrowsExactly<InvalidOperationException>();

        await Assert.That(exception!.Message).IsEqualTo(TerminalThrowMessage);
        await Assert.That(defaultObserver.Completed).IsEqualTo(DefaultObserverTerminalReplayCount);

        var lateObserver = new RecordingObserver<int>();
        using var subscription = signal.Subscribe(lateObserver);

        await Assert.That(lateObserver.Completed).IsEqualTo(1);
    }

    /// <summary>Concurrent subscribe and dispose operations do not prevent default-observer restoration.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConcurrentSubscribeDisposeRestoresDefaultObserver()
    {
        var defaultObserver = new RecordingObserver<int>();
        using var signal = new ScheduledSignal<int>(Sequencer.Immediate, defaultObserver);
        var tasks = Enumerable.Range(0, WorkerCount)
            .Select(worker => Task.Run(() => SubscribeAndDispose(signal, worker)))
            .ToArray();

        await Task.WhenAll(tasks);

        signal.OnNext(FinalValue);

        await Assert.That(defaultObserver.Values.Contains(FinalValue)).IsTrue();
        await Assert.That(signal.IsDisposed).IsFalse();
    }

    /// <summary>Subscribes and disposes observers repeatedly.</summary>
    /// <param name="signal">The scheduled signal under test.</param>
    /// <param name="worker">The worker identifier.</param>
    private static void SubscribeAndDispose(ScheduledSignal<int> signal, int worker)
    {
        for (var i = 0; i < WorkerIterations; i++)
        {
            using var subscription = signal.Subscribe(new RecordingObserver<int>());
            signal.OnNext((worker * WorkerIterations) + i);
        }
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

    /// <summary>Observer that throws when replayed a terminal notification.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class ThrowingTerminalObserver<T> : IObserver<T>
    {
        /// <inheritdoc />
        public void OnCompleted() => throw new InvalidOperationException(TerminalThrowMessage);

        /// <inheritdoc />
        public void OnError(Exception error) => throw new InvalidOperationException(TerminalThrowMessage, error);

        /// <inheritdoc />
        public void OnNext(T value)
        {
        }
    }

    /// <summary>Scheduled signal subclass exposing the protected dispose overload.</summary>
    /// <typeparam name="T">The signal value type.</typeparam>
    private sealed class ExposedScheduledSignal<T> : ScheduledSignal<T>
    {
        /// <summary>Initializes a new instance of the <see cref="ExposedScheduledSignal{T}"/> class.</summary>
        /// <param name="scheduler">The scheduler value.</param>
        public ExposedScheduledSignal(ISequencer scheduler)
            : base(scheduler)
        {
        }

        /// <summary>Invokes the protected dispose overload without managed cleanup.</summary>
        public void DisposeWithoutManagedCleanup() => Dispose(disposing: false);
    }
}
