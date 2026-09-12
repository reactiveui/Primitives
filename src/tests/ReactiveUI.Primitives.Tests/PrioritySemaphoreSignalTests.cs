// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
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

    /// <summary>The number of reentrant operation groups.</summary>
    private const int OperationGroups = 4;

    /// <summary>The number of release and production pairs per group.</summary>
    private const int OperationsPerGroup = 4;

    /// <summary>The number of values queued before draining.</summary>
    private const int SeededValueCount = 12;

    /// <summary>The initial capacity that governs how many queued values drain.</summary>
    private const int InitialDrainCapacity = 3;

    /// <summary>The amount added during alternating capacity updates.</summary>
    private const int CapacityJitter = 2;

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

    /// <summary>Reentrant release and production preserve serialized value delivery.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReentrantOperationsDoNotOverlapValueDelivery()
{
        using var signal = new PrioritySemaphoreSignal<int>(0);
        var inside = false;
        var overlap = false;
        var count = 0;
        var next = 0;
        using var subscription = signal.Subscribe(_ =>
        {
            overlap |= inside;
            inside = true;
            count++;
            if (count == 1)
            {
                for (var group = 0; group < OperationGroups; group++)
                {
                    for (var iteration = 0; iteration < OperationsPerGroup; iteration++)
                    {
                        signal.Release();
                        next++;
                        signal.OnNext(next);
                        signal.MaximumCount = InitialDrainCapacity + (iteration % CapacityJitter);
                        next++;
                        signal.OnNext(InitialDrainCapacity + next);
                    }
                }
            }

            inside = false;
        });
        for (var value = 0; value < SeededValueCount; value++)
        {
            signal.OnNext(value);
        }

        signal.MaximumCount = InitialDrainCapacity;
        signal.MaximumCount = int.MaxValue;
        const int ExpectedValues = SeededValueCount + (OperationGroups * OperationsPerGroup * 2);
        await Assert.That(overlap).IsFalse();
        await Assert.That(count).IsEqualTo(ExpectedValues);
    }

    /// <summary>Reentrant completion waits until value delivery returns.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReentrantCompletionWaitsForValueDelivery()
{
        using var signal = new PrioritySemaphoreSignal<int>(0);
        List<int> values = [];
        var inside = false;
        var overlap = false;
        var completed = 0;
        using var subscription = signal.Subscribe(
            value =>
        {
            inside = true;
            values.Add(value);
            signal.OnCompleted();
            inside = false;
        },
            static _ => { },
            () =>
        {
            overlap |= inside;
            completed++;
        });
        signal.OnNext(ThirdValue);
        signal.OnNext(FirstValue);
        signal.OnNext(SecondValue);
        signal.MaximumCount = 1;
        await Assert.That(completed).IsEqualTo(1);
        await Assert.That(values.SequenceEqual([FirstValue, SecondValue, ThirdValue])).IsTrue();
        await Assert.That(overlap).IsFalse();
    }

    /// <summary>Reentrant errors wait until value delivery returns.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReentrantErrorWaitsForValueDelivery()
{
        using var signal = new PrioritySemaphoreSignal<int>(0);
        List<int> values = [];
        var inside = false;
        var overlap = false;
        InvalidOperationException expected = new("expected");
        Exception? observed = null;
        using var subscription = signal.Subscribe(
            value =>
        {
            inside = true;
            values.Add(value);
            signal.OnError(expected);
            inside = false;
        },
            error =>
        {
            overlap |= inside;
            observed = error;
        });
        signal.OnNext(ThirdValue);
        signal.OnNext(FirstValue);
        signal.OnNext(SecondValue);
        signal.MaximumCount = 1;
        await Assert.That(observed).IsSameReferenceAs(expected);
        await Assert.That(values.SequenceEqual([FirstValue])).IsTrue();
        await Assert.That(overlap).IsFalse();
    }

    /// <summary>A second error notification after the signal has terminated is ignored.</summary>
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

    /// <summary>A throwing delivery releases drain ownership so a later release drains again.</summary>
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

        // Delivery throws out of the drain loop; the finally path releases ownership.
        var first = Assert.Throws<InvalidOperationException>(() => signal.OnNext(FirstValue));
        await Assert.That(first).IsSameReferenceAs(failure);

        // Capacity is exhausted, so this value only enqueues.
        signal.OnNext(SecondValue);

        // Releasing frees capacity and begins a fresh drain, so the delivery throws again.
        var second = Assert.Throws<InvalidOperationException>(signal.Release);
        await Assert.That(second).IsSameReferenceAs(failure);
        await Assert.That(deliveries).IsEqualTo(FirstDrainCount);
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
}
